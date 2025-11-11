-- Step 1: Stream to capture changes on CUSTOMER (inserts, updates, deletes)
CREATE OR REPLACE STREAM customer_table_changes
ON TABLE customer;

-- Check streams in current schema
SHOW STREAMS;

-- See pending changes in the stream
SELECT * FROM customer_table_changes;


-- Step 2: View to turn stream changes into a simple feed for MERGE
-- Adds start_time, end_time, is_current, and a dml_type flag (I/U/D)

CREATE OR REPLACE VIEW v_customer_change_data AS

-- Inserts: new customer rows
SELECT
    CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, STREET, CITY, STATE, COUNTRY,
    update_timestamp AS start_time,
    LAG(update_timestamp) OVER (PARTITION BY customer_id ORDER BY update_timestamp DESC) AS end_time_raw,
    CASE WHEN end_time_raw IS NULL THEN '9999-12-31'::timestamp_ntz ELSE end_time_raw END AS end_time,
    CASE WHEN end_time_raw IS NULL THEN TRUE ELSE FALSE END AS is_current,
    'I' AS dml_type
FROM (
    SELECT CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, STREET, CITY, STATE, COUNTRY, update_timestamp
    FROM customer_table_changes
    WHERE metadata$action = 'INSERT' AND metadata$isupdate = 'FALSE'
)

UNION

-- Updates: close old version and add new version
SELECT
    CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, STREET, CITY, STATE, COUNTRY,
    update_timestamp AS start_time,
    LAG(update_timestamp) OVER (PARTITION BY customer_id ORDER BY update_timestamp DESC) AS end_time_raw,
    CASE WHEN end_time_raw IS NULL THEN '9999-12-31'::timestamp_ntz ELSE end_time_raw END AS end_time,
    CASE WHEN end_time_raw IS NULL THEN TRUE ELSE FALSE END AS is_current,
    dml_type
FROM (
    -- New version to insert
    SELECT CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, STREET, CITY, STATE, COUNTRY, update_timestamp, 'I' AS dml_type
    FROM customer_table_changes
    WHERE metadata$action = 'INSERT' AND metadata$isupdate = 'TRUE'

    UNION

    -- Old current row to close
    SELECT CUSTOMER_ID, NULL, NULL, NULL, NULL, NULL, NULL, NULL, start_time, 'U' AS dml_type
    FROM customer_history
    WHERE customer_id IN (
        SELECT DISTINCT customer_id
        FROM customer_table_changes
        WHERE metadata$action = 'DELETE' AND metadata$isupdate = 'TRUE'
    )
    AND is_current = TRUE
)

UNION

-- Deletes: mark current row as ended
SELECT
    ctc.CUSTOMER_ID, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    ch.start_time,
    CURRENT_TIMESTAMP()::timestamp_ntz AS end_time,
    NULL AS is_current,
    'D' AS dml_type
FROM customer_history ch
JOIN customer_table_changes ctc
  ON ch.customer_id = ctc.customer_id
WHERE ctc.metadata$action = 'DELETE'
  AND ctc.metadata$isupdate = 'FALSE'
  AND ch.is_current = TRUE;

-- Quick check of the view output
SELECT * FROM v_customer_change_data;


-- Step 3: Task to apply changes every minute using MERGE
CREATE OR REPLACE TASK tsk_scd_hist
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '1 minute'
  ERROR_ON_NONDETERMINISTIC_MERGE = FALSE
AS
MERGE INTO customer_history ch
USING v_customer_change_data ccd
  ON ch.customer_id = ccd.customer_id
 AND ch.start_time  = ccd.start_time

-- Close old version
WHEN MATCHED AND ccd.dml_type = 'U' THEN
  UPDATE SET ch.end_time = ccd.end_time, ch.is_current = FALSE

-- Logical delete
WHEN MATCHED AND ccd.dml_type = 'D' THEN
  UPDATE SET ch.end_time = ccd.end_time, ch.is_current = FALSE

-- Insert new version
WHEN NOT MATCHED AND ccd.dml_type = 'I' THEN
  INSERT (CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, STREET, CITY, STATE, COUNTRY, start_time, end_time, is_current)
  VALUES (ccd.CUSTOMER_ID, ccd.FIRST_NAME, ccd.LAST_NAME, ccd.EMAIL, ccd.STREET, ccd.CITY, ccd.STATE, ccd.COUNTRY, ccd.start_time, ccd.end_time, ccd.is_current);

-- Show tasks to confirm creation
SHOW TASKS;

-- Control the task
ALTER TASK tsk_scd_hist SUSPEND;  -- pause
-- ALTER TASK tsk_scd_hist RESUME; -- resume
