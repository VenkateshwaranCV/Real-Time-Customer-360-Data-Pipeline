CREATE OR REPLACE STAGE SCD_DEMO.SCD2.customer_ext_stage
    url = 'your_url'
    credentials = (AWS_KEY_ID='AWS_KEY_ID' AWS_SECRET_KEY='AWS_SECRET_KEY');

CREATE OR REPLACE FILE FORMAT SCD_DEMO.SCD2.CSV
TYPE = CSV,
FIELD_DELIMITER = ","
SKIP_HEADER = 1;

SHOW STAGES;
LIST @customer_ext_stage;

CREATE OR REPLACE PIPE customer_s3_pipe
    auto_ingest = true
    as
    COPY INTO customer_raw
    FROM @customer_ext_stage
    FILE_FORMAT = CSV ;

SHOW pipes;

SELECT count(*) FROM customer_raw limit 10;

TRUNCATE customer_raw;

    