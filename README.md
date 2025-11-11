# Real-Time-Customer-360-Data-Pipeline
I made a real-time Customer 360 data pipeline that gives a single, up-to-date view of all customer profiles. The project constantly takes in streaming customer data, puts it into Snowflake using Snowpipe, and uses Slowly Changing Dimension (SCD Type 2) logic to keep both the most up-to-date customer state and a full historical record.
