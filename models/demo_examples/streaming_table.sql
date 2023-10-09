{{
    config(
        materialized='streaming_table',
        enabled=false
    )
}}

SELECT * FROM STREAM read_files('s3://sales-sandbox-databricks-unity-catalog/jaffle-shop/orders')