{{
    config(
        materialized='streaming_table'
    )
}}

SELECT * FROM STREAM read_files('s3://sales-sandbox-databricks-unity-catalog/jaffle-shop/orders')