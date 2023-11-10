{{
    config(
        materialized='table'
    )
}}

select * from hive_metastore.analytics.fct_order_items