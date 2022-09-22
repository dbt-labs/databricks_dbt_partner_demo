{{ config(
    materialized='incremental',
    sort='updated_at',
    unique_key='tweet_id') 
    }}

with

tweet_metrics as (

    select *
    from {{ ref('stg_tweet_metrics') }}
    {% if is_incremental() %}

    -- this filter will only be applied on an incremental run
    where updated_at > (select max(updated_at) from {{ this }})

    {% endif %}

)

select *
from tweet_metrics
