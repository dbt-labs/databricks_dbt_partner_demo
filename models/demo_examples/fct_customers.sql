{{
    config(
        materialized='table'
    )
}}

with customers as (
   select *
     from {{ ref('stg_retail_customers') }}
)

, orders as (
   select *
     from {{ ref('stg_retail_sales_order_items') }}
)

, customer_calcs as (
   select customer_id
        , sum(price * quantity) as total_order_amount
        , count(distinct order_number) as total_order_count
        , min(order_date) as first_order_date
        , max(order_date) as last_order_date
     from orders
     group by 1
)

, final_join as (
  select customers.customer_id
       , customers.customer_name
       , customers.tax_id
       , customers.tax_code
       , customers.state
       , customer_calcs.total_order_amount
       , customer_calcs.total_order_count
       , customer_calcs.first_order_date
       , customer_calcs.last_order_date
  from customers
  join customer_calcs
    on customers.customer_id = customer_calcs.customer_id
)

select *
  from final_join