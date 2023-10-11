with sales_orders as (
select *
  from {{ ref('stg_retail_sales_orders') }}
)

, explode_orders as (
select order_number
     , customer_id
     , customer_name
     , order_date
     , number_of_line_items
     , explode(order_items) as ordered_items_explode
  from sales_orders
)

, order_items as (
select order_number
     , customer_id
     , customer_name
     , order_date
     , number_of_line_items
     , ordered_items_explode.curr as currency
     , ordered_items_explode.id as product_id
     , ordered_items_explode.name as product_name
     , ordered_items_explode.price as price
     , ordered_items_explode.promotion_info as promotion_info
     , ordered_items_explode.qty as quantity
     , ordered_items_explode.unit as unit
  from explode_orders
)

select *
  from order_items