with source as (
    select * 
      from {{ source('retail', 'sales_orders') }}
)

, renamed as (
    select clicked_items
         , customer_id
         , customer_name
         , cast(number_of_line_items as int) as number_of_line_items
         , from_unixtime(order_datetime,'yyyy-MM-dd') as order_date
         , order_number
         , ordered_products as order_items
         , promo_info
      from source
)

, duplicate_orders as (
    select order_number
         , max(number_of_line_items) as max_number_of_line_items
      from renamed
     group by 1
)

, de_duped_orders as (
    select renamed.*
      from renamed
      join duplicate_orders
        on renamed.order_number = duplicate_orders.order_number
       and renamed.number_of_line_items = duplicate_orders.max_number_of_line_items
)

    select * 
      from de_duped_orders