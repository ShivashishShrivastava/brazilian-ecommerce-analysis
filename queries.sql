# PHASE 1: Initial Exploration

# 1.1 Check data types of all columns in the "customers" table

select 
  column_name, data_type
from `target.INFORMATION_SCHEMA.COLUMNS`
where table_name = 'customers';

# 1.2 Get the order date range

select
  min(order_purchase_timestamp) as start_date,
  max(order_purchase_timestamp) as end_date
from `target.orders`;

# 1.3 Count distinct cities and states in customer table

select 
  count(distinct customer_city) as unique_cities,
  count(DISTINCT customer_state) as unique_states
from `target.customers`;



# PHASE 2: In-Depth Exploration

## 2.1 Yearly trend in orders

select 
  extract(year from order_purchase_timestamp) as year,
  count(*) as total_orders
from `target.orders`
group by year
order by year;

## 2.2 Monthly seasonality in terms of the no. of orders being placed

select month_name, month, year,count(month) as no_of_orders
from ( select *, extract(month from order_purchase_timestamp) as month,
extract(year from order_purchase_timestamp) as year,
format_datetime('%b', order_purchase_timestamp) as month_name, from `target.orders`)a
group by 1,2,3
order by 2,3;

## 2.3 Order placement time category 

select 
  CASE
    WHEN extract(hour from order_purchase_timestamp) BETWEEN 0 AND 6 THEN 'Dawn'
    WHEN extract(hour from order_purchase_timestamp) BETWEEN 7 AND 12 THEN 'Morning'
    WHEN extract(hour from order_purchase_timestamp) BETWEEN 13 AND 18 THEN 'Afternoon'
    WHEN extract(hour from order_purchase_timestamp) BETWEEN 19 AND 23 THEN 'Night'
  END AS time_of_day,
  COUNT(*) AS order_count
FROM `target.orders`
GROUP BY time_of_day
ORDER BY order_count DESC;



#PHASE 3: Evolution of E-commerce in Brazil

## 3.1 Month-on-month order count per state

select
customer_state,
extract(month from order_purchase_timestamp) as month,
count(*) as no_of_orders,
from `target.orders`
join `target.customers` using (customer_id)
group by customer_state, month
order by customer_state asc, month asc;

## 3.2 Customer distribution by state

select
  customer_state as state,
  count(customer_id) as customer_count
from `target.customers`
group by state
order by customer_count desc;



# PHASE 4: Economic Impact Analysis

## 4.1  Get the % increase in the cost of orders from year 2017 to 2018 (include months between Jan to Aug only).
#You can use the "payment_value" column in the payments table to get the cost of orders.

with base_1 as
(
  select 
  * 
  from `target.orders` 
  inner join `target.payments`  using (order_id)
  where  extract(year from order_purchase_timestamp) between 2017 and 2018 and extract(month from order_purchase_timestamp) between 1 and 8
),
base_2 as
(
  select
  extract(year from order_purchase_timestamp) as year,
  round(sum(payment_value),2) as cost
  from base_1
  group by year
  order by year
),
base_3 as
(
  select
  *, lead(cost) over(order by cost) as cost_next_year
  from base_2
)
select *, (cost_next_year - cost) / cost * 100 as percent_increase
from base_3;


## 4.2 Total & average order price per state

select 
  customer_state,
  round(sum(price),2) as total_price, 
  round(avg(price),2) as average_price
from `target.customers`
inner join `target.orders` using (customer_id)
inner join `target.order_items` using (order_id)
group by customer_state
order by customer_state;

## 4.3 Total & average freight per state

select 
  customer_state,
  round(sum(freight_value),2) as total_freight_value, 
  round(avg(freight_value),2) as avg_freight_value
from `target.customers`
inner join `target.orders` using (customer_id)
inner join `target.order_items` using (order_id)
group by customer_state
order by customer_state;



# PHASE 5: Delivery & Freight Performance

## 5.1 Delivery time & estimated delivery difference (in one query)

select
  order_id,
  timestamp_diff(order_delivered_customer_date, order_purchase_timestamp,day) as time_to_deliver,
  timestamp_diff(order_delivered_customer_date, order_estimated_delivery_date,day) as diff_estimated_delivery
from `target.orders`
where order_status = 'delivered';

## 5.2 Top 5 states with the highest & lowest average freight value

select
(a.customer_state) as highest_avg_freight_state, a.highest_avg_freight_value,
(b.customer_state) as lowest_avg_freight_state,
b.lowest_avg_freight_value
from
(select customer_state, round(avg(freight_value),2) as highest_avg_freight_value ,
row_number() over(order by round(avg(freight_value),2) asc) as rnk from `target.customers` c
inner join `target.orders` ord on ord.customer_id = c.customer_id
inner join `target.order_items` o on o.order_id = ord.order_id
group by 1
order by highest_avg_freight_value desc
limit 5) a
inner join
(select customer_state, round(avg(freight_value),2) as lowest_avg_freight_value, row_number()
over(order by round(avg(freight_value),2) desc) rnk from
`target.customers` c
inner join `target.orders` ord on ord.customer_id = c.customer_id
inner join `target.order_items` o on o.order_id = ord.order_id
group by 1
order by lowest_avg_freight_value
limit 5) b on a.rnk = b.rnk;


## 5.3 Top 5 states with the highest & lowest average delivery time

select (a.customer_state) as highest_avg_time_deliver_state, a.highest_average_time_deliver,
(b.customer_state) as lowest_avg_time_deliver_state, b.lowest_average_time_deliver
from
(select customer_state, round(avg(time_to_deliver),2) as highest_average_time_deliver,
row_number() over(order by round(avg(time_to_deliver),2) desc) as rnk
from
(select customer_state, timestamp_diff(order_delivered_customer_date,
order_purchase_timestamp, day) as time_to_deliver from `target.customers` c
inner join `target.orders` ord on ord.customer_id = c.customer_id
inner join `target.order_items` o on o.order_id = ord.order_id)a
group by 1
order by 2 desc
limit 5)a
inner join
(select customer_state, round(avg(time_to_deliver),2) as lowest_average_time_deliver,
row_number() over(order by round(avg(time_to_deliver),2) asc) as rnk
from
(select customer_state, timestamp_diff(order_delivered_customer_date,
order_purchase_timestamp, day) as time_to_deliver
from `target.customers` c
inner join `target.orders` ord on ord.customer_id = c.customer_id
inner join `target.order_items` o on o.order_id = ord.order_id)a
group by 1
order by 2
limit 5)b on a.rnk =b.rnk;


## 5.4 Top 5 states where the order delivery is faster compared to the estimated date of delivery.

select 
  customer_state as state,
  round(avg(timestamp_diff(order_delivered_customer_date, order_purchase_timestamp, day)),2) as avg_del_time,
  round(avg(timestamp_diff(order_estimated_delivery_date,order_purchase_timestamp, day)),2) as avg_est_del_time
  from `target.orders` 
  inner join `target.customers` using (customer_id)
  where order_status = 'delivered'
  group by state
  order by (avg_del_time - avg_est_del_time)
  limit 5;


# PHASE 6: Payments Analysis

## 6.1 Month-on-month orders by payment type

select
extract ( month from order_purchase_timestamp) as month,
extract ( year from order_purchase_timestamp) as year,payment_type,
count(distinct order_id) as total_orders
from `target.orders`
inner join `target.payments` using (order_id)
group by month, year, payment_type
order by month, year;


##6.2. Number of orders placed on the basis of the payment installments that have been paid

select
 payment_installments,
 count(distinct order_id) as num_orders,
from `target.payments`
where payment_installments >= 1
group by payment_installments
order by payment_installments;


