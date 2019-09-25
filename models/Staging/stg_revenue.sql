{{ config(materialized='ephemeral') }}

with cte as 
 (SELECT 
  p.v2ProductName name
  , h.transaction.transactionId transactionId
  , sum(p.productRevenue/1000000) revenue
  , sum(p.productQuantity) quantity
  FROM {{var('tableName')}}, {{ unnest('hits', 'h') }},  {{ unnest('h.product', 'p') }}
  WHERE {{tableRange()}}
  {{ group_by(2) }})

SELECT name productName
  , sum(revenue) revenue
  , sum(quantity) quantity
  , count(distinct transactionId) transactions
  FROM cte
  group by 1
  order by revenue desc