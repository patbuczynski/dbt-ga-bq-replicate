{{config(materialized = 'table')}}

SELECT *, sum(revenue/quantity) avgPrice, sum(quantity/transactions) avgQuantity 
FROM {{ref('stg_revenue')}}
GROUP BY 1,2,3,4
  ORDER BY 2 DESC