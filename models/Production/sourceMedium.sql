{{config(materialized = 'table')}}

with s as ({{getSessions('concat(trafficSource.source, " / ", trafficSource.medium)', 'sm')}}), 
             
b as      ({{getBounces('concat(trafficSource.source, " / ", trafficSource.medium)', 'sm')}}),

h as      (SELECT 
          concat(trafficSource.source, " / ", trafficSource.medium) sm 
          , count(*) allHits
          FROM {{var('tableName')}}, unnest(hits) as h
          WHERE h.type = "PAGE"
	  AND {{ tableRange() }}
          GROUP BY 1),   
  
-- transaction logic table
  
t as      (SELECT 
          concat(trafficSource.source, " / ", trafficSource.medium) sm ,
          sum(totals.transactions) transactions,
          sum(totals.totalTransactionRevenue / 1000000) revenue
          FROM {{var('tableName')}}
	  WHERE {{ tableRange() }}
          GROUP BY 1) 
   
          
SELECT 
ga.sm
, HLL_COUNT.MERGE(users_hll) Users
, sum(NewUsers) newUsers
, s.sessions
, sum((b.bounces / s.sessions) * 100) as bounceRate
, sum(h.allHits / s.sessions) pagesSession
, sum((t.transactions / s.sessions) * 100) ecommerceConversionRate
, sum(t.transactions) Transactions
, sum(t.revenue) Revenue
FROM
  (SELECT 
  concat(trafficSource.source, " / ", trafficSource.medium) sm 
  , count(distinct concat(fullVisitorId, cast(visitId as string))) as Sessions
  , HLL_COUNT.INIT(fullVisitorId, 14) as users_hll
  ,  COUNT(DISTINCT(Case When visitNumber = 1 then fullvisitorID else null end)) as NewUsers
  FROM {{var('tableName')}}, unnest (hits) as h
  WHERE h.isInteraction = true
  AND {{ tableRange() }}
  GROUP BY 1
  ORDER BY 2 desc) as ga

JOIN b ON b.sm = ga.sm
JOIN s ON s.sm = ga.sm
JOIN h ON h.sm = ga.sm
JOIN t ON t.sm = ga.sm

GROUP BY 1,4
ORDER BY 2 desc