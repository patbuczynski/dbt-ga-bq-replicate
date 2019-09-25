{{config(materialized = 'table')}}

SELECT 
ga.Page
, Pageviews
, uniquePageviews
, Entraces
, Exits
, s.sessions sessions
, sum((b.bounces / s.sessions) * 100) as bounceRate
, sum((Exits / Pageviews) * 100) percExits
FROM 
  (SELECT 
  h.page.pagePath Page
  , count(*) Pageviews
  , count(distinct concat(fullVisitorId, cast(visitID as string))) uniquePageviews
  , sum(CASE when h.isEntrance is not null then 1 end) Entraces
  , sum(CASE when h.isExit is not null then 1 end) Exits
  FROM {{var('tableName')}}, unnest(hits) as h
  WHERE h.type = "PAGE" AND {{tableRange()}}
  GROUP BY 1
  ORDER BY 2 DESC) ga

JOIN ({{ getSessions('h.page.pagePath', 'page') }}) as s on s.page = ga.page
JOIN ({{ getBounces('h.page.pagePath', 'page') }}) as b on b.page = ga.page

GROUP BY 1,2, 3, 4, 5, 6, b.bounces, s.sessions
ORDER BY 2 DESC
