{% macro getBounces(field, alias) -%}

SELECT
  {{ alias }} 
  , sum(CASE WHEN (noInteractions = 1 and isInteraction = true) or noInteractions = 0 THEN bounces ELSE null END) AS bounces
  FROM (SELECT
        {{ field }} {{ alias }}
        , COUNTIF(h.isInteraction = true) OVER (PARTITION BY fullVisitorId, visitId) AS noInteractions
        , h.isInteraction 
        , totals.bounces
        FROM {{var('tableName')}}, {{ unnest('hits', 'h') }}
	WHERE {{tableRange()}}
        GROUP BY 1, fullVisitorId, h.isInteraction, totals.bounces, visitId)
  {{ group_by(1) }}
  ORDER BY 2 DESC

{%- endmacro -%}