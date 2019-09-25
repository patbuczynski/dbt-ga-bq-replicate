{% macro getSessions(field, alias) -%}

	SELECT 
  {{ alias }}
  , sum(CASE WHEN hitNumber = first_hit THEN visits ELSE null END) AS sessions
  FROM (SELECT
        {{ field }} {{ alias }}
        , MIN(h.hitNumber) OVER (PARTITION BY fullVisitorId, visitStartTime) AS first_hit
        , h.hitNumber
        , totals.visits
        FROM {{var('tableName')}}, {{ unnest('hits', 'h') }}
	WHERE {{tableRange()}}
        GROUP BY 1, h.hitNumber, fullVisitorId, visitStartTime, totals.visits, visitId
        ORDER BY 2 DESC)
      {{ group_by(1) }}
      ORDER BY 2 DESC

{%- endmacro %}

