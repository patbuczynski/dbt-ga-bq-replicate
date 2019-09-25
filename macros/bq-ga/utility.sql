{% macro uid() -%}

 concat(fullVisitorId, cast(visitId as string)) as uid

{%- endmacro %}

{% macro tableRange() -%}

_table_suffix between '{{var("rangeStart")}}' and '{{var("rangeEnd")}}'

{%- endmacro %}

{% macro parseDate() -%}

parse_date("%Y%m%d", date) date_

{%- endmacro %}

{% macro eventCase(value) -%}

sum(CASE when h.eventInfo.eventAction = '{{value}}' then 1 else 0 end) as {{value | replace('-', '_')}}

{%- endmacro %}

{% macro unnest(field, alias) -%}

unnest({{field}}) as {{alias}}

{%- endmacro %}

{% macro group_by(n) %}

  GROUP BY {% for i in range(1, n + 1) %} {{ i }} {% if not loop.last %} , {% endif %} {% endfor %}

{% endmacro %}

