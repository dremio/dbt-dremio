/*Copyright (C) 2022 Dremio Corporation

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.*/

{% macro dremio__get_intervals_between(start_date, end_date, datepart) -%}
    {%- call statement('get_intervals_between', fetch_result=True) %}
        {{dremio__datediff(start_date, end_date, datepart)}}
    {%- endcall -%}
    {%- set value_list = load_result('get_intervals_between') -%}

    {%- if value_list and value_list['data'] -%}
        {%- set values = value_list['data'] | map(attribute=0) | list %}
        {{ return(values[0]) }}
    {%- else -%}
        {{ return(1) }}
    {%- endif -%}

{%- endmacro %}


{% macro dremio__dateadd(datepart, interval, from_date_or_timestamp) %}
        {% set interval = interval.replace('order by 1', '') %}
        {% if datepart=='year' %}
            select TIMESTAMPADD(SQL_TSI_YEAR, CAST({{interval}} as int), CAST({{from_date_or_timestamp}} as TIMESTAMP))
        {% elif datepart=='quarter' %}
            select TIMESTAMPADD(SQL_TSI_QUARTER, CAST({{interval}} as int), CAST({{from_date_or_timestamp}} as TIMESTAMP))
        {% elif datepart=='month' %}
            select TIMESTAMPADD(SQL_TSI_MONTH, CAST({{interval}} as int), CAST({{from_date_or_timestamp}} as TIMESTAMP))
        {% elif datepart=='week' %}
            select TIMESTAMPADD(SQL_TSI_WEEK, CAST({{interval}} as int), CAST({{from_date_or_timestamp}} as TIMESTAMP))
        {% elif datepart=='hour' %}
            select TIMESTAMPADD(SQL_TSI_HOUR, CAST({{interval}} as int), CAST({{from_date_or_timestamp}} as TIMESTAMP))
        {% elif datepart=='minute' %}
            select TIMESTAMPADD(SQL_TSI_MINUTE, CAST({{interval}} as int), CAST({{from_date_or_timestamp}} as TIMESTAMP))
        {% elif datepart=='second' %}
            select TIMESTAMPADD(SQL_TSI_SECOND, CAST({{interval}} as int), CAST({{from_date_or_timestamp}} as TIMESTAMP))
        {% else %}
            select TIMESTAMPADD(SQL_TSI_DAY, CAST({{interval}} as int), CAST({{from_date_or_timestamp}} as TIMESTAMP))
        {% endif %}

{% endmacro %}

{% macro dremio__datediff(start_date, end_date, datepart) %}
    {% if datepart=='year' %}
        select (EXTRACT(YEAR FROM {{end_date}}) - EXTRACT(YEAR FROM {{start_date}})) 
    {% elif datepart=='quarter' %}
        select ((EXTRACT(YEAR FROM {{end_date}}) - EXTRACT(YEAR FROM {{start_date}})) * 4 + CEIL(EXTRACT(MONTH FROM {{end_date}}) / 3.0) - CEIL(EXTRACT(MONTH FROM {{start_date}}) / 3.0))
    {% elif datepart=='month' %}
        select ((EXTRACT(YEAR FROM {{end_date}}) - EXTRACT(YEAR FROM {{start_date}})) * 12 + (EXTRACT(MONTH FROM {{end_date}}) - EXTRACT(MONTH FROM {{start_date}})))
    {% elif datepart=='dayofyear' %}
        select TIMESTAMPDIFF(SQL_TSI_DAY, {{start_date}}, {{end_date}})
    {% elif datepart=='weekday' %}
        select CAST(CAST({{end_date}} AS DATE) - CAST({{start_date}} AS DATE) AS INTEGER)
    {% elif datepart=='week' %}
        select DATEDIFF(NEXT_DAY({{end_date}},&apos;SUNDAY&apos;),NEXT_DAY({{start_date}},&apos;SUNDAY&apos;))/7 
    {% elif datepart=='hour' %}
        select TIMESTAMPDIFF(hour, {{start_date}}, {{end_date}});
    {% elif datepart=='minute' %}
        select TIMESTAMPDIFF(minute, {{start_date}}, {{end_date}});
    {% elif datepart=='second' %}
        select TIMESTAMPDIFF(second, {{start_date}}, {{end_date}});
    {% else %}
        select DATEDIFF({{end_date}}, {{start_date}})
    {% endif %}
{% endmacro %}