{% macro export_to_csv(table_name) %}
    {% set query %}
        select * from {{ ref(table_name) }}
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}


    
