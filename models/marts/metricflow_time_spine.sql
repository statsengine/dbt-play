-- models/marts/metricflow_time_spine.sql

{% set start_date = '2020-01-01' %}
{% set end_date = '2030-12-31' %}

WITH date_range AS (
    SELECT
        GENERATE_DATE_ARRAY(DATE('{{ start_date }}'), DATE('{{ end_date }}'))
            AS day
),

dates AS (
    SELECT
        day AS date_day,
        EXTRACT(YEAR FROM day) AS date_year,
        EXTRACT(MONTH FROM day) AS date_month,
        EXTRACT(DAY FROM day) AS date_day_of_month,
        EXTRACT(ISOWEEK FROM day) AS date_week,
        EXTRACT(QUARTER FROM day) AS date_quarter
    FROM
        UNNEST((SELECT day FROM date_range)) AS day
)

SELECT *
FROM
    dates
ORDER BY
    date_day
