SELECT DISTINCT
    time AS date_time,
    toDate(time) AS date,
    toDayOfWeek(time) AS day_of_week,
    toMonth(time) AS month,
    toYear(time) AS year
FROM {{ ref('silver_weather') }}