-- models/staging/stg_pollution.sql
SELECT
    location_id,
    sensor_id,
    parameter,
    value,
    unit,
    lat,
    lon,
    country,
    event_time,
    -- on pivote les paramètres en colonnes séparées pour simplifier les jointures
    CASE WHEN parameter = 'pm25' THEN value ELSE NULL END AS pm25,
    CASE WHEN parameter = 'no2'  THEN value ELSE NULL END AS no2,
    CASE WHEN parameter = 'o3'   THEN value ELSE NULL END AS o3
FROM {{ source('pulseearth', 'pollution') }}
WHERE value >= 0
  AND lat IS NOT NULL
  AND lon IS NOT NULL