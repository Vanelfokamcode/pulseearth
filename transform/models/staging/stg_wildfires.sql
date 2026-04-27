-- models/staging/stg_wildfires.sql
SELECT
    lat,
    lon,
    brightness,
    frp,
    confidence,
    satellite,
    daynight,
    scan,
    track,
    event_time,
    -- feature ML : intensité normalisée
    CASE
        WHEN frp < 10   THEN 'low'
        WHEN frp < 100  THEN 'moderate'
        WHEN frp < 1000 THEN 'high'
        ELSE 'extreme'
    END                AS frp_class
FROM {{ source('pulseearth', 'wildfires') }}
WHERE lat IS NOT NULL
  AND lon IS NOT NULL
  AND frp > 0