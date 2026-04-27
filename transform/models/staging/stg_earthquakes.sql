-- models/staging/stg_earthquakes.sql
SELECT
    id,
    mag,
    place,
    lon,
    lat,
    depth_km,
    coalesce(alert, 'none')          AS alert,
    tsunami,
    sig,
    status,
    mag_type,
    net,
    event_time,
    -- feature ML : classification de profondeur
    CASE
        WHEN depth_km < 20  THEN 'shallow'
        WHEN depth_km < 70  THEN 'intermediate'
        ELSE 'deep'
    END                              AS depth_class,
    -- feature ML : classification de magnitude
    CASE
        WHEN mag < 2.0 THEN 'micro'
        WHEN mag < 4.0 THEN 'minor'
        WHEN mag < 6.0 THEN 'moderate'
        WHEN mag < 7.0 THEN 'strong'
        ELSE 'major'
    END                              AS mag_class
FROM {{ source('pulseearth', 'earthquakes') }}
WHERE mag IS NOT NULL
  AND lat IS NOT NULL
  AND lon IS NOT NULL