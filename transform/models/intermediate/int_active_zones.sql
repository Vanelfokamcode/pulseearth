-- models/intermediate/int_active_zones.sql
-- Agrège les événements par cellule géographique de 1°x1°
-- pour identifier les zones avec activité simultanée multi-source

WITH eq_zones AS (
    SELECT
        round(lat)  AS zone_lat,
        round(lon)  AS zone_lon,
        count(*)    AS eq_count,
        max(mag)    AS max_mag,
        avg(mag)    AS avg_mag,
        max(event_time) AS last_eq_time
    FROM {{ ref('stg_earthquakes') }}
    WHERE event_time >= now() - INTERVAL 7 DAY
    GROUP BY zone_lat, zone_lon
),

wf_zones AS (
    SELECT
        round(lat)  AS zone_lat,
        round(lon)  AS zone_lon,
        count(*)    AS fire_count,
        max(frp)    AS max_frp,
        avg(frp)    AS avg_frp,
        max(event_time) AS last_fire_time
    FROM {{ ref('stg_wildfires') }}
    WHERE event_time >= now() - INTERVAL 7 DAY
    GROUP BY zone_lat, zone_lon
),

pol_zones AS (
    SELECT
        round(lat)  AS zone_lat,
        round(lon)  AS zone_lon,
        avg(pm25)   AS avg_pm25,
        max(pm25)   AS max_pm25,
        avg(no2)    AS avg_no2,
        max(event_time) AS last_pol_time
    FROM {{ ref('stg_pollution') }}
    WHERE event_time >= now() - INTERVAL 7 DAY
    GROUP BY zone_lat, zone_lon
)

SELECT
    coalesce(eq.zone_lat, wf.zone_lat, pol.zone_lat) AS zone_lat,
    coalesce(eq.zone_lon, wf.zone_lon, pol.zone_lon) AS zone_lon,
    coalesce(eq.eq_count,    0)    AS eq_count,
    coalesce(eq.max_mag,     0)    AS max_mag,
    coalesce(eq.avg_mag,     0)    AS avg_mag,
    coalesce(wf.fire_count,  0)    AS fire_count,
    coalesce(wf.max_frp,     0)    AS max_frp,
    coalesce(wf.avg_frp,     0)    AS avg_frp,
    coalesce(pol.avg_pm25,   0)    AS avg_pm25,
    coalesce(pol.max_pm25,   0)    AS max_pm25,
    coalesce(pol.avg_no2,    0)    AS avg_no2,
    -- score de risque composite brut (affiné par XGBoost au chapitre 11)
    (
        coalesce(eq.max_mag, 0) * 10 +
        coalesce(wf.max_frp, 0) / 100 +
        coalesce(pol.max_pm25, 0) / 10
    )                              AS raw_risk_score,
    greatest(
        coalesce(eq.last_eq_time,   toDateTime('1970-01-01')),
        coalesce(wf.last_fire_time, toDateTime('1970-01-01')),
        coalesce(pol.last_pol_time, toDateTime('1970-01-01'))
    )                              AS last_activity
FROM eq_zones eq
FULL OUTER JOIN wf_zones wf
    ON eq.zone_lat = wf.zone_lat AND eq.zone_lon = wf.zone_lon
FULL OUTER JOIN pol_zones pol
    ON coalesce(eq.zone_lat, wf.zone_lat) = pol.zone_lat
    AND coalesce(eq.zone_lon, wf.zone_lon) = pol.zone_lon