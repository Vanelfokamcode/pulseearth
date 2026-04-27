-- models/marts/mart_risk_zones.sql
-- Table finale consommée par FastAPI → Three.js
-- Une ligne par zone géographique avec son score de risque actuel

SELECT
    zone_lat,
    zone_lon,
    eq_count,
    max_mag,
    fire_count,
    max_frp,
    avg_pm25,
    max_pm25,
    raw_risk_score,
    last_activity,
    -- normalisation 0-100 pour le frontend
    least(
        round(raw_risk_score / 10 * 100),
        100
    )                   AS risk_score_pct,
    -- label pour l'affichage globe
    CASE
        WHEN raw_risk_score > 100 THEN 'critical'
        WHEN raw_risk_score > 50  THEN 'high'
        WHEN raw_risk_score > 20  THEN 'moderate'
        ELSE 'low'
    END                 AS risk_level,
    now()               AS computed_at
FROM {{ ref('int_active_zones') }}
WHERE last_activity >= now() - INTERVAL 7 DAY
ORDER BY raw_risk_score DESC