{{ config(
    materialized = "table",
    schema = "intermediate",
) }}
-- 1. Indicator: Noodles, Chips, Chocolate and Samosa are unhealthy
--    Question codes - 1.1 , 1.2, 1.5, 1.6
-- **
-- 2. Indicator: Whole grains and Whole fruits are healthy
--    Question codes - 1.3, 1.4

SELECT
    partner,
    survey_type,
    COUNT(
        DISTINCT survey_id
    ) AS total_count,
    COUNT(
        DISTINCT CASE
            WHEN "value" = 'Unhealthy'
            AND question_code IN (
                '1.1',
                '1.2',
                '1.5',
                '1.6'
            ) THEN survey_id
        END
    ) AS "count",
    'Noodles, Chips, Chocolate and Samosa are unhealthy' AS "indicator"
FROM
    {{ ref("unpivot_flatten") }}
GROUP BY
    partner,
    survey_type
UNION ALL
SELECT
    partner,
    survey_type,
    COUNT(
        DISTINCT survey_id
    ) AS total_count,
    COUNT(
        DISTINCT CASE
            WHEN "value" = 'Healthy'
            AND question_code IN (
                '1.3',
                '1.4'
            ) THEN survey_id
        END
    ) AS "count",
    'Whole grains and whole fruits are healthy' AS "indicator"
FROM
    {{ ref("unpivot_flatten") }}
GROUP BY
    partner,
    survey_type
ORDER BY
    partner,
    survey_type
