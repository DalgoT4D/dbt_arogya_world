{{ config(
    materialized = "table",
    schema = "intermediate",
) }}
-- Compute score for each awareness indicator
-- 1. Indicator: Noodles, Chips, Chocolate and Samosa are unhealthy
--    Question codes - 1.1 , 1.2, 1.5, 1.6
--    Score - 1 for each unhealthy response; total score 4
-- **
-- 2. Indicator: Whole grains and Whole fruits are healthy
--    Question codes - 1.3, 1.4
--    Score - 1 for each healthy response; total score 2
WITH question_wise_score AS (

    SELECT
        partner,
        survey_type,
        survey_id,
        SUM(
            CASE
                WHEN question_code = '1.1'
                AND "value" = 'Unhealthy' THEN 1
                ELSE 0
            END + CASE
                WHEN question_code = '1.2'
                AND "value" = 'Unhealthy' THEN 1
                ELSE 0
            END + CASE
                WHEN question_code = '1.5'
                AND "value" = 'Unhealthy' THEN 1
                ELSE 0
            END + CASE
                WHEN question_code = '1.6'
                AND "value" = 'Unhealthy' THEN 1
                ELSE 0
            END
        ) AS score,
        4 AS total_score,
        'Noodles, Chips, Chocolate and Samosa are unhealthy' AS "indicator"
    FROM
        {{ ref("unpivot_flatten") }}
    GROUP BY
        partner,
        survey_type,
        survey_id
    UNION ALL
    SELECT
        partner,
        survey_type,
        survey_id,
        SUM(
            CASE
                WHEN question_code = '1.3'
                AND "value" = 'Unhealthy' THEN 1
                ELSE 0
            END + CASE
                WHEN question_code = '1.4'
                AND "value" = 'Unhealthy' THEN 1
                ELSE 0
            END
        ) AS score,
        2 AS total_score,
        'Whole grains and whole fruits are healthy' AS "indicator"
    FROM
        {{ ref("unpivot_flatten") }}
    GROUP BY
        partner,
        survey_type,
        survey_id
)
SELECT
    partner,
    survey_type,
    survey_id,
    CASE
        WHEN SUM(score) = 0 THEN 'not_aware'
        WHEN SUM(score) > 0
        AND SUM(score) < SUM(total_score) * 0.5 THEN 'partially_aware'
        ELSE 'aware'
    END AS "category"
FROM
    question_wise_score
GROUP BY
    partner,
    survey_type,
    survey_id
ORDER BY
    partner,
    survey_type,
    survey_id
