{{ config(
    materialized = "table",
    schema = "intermediate",
) }}
-- 1. Indicator: Daily intake of breakfast
--    Question codes - 3
-- **
-- 2. Indicator: Dialy intake of fruits
--    Question codes - 4.1

SELECT
    partner,
    survey_type,
    COUNT(
        DISTINCT survey_id
    ) AS total_count,
    COUNT(
        DISTINCT CASE
            WHEN "value" = 'Daily'
            AND question_code = '3' THEN survey_id
        END
    ) AS "count",
    'Daily intake of breakfast' AS "indicator"
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
            WHEN "value" = 'Daily'
            AND question_code = '4.1' THEN survey_id
        END
    ) AS "count",
    'Daily intake of fruits' AS "indicator"
FROM
    {{ ref("unpivot_flatten") }}
GROUP BY
    partner,
    survey_type
ORDER BY
    partner,
    survey_type
