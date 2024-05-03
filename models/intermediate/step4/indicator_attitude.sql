{{ config(
    materialized = "table",
    schema = "intermediate",
) }}
-- 1. Indicator: Preference for breakfast options
--    Question codes - 5, healthy ones ('Upma','Boiled Potato Chat','Vegetable Parantha')
-- **
-- 2. Indicator: Preference for snacks options
--    Question codes - 6, healthy ones ('Whole fruits','Packaged Juices')

SELECT
    partner,
    survey_type,
    COUNT(
        DISTINCT survey_id
    ) AS total_count,
    COUNT(
        DISTINCT CASE
            WHEN "value" IN (
                'Upma',
                'Boiled Potato Chat',
                'Vegetable Parantha'
            )
            AND question_code = '5' THEN survey_id
        END
    ) AS "count",
    'Preference for breakfast options' AS "indicator"
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
            WHEN "value" IN (
                'Poha',
                'Whole fruits',
                'Packaged Juices '
            )
            AND question_code = '6' THEN survey_id
        END
    ) AS "count",
    'Preference for snacks options' AS "indicator"
FROM
    {{ ref("unpivot_flatten") }}
GROUP BY
    partner,
    survey_type
ORDER BY
    partner,
    survey_type
