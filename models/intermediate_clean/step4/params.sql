{{ config(
    materialized = "table",
) }}

WITH cte AS (

    SELECT
        DISTINCT t1.partner,
        t2.survey_year,
        t4.program,
        t3.survey_type
    FROM
        {{ ref("decode_options_unpivot") }} AS t1
        CROSS JOIN (
            SELECT
                DISTINCT survey_year
            FROM
                {{ ref("decode_options_unpivot") }}
        ) AS t2
        CROSS JOIN (
            SELECT
                DISTINCT survey_type
            FROM
                {{ ref("decode_options_unpivot") }}
        ) AS t3
        CROSS JOIN (
            SELECT
                DISTINCT program
            FROM
                {{ ref("decode_options_unpivot") }}
        ) AS t4
)
SELECT
    cte.partner,
    cte.survey_year,
    cte.program,
    cte.survey_type,
    COUNT(
        DISTINCT t5.survey_id
    ) AS "total_count"
FROM
    cte
    LEFT JOIN {{ ref("decode_options_unpivot") }} AS t5
    ON cte.partner = t5.partner
    AND cte.survey_year = t5.survey_year
    AND cte.program = t5.program
    AND cte.survey_type = t5.survey_type
GROUP BY
    cte.partner,
    cte.survey_year,
    cte.program,
    cte.survey_type
