{{ config(
    materialized = "table",
) }}

WITH cte AS (

    SELECT
        "partner",
        "survey_year",
        "program",
        "survey_type",
        "question_code",
        "ques_no",
        "value",
        COUNT(
            DISTINCT survey_id
        ) AS "count"
    FROM
        {{ ref("decode_options_unpivot") }}
    GROUP BY
        "partner",
        "survey_year",
        "program",
        "survey_type",
        "question_code",
        "ques_no",
        "value"
)
SELECT
    params.partner,
    params.survey_year,
    params.program,
    params.survey_type,
    cte.question_code,
    cte.ques_no,
    dictionary.slug AS "indicator",
    cte.value,
    cte.count,
    params.total_count
FROM
    cte
    INNER JOIN {{ ref("params") }}
    params
    ON cte.partner = params.partner
    AND cte.survey_year = params.survey_year
    AND cte.program = params.program
    AND cte.survey_type = params.survey_type
    INNER JOIN {{ source(
        'uncleaned',
        'dictionary'
    ) }} AS dictionary
    ON dictionary.code = cte.question_code
