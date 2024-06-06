{{ config(
    materialized = "table",
) }}
-- Figure out the count of each option in the question
WITH agg_question_wise AS (

    SELECT
        partner,
        survey_type,
        question_code,
        "value",
        COUNT(*) AS count_value
    FROM
        {{ ref("unpivot_flatten") }}
    GROUP BY
        partner,
        survey_type,
        question_code,
        "value"
    ORDER BY
        partner,
        question_code,
        "value",
        survey_type
)
SELECT
    agg.*,
    dict.label AS question_label,
    dict.category AS domain,
    dict.num AS question_num
FROM
    agg_question_wise agg
    INNER JOIN {{ source(
        'general',
        'survey_dictionary'
    ) }} AS dict
    ON agg.question_code = CAST(
        dict.code AS text
    )
