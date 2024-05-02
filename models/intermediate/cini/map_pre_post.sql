{{ config(
    materialized = "table",
    schema = "intermediate",
) }}
-- For each survey_id you will find a corresponding pre and post responses
WITH rows_with_survey_id AS (

    SELECT
        ROW_NUMBER() over (
            PARTITION BY "Survey_Type"
            ORDER BY
                (
                    SELECT
                        NULL
                )
        ) AS survey_id,
        "Survey_Type" AS survey_type,
        "Timestamp" AS "timestamp",*
    FROM
        {{ source(
            'cini',
            'pre_post_answers'
        ) }}
)
SELECT
    *
FROM
    rows_with_survey_id
