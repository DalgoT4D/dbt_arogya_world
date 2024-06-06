{{ config(
    materialized = "table",
) }}
-- For each survey_id you will find a corresponding pre and post responses
WITH rows_with_survey_id AS (

    SELECT
        ROW_NUMBER() over (
            PARTITION BY "partner",
            "Survey_Type"
            ORDER BY
                (
                    SELECT
                        NULL
                )
        ) AS survey_id,
        "Survey_Type" AS survey_type,
        TO_DATE(
            "Timestamp",
            'DD-MM-YYYY'
        ) AS "timestamp",*
    FROM
        {{ ref('merged_data') }}
)
SELECT
    *
FROM
    rows_with_survey_id
