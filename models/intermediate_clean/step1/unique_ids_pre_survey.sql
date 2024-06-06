{{ config(
    materialized = "table",
) }}

SELECT
    ROW_NUMBER() over () AS id,
    'pre' AS survey_type,*
FROM
    {{ source(
        'uncleaned',
        'pre_survey'
    ) }}
