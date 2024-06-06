{{ config(
    materialized = "table",
) }}

SELECT
    ROW_NUMBER() over () AS id,
    'post' AS survey_type,*
FROM
    {{ source(
        'uncleaned',
        'post_survey'
    ) }}
