{{ config(
    materialized = "table",
    schema = "intermediate",
) }}

SELECT
    *,
    'cini' AS partner
FROM
    {{ source(
        'cini',
        'pre_post_answers'
    ) }}
UNION ALL
SELECT
    *,
    'sru' AS partner
FROM
    {{ source(
        'sru',
        'pre_post_answers'
    ) }}
