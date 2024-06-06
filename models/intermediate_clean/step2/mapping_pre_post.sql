{{ config(
    materialized = "table",
) }}
-- map/match pre & post based on student_name (4), class (9), section (10) & date_of_birth (12)
-- keep only the unique mapping(s). that is one pre with one post; remove all duplicates
WITH unique_tuples AS (

    SELECT
        pre."4",
        pre."9",
        pre."10",
        pre."12"
    FROM
        {{ ref('unique_ids_pre_survey') }} AS pre
        INNER JOIN {{ ref("unique_ids_post_survey") }} AS post
        ON pre."4" = post."4"
        AND pre."9" = post."9"
        AND pre."10" = post."10"
        AND TO_DATE(
            pre."12",
            'DD-MM-YYYY'
        ) = TO_DATE(
            post."12",
            'DD-MM-YYYY'
        )
    GROUP BY
        pre."4",
        pre."9",
        pre."10",
        pre."12"
    HAVING
        COUNT(*) = 1
),
unique_tuple_ids AS (
    SELECT
        *,
        ROW_NUMBER() over () AS survey_id
    FROM
        unique_tuples
)
SELECT
    pre.*,
    unique_tuple_ids.survey_id
FROM
    {{ ref("unique_ids_pre_survey") }} AS pre
    INNER JOIN unique_tuple_ids
    ON pre."4" = unique_tuple_ids."4"
    AND pre."9" = unique_tuple_ids."9"
    AND pre."10" = unique_tuple_ids."10"
    AND TO_DATE(
        pre."12",
        'DD-MM-YYYY'
    ) = TO_DATE(
        unique_tuple_ids."12",
        'DD-MM-YYYY'
    )
UNION ALL
SELECT
    post.*,
    unique_tuple_ids.survey_id
FROM
    {{ ref("unique_ids_post_survey") }} AS post
    INNER JOIN unique_tuple_ids
    ON post."4" = unique_tuple_ids."4"
    AND post."9" = unique_tuple_ids."9"
    AND post."10" = unique_tuple_ids."10"
    AND TO_DATE(
        post."12",
        'DD-MM-YYYY'
    ) = TO_DATE(
        unique_tuple_ids."12",
        'DD-MM-YYYY'
    )
