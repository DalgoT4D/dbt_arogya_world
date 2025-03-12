{{ config(
    materialized = "table",
) }}
-- This model constructs or targets the expected response of each indicator & filters them out
---------------------- 13 -----------------------
-- 13.1 Instant Noodles are Unhealthy
WITH cte AS (

    SELECT
        partner,
        survey_year,
        survey_type,
        question_code,
        ques_no,
        CONCAT(
            "indicator",
            ' are Unhealthy'
        ) AS "indicator",
        SUM(
            CASE
                WHEN "value" = 'Unhealthy' THEN "count"
                ELSE 0
            END
        ) AS "count",
        SUM("count") AS "total_count"
    FROM
        {{ ref("indicator_question_wise") }}
    WHERE
        question_code = '13.1'
    GROUP BY
        partner,
        survey_year,
        survey_type,
        question_code,
        ques_no,
        "indicator"
    UNION ALL
        -- 13.2 Chips are Unhealthy
    SELECT
        partner,
        survey_year,
        survey_type,
        question_code,
        ques_no,
        CONCAT(
            "indicator",
            ' are Unhealthy'
        ) AS "indicator",
        SUM(
            CASE
                WHEN "value" = 'Unhealthy' THEN "count"
                ELSE 0
            END
        ) AS "count",
        SUM("count") AS "total_count"
    FROM
        {{ ref("indicator_question_wise") }}
    WHERE
        question_code = '13.2'
    GROUP BY
        partner,
        survey_year,
        survey_type,
        question_code,
        ques_no,
        "indicator"
    UNION ALL
        -- 13.3 Grains/Cereals are Healthy
    SELECT
        partner,
        survey_year,
        survey_type,
        question_code,
        ques_no,
        'Grains/Cereals are Healthy' AS "indicator",
        SUM(
            CASE
                WHEN "value" = 'Healthy' THEN "count"
                ELSE 0
            END
        ) AS "count",
        SUM("count") AS "total_count"
    FROM
        {{ ref("indicator_question_wise") }}
    WHERE
        question_code = '13.3'
    GROUP BY
        partner,
        survey_year,
        survey_type,
        question_code,
        ques_no,
        "indicator"
    UNION ALL
        -- 13.4 Grains/Cereals are Healthy
    SELECT
        partner,
        survey_year,
        survey_type,
        question_code,
        ques_no,
        'Dals are Healthy' AS "indicator",
        SUM(
            CASE
                WHEN "value" = 'Healthy' THEN "count"
                ELSE 0
            END
        ) AS "count",
        SUM("count") AS "total_count"
    FROM
        {{ ref("indicator_question_wise") }}
    WHERE
        question_code = '13.4'
    GROUP BY
        partner,
        survey_year,
        survey_type,
        question_code,
        ques_no,
        "indicator"
    UNION ALL
        -- 13.5 Grains/Cereals are Healthy
    SELECT
        partner,
        survey_year,
        survey_type,
        question_code,
        ques_no,
        'Chocolates are Unhealthy' AS "indicator",
        SUM(
            CASE
                WHEN "value" = 'Healthy' THEN "count"
                ELSE 0
            END
        ) AS "count",
        SUM("count") AS "total_count"
    FROM
        {{ ref("indicator_question_wise") }}
    WHERE
        question_code = '13.5'
    GROUP BY
        partner,
        survey_year,
        survey_type,
        question_code,
        ques_no,
        "indicator"
    UNION ALL
        -- 13.6 Fried food is Unhealthy
    SELECT
        partner,
        survey_year,
        survey_type,
        question_code,
        ques_no,
        'Fried food is Unhealthy' AS "indicator",
        SUM(
            CASE
                WHEN "value" = 'Unhealthy' THEN "count"
                ELSE 0
            END
        ) AS "count",
        SUM("count") AS "total_count"
    FROM
        {{ ref("indicator_question_wise") }}
    WHERE
        question_code = '13.6'
    GROUP BY
        partner,
        survey_year,
        survey_type,
        question_code,
        ques_no,
        "indicator"
    UNION ALL
        ---------------------- 21 -----------------------
    SELECT
        partner,
        survey_year,
        survey_type,
        question_code,
        ques_no,
        "value" AS "indicator",
        SUM("count") AS "count",
        SUM("total_count") AS "total_count"
    FROM
        {{ ref("indicator_question_wise") }}
    WHERE
        question_code = '21'
    GROUP BY
        partner,
        survey_year,
        survey_type,
        question_code,
        ques_no,
        "value"
)
SELECT
    *,
    CONCAT(
        question_code,
        ' ',
        "indicator"
    ) AS "ques_indicator"
FROM
    cte
