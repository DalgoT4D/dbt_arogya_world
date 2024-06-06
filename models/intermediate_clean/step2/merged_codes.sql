{{ config(
    materialized = "table",
) }}
-- All metadata/basic details (name, class, section, partner, year etc...) should be decoded here if already not done

SELECT
    survey_id,
    survey_type,
    year_dict.slug AS "survey_year",
    partner_dict.slug AS "partner",
    program_dict.slug AS "program",
    "4" AS "student_name",
    "6" AS "state",
    "7" AS "district",
    "8" AS "school",
    "9" AS "class",
    "10" AS "section",
    "11" AS "roll_no",
    TO_DATE(
        "12",
        'DD-MM-YYYY'
    ) AS "date_of_birth",
    SUBSTRING(
        "5"
        FROM
            '(^[0-9]{1,2}\.[0-9A-Z]{1,2}\.)'
    ) AS "5",
    SUBSTRING(
        "13.1"
        FROM
            '(^[0-9]{1,2}\.[0-9A-Z]{1,2}\.)'
    ) AS "13.1",
    SUBSTRING(
        "13.2"
        FROM
            '(^[0-9]{1,2}\.[0-9A-Z]{1,2}\.)'
    ) AS "13.2",
    SUBSTRING(
        "13.3"
        FROM
            '(^[0-9]{1,2}\.[0-9A-Z]{1,2}\.)'
    ) AS "13.3",
    SUBSTRING(
        "13.4"
        FROM
            '(^[0-9]{1,2}\.[0-9A-Z]{1,2}\.)'
    ) AS "13.4",
    SUBSTRING(
        "13.5"
        FROM
            '(^[0-9]{1,2}\.[0-9A-Z]{1,2}\.)'
    ) AS "13.5",
    SUBSTRING(
        "13.6"
        FROM
            '(^[0-9]{1,2}\.[0-9A-Z]{1,2}\.)'
    ) AS "13.6",
    SUBSTRING(
        "14.1"
        FROM
            '(^[0-9]{1,2}\.[0-9A-Z]{1,2}\.)'
    ) AS "14.1",
    SUBSTRING(
        "14.2"
        FROM
            '(^[0-9]{1,2}\.[0-9A-Z]{1,2}\.)'
    ) AS "14.2",
    SUBSTRING(
        "14.3"
        FROM
            '(^[0-9]{1,2}\.[0-9A-Z]{1,2}\.)'
    ) AS "14.3",
    SUBSTRING(
        "14.4"
        FROM
            '(^[0-9]{1,2}\.[0-9A-Z]{1,2}\.)'
    ) AS "14.4",
    SUBSTRING(
        "14.5"
        FROM
            '(^[0-9]{1,2}\.[0-9A-Z]{1,2}\.)'
    ) AS "14.5",
    SUBSTRING(
        "14.6"
        FROM
            '(^[0-9]{1,2}\.[0-9A-Z]{1,2}\.)'
    ) AS "14.6",
    SUBSTRING(
        "15"
        FROM
            '(^[0-9]{1,2}\.[0-9A-Z]{1,2}\.)'
    ) AS "15",
    SUBSTRING(
        "16.1"
        FROM
            '(^[0-9]{1,2}\.[0-9A-Z]{1,2}\.)'
    ) AS "16.1",
    SUBSTRING(
        "16.2"
        FROM
            '(^[0-9]{1,2}\.[0-9A-Z]{1,2}\.)'
    ) AS "16.2",
    SUBSTRING(
        "16.3"
        FROM
            '(^[0-9]{1,2}\.[0-9A-Z]{1,2}\.)'
    ) AS "16.3",
    SUBSTRING(
        "16.4"
        FROM
            '(^[0-9]{1,2}\.[0-9A-Z]{1,2}\.)'
    ) AS "16.4",
    SUBSTRING(
        "16.5"
        FROM
            '(^[0-9]{1,2}\.[0-9A-Z]{1,2}\.)'
    ) AS "16.5",
    SUBSTRING(
        "16.6"
        FROM
            '(^[0-9]{1,2}\.[0-9A-Z]{1,2}\.)'
    ) AS "16.6",
    SUBSTRING(
        "16.7"
        FROM
            '(^[0-9]{1,2}\.[0-9A-Z]{1,2}\.)'
    ) AS "16.7",
    SUBSTRING(
        "17"
        FROM
            '(^[0-9]{1,2}\.[0-9A-Z]{1,2}\.)'
    ) AS "17",
    SUBSTRING(
        "18"
        FROM
            '(^[0-9]{1,2}\.[0-9A-Z]{1,2}\.)'
    ) AS "18",
    SUBSTRING(
        "19"
        FROM
            '(^[0-9]{1,2}\.[0-9A-Z]{1,2}\.)'
    ) AS "19",
    SUBSTRING(
        "20"
        FROM
            '(^[0-9]{1,2}\.[0-9A-Z]{1,2}\.)'
    ) AS "20",
    SUBSTRING(
        "21"
        FROM
            '(^[0-9]{1,2}\.[0-9A-Z]{1,2}\.)'
    ) AS "21"
FROM
    {{ ref('mapping_pre_post') }}
    LEFT JOIN {{ source(
        'uncleaned',
        'dictionary'
    ) }} AS year_dict
    ON SUBSTRING(
        "1"
        FROM
            '(^[0-9]{1,2}\.[0-9A-Z]{1,2}\.)'
    ) = CONCAT(
        year_dict.code,
        '.'
    )
    AND "type" = 'option'
    LEFT JOIN {{ source(
        'uncleaned',
        'dictionary'
    ) }} AS partner_dict
    ON SUBSTRING(
        "2"
        FROM
            '(^[0-9]{1,2}\.[0-9A-Z]{1,2}\.)'
    ) = CONCAT(
        partner_dict.code,
        '.'
    )
    AND partner_dict.type = 'option'
    LEFT JOIN {{ source(
        'uncleaned',
        'dictionary'
    ) }} AS program_dict
    ON SUBSTRING(
        "3"
        FROM
            '(^[0-9]{1,2}\.[0-9A-Z]{1,2}\.)'
    ) = CONCAT(
        program_dict.code,
        '.'
    )
    AND program_dict.type = 'option'
