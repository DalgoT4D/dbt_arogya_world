{{ config(
    materialized = "table",
) }}

SELECT
    unpivot_codes.survey_id,
    unpivot_codes.survey_type,
    unpivot_codes."survey_year",
    unpivot_codes."partner",
    unpivot_codes."program",
    unpivot_codes."student_name",
    unpivot_codes."state",
    unpivot_codes."district",
    unpivot_codes."school",
    unpivot_codes."class",
    unpivot_codes."section",
    unpivot_codes."roll_no",
    unpivot_codes."date_of_birth",
    unpivot_codes.question_code,
    dictionary.category,
    dictionary.ques_no,
    dictionary.slug AS "value"
FROM
    {{ ref("unpivot_codes") }}
    unpivot_codes
    INNER JOIN {{ source(
        'uncleaned',
        'dictionary'
    ) }}
    dictionary
    ON unpivot_codes.option_code = CONCAT(
        dictionary.code,
        '.'
    )
    AND dictionary.type = 'option'
