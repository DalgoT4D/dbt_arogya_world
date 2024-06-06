{{ config(
    materialized = "table",
) }}
{{ dbt_utils.unpivot(relation = ref('merged_codes'), exclude = ['survey_id','survey_type','survey_year', 'partner', 'student_name','program','state', 'district', 'school', 'class', 'section', 'roll_no', 'date_of_birth'], field_name = 'question_code', value_name = 'option_code') }}
