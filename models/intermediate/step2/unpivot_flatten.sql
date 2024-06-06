{{ config(
    materialized = "table",
) }}
{{ dbt_utils.unpivot(relation = ref('map_pre_post'), exclude = ['survey_id','partner','survey_type','timestamp'], remove = ['Timestamp', 'Survey_Type'], field_name = 'question_code') }}
