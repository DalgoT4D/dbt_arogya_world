{{ config(
    materialized = "table",
    schema = "intermediate",
) }}
{{ dbt_utils.unpivot(relation = ref('map_pre_post'), exclude = ['survey_id', 'survey_type','timestamp'], remove = ['Timestamp', 'Survey_Type'], field_name = 'question_code') }}
