create table longform_joined as(

select ip_address::varchar as ip_address, q1, q2, q3, q4, q5_gender, q6, q7, "usage", frequency, "mode", mode_issues, condition_1, condition_2, condition_3, condition_4, condition_5, condition_6, condition_7, condition_8, condition_9, priorities, ideas
from longform_online lo 

union 

select ip_address, q1, q2, q3, q4, q5, q6, q7, "usage", frequency, "mode", mode_issues, condition_1, condition_2, condition_3, condition_4, condition_5, condition_6, condition_7, condition_8, condition_9, priorities, ideas
from in_person_survey_response_data_entry ipsrde 
)