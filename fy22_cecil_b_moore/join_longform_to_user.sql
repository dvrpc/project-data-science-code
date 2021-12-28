create table longform_joined as (
select users.ip_address, users.responded_to_survey_question, users.q1, users.q2, users.q3, users.q4, users.q5, users.q6, users.q7, longform."usage", longform.frequency, longform."mode", longform.mode_issues, longform.condition_1, longform.condition_2, longform.condition_3, longform.condition_4, longform.condition_5, longform.condition_6, longform.condition_7, longform.condition_8, longform.condition_9, longform.created_on 
from pins_longformsurvey as longform
inner join pins_mapuser as users
on users.ip_address = longform.ip_address )