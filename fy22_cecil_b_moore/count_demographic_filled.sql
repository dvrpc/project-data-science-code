select count(*) from in_person_survey_response_data_entry ipsrde
where q1 <> ''
and q2 <> ''
and q3 <> ''
and q4 <> ''
and q5 <> ''
and q6::text <> ''
and q7 <> ''


select count(*) from longform_online lo
where q1 <> ''
and q2 <> ''
and q3 <> ''
and q4 <> ''
and q5_gender <> ''
and q6 <> ''
and q7 <> ''


