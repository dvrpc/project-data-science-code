select q2, count(q2) from longform_online lo 
group by q2

select q2, count(q2) from  in_person_survey_response_data_entry ipsrde 
group by q2

select q2, count(q2) from longform_joined lj 
group by q2

