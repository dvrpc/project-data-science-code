drop table if exists gender;
create table gender as(
select q5_gender , count(q5_gender) from longform_joined lj 
group by q5_gender);


select q5_gender , count(q5_gender) from pins_mapuser pm 
group by q5_gender 

select q5, count(q5) from in_person_survey_response_data_entry ipsrde 
group by q5

select q5_gender, count(q5_gender) from longform_joined lj 
group by q5_gender 