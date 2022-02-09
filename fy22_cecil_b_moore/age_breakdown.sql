drop table if exists age_breakdown;
create table age_breakdown as(
select q3, count(q3) from longform_joined lj 
group by q3);