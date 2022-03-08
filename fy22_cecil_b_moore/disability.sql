drop table if exists disability;
create table disability as(
select q4 , count(q4) from longform_joined lj 
group by q4);