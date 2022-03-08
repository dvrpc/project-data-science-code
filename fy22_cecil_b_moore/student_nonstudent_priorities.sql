drop table if exists non_student_priorities;
create table non_student_priorities as(
with nonstudent as(
select q2, q3, q4, q5_gender, q6, q7, "usage", frequency, priorities from longform_joined lj 
where "usage" not like '%Commute to school%')
SELECT split_part(priorities, ';', 1) as p1, count(priorities) as p1count
from nonstudent 
group by p1
order by p1);


drop table if exists student_priorities;
create table student_priorities as(
with student as(
select q2, q3, q4, q5_gender, q6, q7, "usage", frequency, priorities from longform_joined lj 
where "usage" like '%Commute to school%')
SELECT split_part(priorities, ';', 1) as p1, count(priorities) as p1count
from student
group by p1
order by p1);


v2
-----non student

with unnested (priorities) as (
   select unnest(string_to_array(priorities, ';')) from longform_joined lj where "usage" not like '%Commute to school%')
select count(*)
from unnested lj 
where priorities = 'Safe pedestrian crossings';

with unnested (priorities) as (
   select unnest(string_to_array(priorities, ';')) from longform_joined lj where "usage" not like '%Commute to school%')
select count(*)
from unnested lj 
where priorities = 'Safe bike lanes';

with unnested (priorities) as (
   select unnest(string_to_array(priorities, ';')) from longform_joined lj where "usage" not like '%Commute to school%')
select count(*)
from unnested lj 
where priorities = 'Increased pedestrian space';

with unnested (priorities) as (
   select unnest(string_to_array(priorities, ';')) from longform_joined lj where "usage" not like '%Commute to school%')
select count(*)
from unnested lj 
where priorities = 'Better parking and loading';

with unnested (priorities) as (
   select unnest(string_to_array(priorities, ';')) from longform_joined lj where "usage" not like '%Commute to school%')
select count(*)
from unnested lj 
where priorities = 'Safe bus boarding';

with unnested (priorities) as (
   select unnest(string_to_array(priorities, ';')) from longform_joined lj where "usage" not like '%Commute to school%')
select count(*)
from unnested lj 
where priorities like 'Other%';

--student

with unnested (priorities) as (
   select unnest(string_to_array(priorities, ';')) from longform_joined lj where "usage" like '%Commute to school%')
select count(*)
from unnested lj 
where priorities = 'Safe pedestrian crossings';

with unnested (priorities) as (
   select unnest(string_to_array(priorities, ';')) from longform_joined lj where "usage" like '%Commute to school%')
select count(*)
from unnested lj 
where priorities = 'Safe bike lanes';

with unnested (priorities) as (
   select unnest(string_to_array(priorities, ';')) from longform_joined lj where "usage" like '%Commute to school%')
select count(*)
from unnested lj 
where priorities = 'Increased pedestrian space';

with unnested (priorities) as (
   select unnest(string_to_array(priorities, ';')) from longform_joined lj where "usage" like '%Commute to school%')
select count(*)
from unnested lj 
where priorities = 'Better parking and loading';

with unnested (priorities) as (
   select unnest(string_to_array(priorities, ';')) from longform_joined lj where "usage" like '%Commute to school%')
select count(*)
from unnested lj 
where priorities = 'Safe bus boarding';

with unnested (priorities) as (
   select unnest(string_to_array(priorities, ';')) from longform_joined lj where "usage" like '%Commute to school%')
select count(*)
from unnested lj 
where priorities like 'Other%';





