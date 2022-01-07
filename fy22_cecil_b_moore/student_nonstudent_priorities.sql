with nonstudent as(
select q2, q3, q4, q5_gender, q6, q7, "usage", frequency, priorities from longform_joined lj 
where "usage" not like '%Commute to school%')
SELECT split_part(priorities, ';', 1) as p1, count(priorities) as p1count
from nonstudent 
group by p1
order by p1

with student as(
select q2, q3, q4, q5_gender, q6, q7, "usage", frequency, priorities from longform_joined lj 
where "usage" like '%Commute to school%')
SELECT split_part(priorities, ';', 1) as p1, count(priorities) as p1count
from student
group by p1
order by p1









