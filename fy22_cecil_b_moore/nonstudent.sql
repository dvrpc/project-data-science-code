with nonstudent as(
select q2, q3, q4, q5_gender, q6, q7, "usage", frequency, mode from longform_joined lj 
where "usage" not like '%Commute to school%')
select q2, count(q2) from nonstudent 
group by q2

