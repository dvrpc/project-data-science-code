SELECT split_part(priorities, ';', 1) AS p1
     , split_part(priorities, ';', 2) AS p2
     , split_part(priorities, ';', 3) AS p3
     , split_part(priorities, ';', 4) AS p4
     , split_part(priorities, ';', 5) AS p5
FROM   longform_joined lj


--these queries, although not as concise, do the same thing as above but with proper grouping and count column

SELECT split_part(priorities, ';', 1) as p1, count(priorities) as p1count
from longform_joined lj 
group by p1
order by p1

SELECT split_part(priorities, ';', 2) as p2, count(priorities) as p2count
from longform_joined lj 
group by p2
order by p2

SELECT split_part(priorities, ';', 3) as p3, count(priorities) as p3count
from longform_joined lj 
group by p3
order by p3

SELECT split_part(priorities, ';', 4) as p4, count(priorities) as p4count
from longform_joined lj 
group by p4
order by p4

SELECT split_part(priorities, ';', 5) as p5, count(priorities) as p5count
from longform_joined lj 
group by p5
order by p5
