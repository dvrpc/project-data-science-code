SELECT split_part(priorities, ';', 1) AS p1
     , split_part(priorities, ';', 2) AS p2
     , split_part(priorities, ';', 3) AS p3
     , split_part(priorities, ';', 4) AS p4
     , split_part(priorities, ';', 5) AS p5
FROM   longform_joined lj;


SELECT split_part(priorities, ';', 1) as p1, count(priorities) as count
from longform_joined lj 
group by p1


