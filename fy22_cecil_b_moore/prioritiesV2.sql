with unnested (priorities) as (
   select unnest(string_to_array(priorities, ';')) from longform_joined lj)
select count(*)
from unnested lj 
where priorities = 'Safe pedestrian crossings';

with unnested (priorities) as (
   select unnest(string_to_array(priorities, ';')) from longform_joined lj)
select count(*)
from unnested lj 
where priorities = 'Safe bike lanes';

with unnested (priorities) as (
   select unnest(string_to_array(priorities, ';')) from longform_joined lj)
select count(*)
from unnested lj 
where priorities = 'Increased pedestrian space';

with unnested (priorities) as (
   select unnest(string_to_array(priorities, ';')) from longform_joined lj)
select count(*)
from unnested lj 
where priorities = 'Better parking and loading';

with unnested (priorities) as (
   select unnest(string_to_array(priorities, ';')) from longform_joined lj)
select count(*)
from unnested lj 
where priorities = 'Safe bus boarding';

with unnested (priorities) as (
   select unnest(string_to_array(priorities, ';')) from longform_joined lj)
select count(*)
from unnested lj 
where priorities = 'Less aggressive driving';


with unnested (priorities) as (
   select unnest(string_to_array(priorities, ';')) from longform_joined lj)
select count(*)
from unnested lj 
where priorities = 'Quick drive times';

with unnested (priorities) as (
   select unnest(string_to_array(priorities, ';')) from longform_joined lj)
select count(*)
from unnested lj 
where priorities like 'Other%';





