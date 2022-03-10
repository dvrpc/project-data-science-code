with unnested (use) as (
   select unnest(string_to_array("usage", ';')) from longform_joined lj)
select count(*)
from unnested 
where use = 'Commute to work';

with unnested (use) as (
   select unnest(string_to_array("usage", ';')) from longform_joined lj)
select count(*)
from unnested 
where use = 'Commute to school';

with unnested (use) as (
   select unnest(string_to_array("usage", ';')) from longform_joined lj)
select count(*)
from unnested
where use = 'Run errands or go shopping';

with unnested (use) as (
   select unnest(string_to_array("usage", ';')) from longform_joined lj)
select count(*)
from unnested
where use = 'Go to religious services';

with unnested (use) as (
   select unnest(string_to_array("usage", ';')) from longform_joined lj)
select count(*)
from unnested
where use = 'Go out to restaurants or bars, socialize or entertainment';

with unnested (use) as (
   select unnest(string_to_array("usage", ';')) from longform_joined lj)
select count(*)
from unnested
where use = 'I do not currently use Cecil B. Moore Avenue';

with unnested (use) as (
   select unnest(string_to_array("usage", ';')) from longform_joined lj)
select count(*)
from unnested
where use like 'Other%';

