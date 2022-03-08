drop table if exists intersection_buffer;
create table intersection_buffer as(
select st_buffer(geom, .0004) as geom, id
from intersection_points ip);


drop table if exists intersection_priority;

SELECT 
id,
count(CASE WHEN tag_1 THEN 1 END) bicycle, 
count(CASE WHEN tag_2 THEN 1 END) pedestrian,
count(CASE WHEN tag_3 THEN 1 END) transit,
count(CASE WHEN tag_4 THEN 1 END) speeding,
count(CASE WHEN tag_5 THEN 1 END) visibility,
count(CASE WHEN tag_6 THEN 1 END) access_ada,
count(CASE WHEN tag_7 THEN 1 END) maintenance,
count(CASE WHEN tag_8 THEN 1 END) traffic,
count(CASE WHEN tag_9 THEN 1 END) parking,
count(CASE WHEN tag_10 THEN 1 END) wayfinding
from  
(
	SELECT pp.*, ib.*
	FROM pins_pin pp
	JOIN intersection_buffer ib
	ON ST_Intersects(ib.geom, pp.geom))a
group by id
	



