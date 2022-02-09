drop table if exists pin_tag_analysis;
create table pin_tag_analysis as (
select 
	count(tag_1) filter (where tag_1) as bicycle 
	, count(tag_2) filter (where tag_2) as pedestrian 
	, count(tag_3) filter (where tag_3) as transit 
	, count(tag_4) filter (where tag_4) as speeding 
	, count(tag_5) filter (where tag_5) as visibility 
	, count(tag_6) filter (where tag_6) as access_ada 
	, count(tag_7) filter (where tag_7) as maintenence 
	, count(tag_8) filter (where tag_8) as traffic 
	, count(tag_9) filter (where tag_9) as parking 
	, count(tag_10) filter (where tag_10) as wayfinding 
from pins_pin pp );

