drop table if exists pin_zip;

create table pin_zip as
(
	select zcta5ce, geom, count(pj.q6)
	from zips
	inner join pins_joined pj 
	on pj.q6 = zips.zcta5ce 
	group by zcta5ce, zips.geom)