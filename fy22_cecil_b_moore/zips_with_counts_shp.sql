drop table if exists zips_with_counts;

create table zips_with_counts as (
	select zcta5ce, geom, count(lj.q6)
	from zips 
	inner join longform_joined lj 
	on lj.q6 = zips.zcta5ce 
	group by zcta5ce, zips.geom)
	
