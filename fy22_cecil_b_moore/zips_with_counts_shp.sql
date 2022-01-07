drop table if exists zips_with_counts;

create table zips_with_counts as (
	select zcta5ce, geom, count(lj.q6)
	from tl_pennsylvania5digit2009 tpd 
	inner join longform_joined lj 
	on lj.q6 = tpd.zcta5ce 
	group by zcta5ce, tpd.geom)
	
