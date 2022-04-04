with old_city_taz as (
	select * 
	from "2010_TAZ" 
	where taz between 2 and 144)
select * from old_city_taz;


	
drop table if exists nj_philly_rec_trips;
create table nj_philly_rec_trips as (
	select geom, sum(compositeweight) from trip 
	left join "2010_TAZ" as shapes
	on shapes.taz=trip.o_taz 
	where d_county = 42101
		and o_state = 34
		and d_loc_type = 4
	group by geom
	order by geom
);


drop table if exists philly_nj_rec_trips;
create table philly_nj_rec_trips as (
	select geom, sum(compositeweight) from trip 
	left join "2010_TAZ" as shapes
	on shapes.taz=trip.d_taz 
	where o_county = 42101
		and d_state = 34
		and d_loc_type = 4
	group by geom
	order by geom 
);