--All Injury Rear-end Crashes
select count (crash_table)
	from crash_pennsylvania as crash_table 
	where (crash_table.collision_type = '1')
	and (crash_table.crash_year = '2017' 
		or crash_table.crash_year = '2018' 
		or crash_table.crash_year = '2019' 
		or crash_table.crash_year = '2020' 
		or crash_table.crash_year = '2021')
	and not (crash_table.max_severity_level = '0')
	
--KSI Rear-end Crashes
select count (crash_table)
	from crash_pennsylvania as crash_table 
	where (crash_table.collision_type = '1')
	and (crash_table.crash_year = '2017' 
		or crash_table.crash_year = '2018' 
		or crash_table.crash_year = '2019' 
		or crash_table.crash_year = '2020' 
		or crash_table.crash_year = '2021')
	and (crash_table.max_severity_level = '1' or crash_table.max_severity_level = '2')
	
--All Injury Angle Crashes
select count (crash_table)
	from crash_pennsylvania as crash_table 
	where (crash_table.collision_type = '4')
	and (crash_table.crash_year = '2017' 
		or crash_table.crash_year = '2018' 
		or crash_table.crash_year = '2019' 
		or crash_table.crash_year = '2020' 
		or crash_table.crash_year = '2021')
	and not (crash_table.max_severity_level = '0')
	
--KSI Angle Crashes
select count (crash_table)
	from crash_pennsylvania as crash_table 
	where (crash_table.collision_type = '4')
	and (crash_table.crash_year = '2017' 
		or crash_table.crash_year = '2018' 
		or crash_table.crash_year = '2019' 
		or crash_table.crash_year = '2020' 
		or crash_table.crash_year = '2021')
	and (crash_table.max_severity_level = '1' or crash_table.max_severity_level = '2')
	
--Pedestrians Injured in Crashes
select count (person_table)
	from crash_pa_person as person_table
	inner join crash_pennsylvania as crash_table on person_table.crn = crash_table.crn 
	where (person_table.person_type = '7')
	and (crash_table.crash_year = '2017' 
		or crash_table.crash_year = '2018' 
		or crash_table.crash_year = '2019' 
		or crash_table.crash_year = '2020' 
		or crash_table.crash_year = '2021')
	and not (person_table.inj_severity = '0')
	
--Pedestrian KSI in Crashes
select count (person_table)
	from crash_pa_person as person_table
	inner join crash_pennsylvania as crash_table on person_table.crn = crash_table.crn 
	where (person_table.person_type = '7')
	and (crash_table.crash_year = '2017' 
		or crash_table.crash_year = '2018' 
		or crash_table.crash_year = '2019' 
		or crash_table.crash_year = '2020' 
		or crash_table.crash_year = '2021')
	and (person_table.inj_severity = '1' or person_table.inj_severity = '2')
	
--Bicyclists Injured in Crashes
select count (person_table)
	from crash_pa_person as person_table
	left join crash_pa_vehicle as vehicle_table on (concat(text(person_table.crn), text(person_table.unit_num)) = concat(text(vehicle_table.crn), text(vehicle_table.unit_num)))
	inner join crash_pennsylvania as crash_table on person_table.crn = crash_table.crn 
	where vehicle_table.unit_type = '11'
	and (crash_table.crash_year = '2017' 
		or crash_table.crash_year = '2018' 
		or crash_table.crash_year = '2019' 
		or crash_table.crash_year = '2020' 
		or crash_table.crash_year = '2021')
	and not (person_table.inj_severity = '0')
	
--Bicyclists KSI in Crashes
select count (person_table)
	from crash_pa_person as person_table
	left join crash_pa_vehicle as vehicle_table on (concat(text(person_table.crn), text(person_table.unit_num)) = concat(text(vehicle_table.crn), text(vehicle_table.unit_num)))
	inner join crash_pennsylvania as crash_table on person_table.crn = crash_table.crn 
	where vehicle_table.unit_type = '11'
	and (crash_table.crash_year = '2017' 
		or crash_table.crash_year = '2018' 
		or crash_table.crash_year = '2019' 
		or crash_table.crash_year = '2020' 
		or crash_table.crash_year = '2021')
	and (person_table.inj_severity = '1' or person_table.inj_severity = '2')
	
