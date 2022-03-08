--delete this one, use "weightedconditions" instead


drop table if exists condition2;
create table condition2 as(
	select condition_1, condition_2, condition_3, condition_4, condition_5, condition_6, condition_7, condition_8, condition_9 
	from longform_joined lj );
	
update condition2 set condition_1 = 0 where condition_1 is null;
update condition2 set condition_2 = 0 where condition_2 is null;
update condition2 set condition_3 = 0 where condition_3 is null;
update condition2 set condition_4 = 0 where condition_4 is null;
update condition2 set condition_5 = 0 where condition_5 is null;
update condition2 set condition_6 = 0 where condition_6 is null;
update condition2 set condition_7 = 0 where condition_7 is null;
update condition2 set condition_8 = 0 where condition_8 is null;
update condition2 set condition_9 = 0 where condition_9 is null;

UPDATE condition2 
SET condition_1 = '0'
WHERE condition_1 = '';

UPDATE condition2 
SET condition_2 = '0'
WHERE condition_2 = '';

UPDATE condition2 
SET condition_3 = '0'
WHERE condition_3 = '';

UPDATE condition2 
SET condition_4 = '0'
WHERE condition_4 = '';

UPDATE condition2 
SET condition_5 = '0'
WHERE condition_5 = '';

UPDATE condition2 
SET condition_6 = '0'
WHERE condition_6 = '';

UPDATE condition2 
SET condition_7 = '0'
WHERE condition_7 = '';

UPDATE condition2 
SET condition_8 = '0'
WHERE condition_8 = '';

UPDATE condition2 
SET condition_9 = '0'
WHERE condition_9 = '';


ALTER TABLE condition2 ALTER COLUMN condition_1 TYPE text using (condition_1::integer);
ALTER TABLE condition2 ALTER COLUMN condition_2 TYPE text using (condition_2::integer);
ALTER TABLE condition2 ALTER COLUMN condition_3 TYPE text using (condition_3::integer);
ALTER TABLE condition2 ALTER COLUMN condition_4 TYPE text using (condition_4::integer);
ALTER TABLE condition2 ALTER COLUMN condition_5 TYPE text using (condition_5::integer);
ALTER TABLE condition2 ALTER COLUMN condition_6 TYPE text using (condition_6::integer);
ALTER TABLE condition2 ALTER COLUMN condition_7 TYPE text using (condition_7::integer);
ALTER TABLE condition2 ALTER COLUMN condition_8 TYPE text using (condition_8::integer);
ALTER TABLE condition2 ALTER COLUMN condition_9 TYPE text using (condition_9::integer);


--changing numbers to weight them 
UPDATE condition2 
	SET condition_1 = '-2'
	WHERE condition_1 = '1';
UPDATE condition2
	SET condition_1 = '-1'
	WHERE condition_1 = '2';
UPDATE condition2
	SET condition_1 = '0'
	WHERE condition_1 = '3';
UPDATE condition2
	SET condition_1 = '1'
	WHERE condition_1 = '4';
UPDATE condition2
	SET condition_1 = '2'
	WHERE condition_1 = '5';
