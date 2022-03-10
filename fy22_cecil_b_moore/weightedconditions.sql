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


drop table if exists conditions;
create table conditions as(
	select
	condition_1::int, condition_2::int,condition_3::int,condition_4::int,condition_5::int,condition_6::int,condition_7::int,condition_8::int,condition_9::int
	from condition2 c );
drop table condition2;



--changing numbers to weight them 
UPDATE conditions
	SET condition_1 = '-2'
	WHERE condition_1 = '1';
UPDATE conditions
	SET condition_1 = '-1'
	WHERE condition_1 = '2';
UPDATE conditions
	SET condition_1 = '0'
	WHERE condition_1 = '3';
UPDATE conditions
	SET condition_1 = '1'
	WHERE condition_1 = '4';
UPDATE conditions
	SET condition_1 = '2'
	WHERE condition_1 = '5';


UPDATE conditions
	SET condition_2 = '-2'
	WHERE condition_2 = '1';
UPDATE conditions
	SET condition_2 = '-1'
	WHERE condition_2 = '2';
UPDATE conditions
	SET condition_2 = '0'
	WHERE condition_2 = '3';
UPDATE conditions
	SET condition_2 = '1'
	WHERE condition_2 = '4';
UPDATE conditions
	SET condition_2 = '2'
	WHERE condition_2 = '5';

UPDATE conditions
	SET condition_3 = '-2'
	WHERE condition_3 = '1';
UPDATE conditions
	SET condition_3 = '-1'
	WHERE condition_3 = '2';
UPDATE conditions
	SET condition_3 = '0'
	WHERE condition_3 = '3';
UPDATE conditions
	SET condition_3 = '1'
	WHERE condition_3 = '4';
UPDATE conditions
	SET condition_3 = '2'
	WHERE condition_3 = '5';


UPDATE conditions
	SET condition_4 = '-2'
	WHERE condition_4 = '1';
UPDATE conditions
	SET condition_4 = '-1'
	WHERE condition_4 = '2';
UPDATE conditions
	SET condition_4 = '0'
	WHERE condition_4 = '3';
UPDATE conditions
	SET condition_4 = '1'
	WHERE condition_4 = '4';
UPDATE conditions
	SET condition_4 = '2'
	WHERE condition_4 = '5';

UPDATE conditions
	SET condition_5 = '-2'
	WHERE condition_5 = '1';
UPDATE conditions
	SET condition_5 = '-1'
	WHERE condition_5 = '2';
UPDATE conditions
	SET condition_5 = '0'
	WHERE condition_5 = '3';
UPDATE conditions
	SET condition_5 = '1'
	WHERE condition_5 = '4';
UPDATE conditions
	SET condition_5 = '2'
	WHERE condition_5 = '5';

UPDATE conditions
	SET condition_6 = '-2'
	WHERE condition_6 = '1';
UPDATE conditions
	SET condition_6 = '-1'
	WHERE condition_6 = '2';
UPDATE conditions
	SET condition_6 = '0'
	WHERE condition_6 = '3';
UPDATE conditions
	SET condition_6 = '1'
	WHERE condition_6 = '4';
UPDATE conditions
	SET condition_6 = '2'
	WHERE condition_6 = '5';

UPDATE conditions
	SET condition_7 = '-2'
	WHERE condition_7 = '1';
UPDATE conditions
	SET condition_7 = '-1'
	WHERE condition_7 = '2';
UPDATE conditions
	SET condition_7 = '0'
	WHERE condition_7 = '3';
UPDATE conditions
	SET condition_7 = '1'
	WHERE condition_7 = '4';
UPDATE conditions
	SET condition_7 = '2'
	WHERE condition_7 = '5';

UPDATE conditions
	SET condition_8 = '-2'
	WHERE condition_8 = '1';
UPDATE conditions
	SET condition_8 = '-1'
	WHERE condition_8 = '2';
UPDATE conditions
	SET condition_8 = '0'
	WHERE condition_8 = '3';
UPDATE conditions
	SET condition_8 = '1'
	WHERE condition_8 = '4';
UPDATE conditions
	SET condition_8 = '2'
	WHERE condition_8 = '5';

UPDATE conditions
	SET condition_9 = '-2'
	WHERE condition_9 = '1';
UPDATE conditions
	SET condition_9 = '-1'
	WHERE condition_9 = '2';
UPDATE conditions
	SET condition_9 = '0'
	WHERE condition_9 = '3';
UPDATE conditions
	SET condition_9 = '1'
	WHERE condition_9 = '4';
UPDATE conditions
	SET condition_9 = '2'
	WHERE condition_9 = '5';

drop table if exists conditions_weighted;
create table conditions_weighted as (select condition_1, count(condition_1) from conditions group by condition_1 order by condition_1);
drop table if exists conditions;


select condition_1, count(condition_1) from conditions group by condition_1 order by condition_1;
select condition_2, count(condition_2) from conditions group by condition_2 order by condition_2;
select condition_3, count(condition_3) from conditions group by condition_3 order by condition_3;
select condition_4, count(condition_4) from conditions group by condition_4 order by condition_4;
select condition_5, count(condition_5) from conditions group by condition_5 order by condition_5;
select condition_6, count(condition_6) from conditions group by condition_6 order by condition_6;
select condition_7, count(condition_7) from conditions group by condition_7 order by condition_7;
select condition_8, count(condition_8) from conditions group by condition_8 order by condition_8;
select condition_9, count(condition_9) from conditions group by condition_9 order by condition_9;

select sum(condition_1), sum(condition_2), sum(condition_3), sum(condition_4),sum(condition_5),sum(condition_6),sum(condition_7),sum(condition_8),sum(condition_9)from conditions;
select condition_2, sum(condition_2) from conditions group by condition_2 order by condition_2;

