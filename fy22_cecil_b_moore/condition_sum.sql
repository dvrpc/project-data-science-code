drop table if exists conditions_sum;

create table conditions_sum as (
select 
	(select sum(cast(
	    case
	        when condition_1 is null then '0'
	        when condition_1 = '' then '0'
		        else condition_1
		    end
		    as float
		)) as condition_1_sum
		from longform_joined lj),
	(select sum(cast(
	    case
	        when condition_2 is null then '0'
	        when condition_2 = '' then '0'
		        else condition_2
		    end
		    as float
		)) as condition_2_sum
		from longform_joined lj),
	(select sum(cast(
	    case
	        when condition_3 is null then '0'
	        when condition_3 = '' then '0'
		        else condition_3
		    end
		    as float
		)) as condition_3_sum
		from longform_joined lj), 
	(select sum(cast(
	    case
	        when condition_4 is null then '0'
	        when condition_4 = '' then '0'
		        else condition_4
		    end
		    as float
		)) as condition_4_sum
		from longform_joined lj), 
	(select sum(cast(
	    case
	        when condition_5 is null then '0'
	        when condition_5 = '' then '0'
		        else condition_5
		    end
		    as float
		)) as condition_5_sum
		from longform_joined lj), 
	(select sum(cast(
	    case
	        when condition_6 is null then '0'
	        when condition_6 = '' then '0'
		        else condition_6
		    end
		    as float
		)) as condition_6_sum
		from longform_joined lj), 
	(select sum(cast(
	    case
	        when condition_7 is null then '0'
	        when condition_7 = '' then '0'
		        else condition_7
		    end
		    as float
		)) as condition_7_sum
		from longform_joined lj), 
	(select sum(cast(
	    case
	        when condition_8 is null then '0'
	        when condition_8 = '' then '0'
		        else condition_8
		    end
		    as float
		)) as condition_8_sum
		from longform_joined lj), 
	(select sum(cast(
	    case
	        when condition_9 is null then '0'
	        when condition_9 = '' then '0'
		        else condition_9
		    end
		    as float
		)) as condition_9_sum
		from longform_joined lj) 
)



