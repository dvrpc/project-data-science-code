drop table if exists pins_joined;

create table pins_joined as (
	select distinct on(pp.ip_address) pp.prompt_1, pp.prompt_2, pm.q1, pm.q2, pm.q3, pm.q4, pm.q5_gender, pm.q6, pm.q7 from pins_pin pp 
	inner join pins_mapuser pm 
	on pp.ip_address = pm.ip_address 
	order by (pp.ip_address)
	);

