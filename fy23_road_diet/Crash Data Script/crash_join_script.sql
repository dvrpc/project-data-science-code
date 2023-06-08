-- nlf id etc are fields that kayla needs for join with other RMS layers
with seg_crash_ids as (
	select cpr.crn, concat(st_rt_no::int, cty_code::int, seg_no::int) as concatid, nlf_id, nlf_cntl_b, nlf_cntl_e, shape 
	from padot_rms rms
	inner join crash_pa_roadway cpr 
	on concat(cpr.route::int, cpr.county::int, cpr.segment::int) = concat(rms.st_rt_no::int, rms.cty_code::int, rms.seg_no::int)
	where (concat(st_rt_no, cty_code, seg_no) ~* '[a-z]') is false
	and district_n::int = 6
	order by concatid desc
	)
select 
	a.concatid,
	a.nlf_id,
	a.nlf_cntl_b,
	a.nlf_cntl_e,
	a.shape,
	sum(b.person_count) as person_count, 
	sum(b.vehicle_count) as vehicle_count, 
	sum(b.automobile_count) as automobile_count, 
  sum(b.fatal_count) as fatal_count,
	sum(b.tot_inj_count) as total_inj_count, 
	sum(b.maj_inj_count) as maj_inj_count,
	sum(b.mod_inj_count) as mod_inj_count, 
	sum(b.ped_count) as ped_count,
	sum(b.ped_death_count) as ped_death_count, 
	sum(b.ped_maj_inj_count) as ped_maj_inj_count, 
	sum(b.nonmotr_count) as nonmotr_count, 
	sum(b.nonmotr_death_count) as nonmotr_death_count, 
	sum(b.nonmotr_susp_serious_inj_count) as nonmotr_susp_serious_inj_count,
	sum(b.bicycle_count) as bicycle_count, 
	sum(b.bicycle_death_count) as bicycle_death_count,
	sum(b.bicycle_maj_inj_count) as bicycle_maj_inj_count, 
	sum(b.bicycle_susp_serious_inj_count) as bicycle_susp_serious_inj_count
from seg_crash_ids a
inner join crash_pennsylvania b 
on a.crn = b.crn
group by a.concatid, a.nlf_id, a.nlf_cntl_b, a.nlf_cntl_e, a.shape
order by a.concatid
