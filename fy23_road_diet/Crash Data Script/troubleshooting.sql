select * from padot_rms 
where st_rt_no ~* '[a-z]' is true
and district_n::int = 6
and lane_cnt > 2
and (cur_aadt/10) <1250