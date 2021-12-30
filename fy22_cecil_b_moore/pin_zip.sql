select zcta5ce, geom, count(pj.q6)
from tl_pennsylvania5digit2009 tpd 
inner join pins_joined pj 
on pj.q6 = tpd.zcta5ce 
group by zcta5ce, tpd.geom