alter table countythresholds add column if not exists group_crash_totals int;
update countythresholds 
set group_crash_totals = (select st_collect(st_linemerge(geom)), sum(total_cras) from countythresholds c)
where vphpd between 30 and 850

select st_collect(st_linemerge(geom)), sum(total_cras) from countythresholds c 
where vphpd between 30 and 850
and concatid::int between 20016730 and 20016751


select sum(total_cras),
    ST_COLLECTIONEXTRACT(
        UNNEST(ST_CLUSTERINTERSECTING(geom)),
        2
    ) AS geom
FROM countythresholds 
where vphpd between 30 and 850
and concatid::int between 20016730 and 20016751



select geom, sum(total_cras) from countythresholds 
where st_rt_no::int = 2005
and (vphpd = 846
or vphpd = 778
or vphpd = 740)
group by substring(concatid, -1, (length(concatid)+1)), geom


select sum(total_cras) from countythresholds 
where vphpd between 30 and 850
group by concatid, substring(concatid, -1, (length(concatid)+1))
order by sum(total_cras)


select st_collect(st_linemerge(geom)), sum(total_cras) from countythresholds c 
where concatid::int between 20016730 and 20016751
and concatid like '%0'


select st_collect(st_linemerge(geom)) as geom, sum(total_cras)  from countythresholds c 
where concatid::int between 20016730 and 20016751
and concatid like '%1'
group by st_touches(geom,geom)

SELECT sum(total_cras), unnest(ST_ClusterIntersecting(geom))
FROM countythresholds
where vphpd between 30 and 850
and concat(st_rt_no, cty_code) = '200167'
GROUP by concat(st_rt_no, cty_code)


create or replace view review as 
SELECT ARRAY_AGG(q.concatid::int) AS id_agg,
       ST_Collect(geom) AS geom, sum(q.total_cras)
FROM   (
  SELECT *,
         ST_ClusterDBSCAN(geom, 0, 1) OVER() AS _clst
  FROM   countythresholds 
  where vphpd between 30 and 850
) q
GROUP BY
  _clst
;


update countythresholds b
set group_crash_totals = a."sum"
from review a
where b.concatid::int = any (a.id_agg)  