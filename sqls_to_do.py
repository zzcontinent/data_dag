# !/usr/bin/env python
# -*- coding: utf-8 -*-
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

str_sql = [
    {
        'TITLE': 'jff_online_service中一个案例',
        'SQL': '''
    INSERT INTO transit_jff.direct_store_name_list_sum(sp_id, league_id, league_name, ent_id, ent_short_name, sp_ent_id, sum_wage_min, sum_wage_max, intv_cnt)
SELECT a.sp_id,
       b.league_id,
       b.league_name,
       d.ent_id,
       d.ent_short_name,
       a.sp_ent_id,
       d.sum_wage_min,
       d.sum_wage_max,
       count(1) AS intv_cnt
FROM ods_jff.name_list a
LEFT JOIN ods_jff.league_store b ON a.sp_id = b.sp_id
LEFT JOIN ods_jff.sp_ent c ON a.sp_ent_id = c.sp_ent_id
LEFT JOIN ods_jff.ent d ON c.ent_id = d.ent_id
WHERE a.is_deleted = 0 -- and b.coop_mode = 3
AND b.is_deleted = 0
  AND d.ent_id > 0
  AND a.intv_dt >= date_sub(current_date(), interval 1 MONTH)
GROUP BY a.sp_id,
         b.league_id,
         b.league_name,
         d.ent_id,
         d.ent_short_name,
         a.sp_ent_id,
         d.sum_wage_min,
         d.sum_wage_max;
    '''
    },
    {
        'TITLE': '周鑫案例1',
        'SQL': '''
        SELECT user_sk, count(distinct (intv_dt_sk, ent_sk)) as intv_cnt, 
round(count(distinct (case when intv_sts=2 then (intv_dt_sk, ent_sk) else null end))/(count(distinct (intv_dt_sk, ent_sk))+0),2) as intv_sccs_rt, 
round(count(distinct (case when fdbk_sts=2 then (intv_dt_sk, ent_sk) else null end))/(count(distinct (intv_dt_sk, ent_sk))+0),2) as hr_rt,
case when count(fdbk_sts=2 or null)=0 then count(distinct (intv_dt_sk, ent_sk)) else
round((count(distinct (intv_dt_sk, ent_sk))+0.0)/count(distinct (case when fdbk_sts=2 then (intv_dt_sk, ent_sk) else null end))) end as hr_per_intv_cnt
, max(event_tm) as max_intv_dt, min(event_tm) as min_intv_dt,
date_part('day', CURRENT_TIMESTAMP-min(event_tm))+1 as intv_dys,
round(count(distinct (intv_dt_sk, ent_sk))/(date_part('year', CURRENT_TIMESTAMP-min(event_tm))+1)::numeric,2) as intv_frq,
round(count(distinct (case when is_inner<2 then (intv_dt_sk, ent_sk) else null end))/count(distinct (intv_dt_sk, ent_sk))::numeric,2) as zxx_intv_rt,
min(case when is_inner<2 then event_tm else CURRENT_DATE end) as first_intv_zxx_tm
FROM dm."fact_interview"
where is_deleted=0 and user_sk!=''
group by user_sk'''
    }

]
