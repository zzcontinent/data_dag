# !/usr/bin/env python
# -*- coding: utf-8 -*-
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

str_sql = [
    '''
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
    ''',

    '''select IDCardNum as id_card_num, concat(AdscriptionMonth, '-01') as bill_related_mo, floor(sum(Amount)*100) as wages, 1 as zxx_type
 from `ods_zhouxinxin`.wb_withdrawapply 
 where AuditStatus=1 and AccountStatus=1 and ApplyType=1 and AdscriptionMonth<'2019-02-01'
 group by IDCardNum, AdscriptionMonth''',
    '''
    SELECT 
    direct_store_name_list_sum_id,
    sp_id,
    league_id,
    league_name,
    ent_id,
    ent_short_name,
    sp_ent_id,
    sum_wage_min,
    sum_wage_max,
    intv_cnt,
    created_tm,
    created_by,
    updated_tm,
    updated_by,
    is_deleted
FROM
    transit_jff.direct_store_name_list_sum;
    '''
]
