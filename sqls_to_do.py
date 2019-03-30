# !/usr/bin/env python
# -*- coding: utf-8 -*-
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

str_sql = [
    #     '''
    #     INSERT INTO transit_jff.direct_store_name_list_sum(sp_id, league_id, league_name, ent_id, ent_short_name, sp_ent_id, sum_wage_min, sum_wage_max, intv_cnt)
    # SELECT a.sp_id,
    #        b.league_id,
    #        b.league_name,
    #        d.ent_id,
    #        d.ent_short_name,
    #        a.sp_ent_id,
    #        d.sum_wage_min,
    #        d.sum_wage_max,
    #        count(1) AS intv_cnt
    # FROM ods_jff.name_list a
    # LEFT JOIN ods_jff.league_store b ON a.sp_id = b.sp_id
    # LEFT JOIN ods_jff.sp_ent c ON a.sp_ent_id = c.sp_ent_id
    # LEFT JOIN ods_jff.ent d ON c.ent_id = d.ent_id
    # WHERE a.is_deleted = 0 -- and b.coop_mode = 3
    # AND b.is_deleted = 0
    #   AND d.ent_id > 0
    #   AND a.intv_dt >= date_sub(current_date(), interval 1 MONTH)
    # GROUP BY a.sp_id,
    #          b.league_id,
    #          b.league_name,
    #          d.ent_id,
    #          d.ent_short_name,
    #          a.sp_ent_id,
    #          d.sum_wage_min,
    #          d.sum_wage_max;
    #     ''',

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
        ''',
    '''truncate table transit_jff.ent_name_list_sum;

insert into transit_jff.ent_name_list_sum(ent_id, ent_short_name, id_card_num, sp_id, sp_short_name, intv_sts, intv_cnt)
SELECT d.ent_id, d.ent_short_name, a.mbr_id_card_num as id_card_num, b.sp_id, b.sp_short_name, a.intv_sts, count(1) as intv_cnt
FROM (
	select intv_dt, mbr_id_card_num, sp_id, sp_ent_id, intv_sts, is_deleted,
	if(@id_card_num=temp.mbr_id_card_num and @sp_id=temp.sp_id and @intv_dt=temp.intv_dt, @rank := @rank+1, @rank := 1) as rank,
	@id_card_num := temp.mbr_id_card_num as id_card_num,  @intv_dt := temp.intv_dt, @sp_id := temp.sp_id
	from (
		select a.intv_dt, a.mbr_id_card_num, a.sp_id, a.sp_ent_id, a.intv_sts, a.is_deleted 
		from ods_jff.name_list a
		inner join (
			select distinct mbr_id_card_num, sp_id, sp_ent_id from transit_jff.temp_name_list_updated
		) b
		on a.sp_id = b.sp_id and a.sp_ent_id = b.sp_ent_id and a.mbr_id_card_num = b.mbr_id_card_num
		where a.is_deleted = 0 and a.mbr_id_card_num is not null and a.mbr_id_card_num != '' and a.intv_dt < current_date()
		and a.sp_id != 9172 and a.sp_id != 1006 and (a.trgt_sp_id = 0 or a.trgt_sp_id is null)
		order by a.mbr_id_card_num, a.sp_id, a.intv_dt, a.created_tm desc
	) temp,
	(select @id_card_num := null, @sp_id := null, @intv_dt := null, @rank := 0) r
) a
inner join ods_jff.sp b
on a.sp_id = b.sp_id
inner join ods_jff.sp_ent c
on a.sp_ent_id = c.sp_ent_id
inner join ods_jff.ent d
on c.ent_id = d.ent_id
where a.rank = 1
group by d.ent_id, d.ent_short_name, a.mbr_id_card_num, b.sp_id, b.sp_short_name, a.intv_sts;
    '''
]
