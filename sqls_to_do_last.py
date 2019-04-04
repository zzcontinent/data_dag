# !/usr/bin/env python
# -*- coding: utf-8 -*-
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

str_sql = [
    {
        'TITLE': '1 生成加盟店名单jff_ent_tmp.zhouxinxin_league_name_list',
        'SQL': '''
    insert into jff_ent_tmp.zhouxinxin_league_name_list(name_list_id, jff_name_list_id, ent_id,
ent_short_name, sp_id, sp_short_name, labor_sp_id, labor_sp_short_name, order_id, id_card_num,
interview_date, interview_status, work_card_num, work_status, entry_date, leave_date)
SELECT a.name_list_id, a.jff_name_list_id, a.ent_id, d.ent_short_name, c.sp_id, c.league_name,
e.real_b_sp_id, f.sp_short_name, a.rcrt_order_id, upper(a.id_card_num), a.intv_dt,
a.intv_sts, a.work_card_no, a.work_sts, a.entry_dt, a.leave_dt
FROM (
	select name_list_id, jff_name_list_id, ent_id, id_card_num, if(work_card_no='', null, work_card_no) as work_card_no,
	intv_dt, intv_sts, work_sts, entry_dt, leave_dt, rcrt_order_id, srce_sp_id, trgt_sp_id,
	if(@intv_dt=temp.intv_dt and @id_card_num=temp.id_card_num, @rank := @rank+1, @rank := 1) as rank,
	@intv_dt := temp.intv_dt,  @id_card_num := temp.id_card_num
	from (
		select name_list_id, jff_name_list_id, ent_id, upper(id_card_num) as id_card_num, trgt_sp_id,
		work_card_no, intv_dt, intv_sts, work_sts, entry_dt, leave_dt, rcrt_order_id, srce_sp_id
		from `ods_zxx_2.0`.name_list
		where is_deleted = 1 
		order by intv_dt, id_card_num, name_list_id desc
		) temp,
		(select @intv_dt := null, @id_card_num := null, @rank := 0) r
) a
left join `ods_zxx_2.0`.sp_mapping b on a.srce_sp_id = b.virtual_sp_id
left join ods_jff.league_store c on b.real_sp_id = c.sp_id
left join `ods_zxx_2.0`.ent d on a.ent_id = d.ent_id
left join ods_jff.sp_real_coop e on a.trgt_sp_id = e.b_sp_id
left join ods_jff.sp f on e.real_b_sp_id = f.sp_id
where a.intv_dt >= c.coop_start_dt and a.rank = 1
and c.sp_id != 1006;
    '''
    },
    {
        'TITLE': '2.1 根据周薪薪的周月薪名单补充入职状态, 月+周账单里存在的，就是在职，没有的就是离职',
        'SQL': '''
        update jff_ent_tmp.zhouxinxin_league_name_list a
inner join (
	select ent_id, if(work_card_no='', null, work_card_no) as work_card_num, id_card_num, entry_dt as entry_date, leave_dt as leave_date,
	if(@entry_dt=temp.entry_dt and @id_card_num=temp.id_card_num, @rank := @rank+1, @rank := 1) as rank,
	@entry_dt := temp.entry_dt, @id_card_num := temp.id_card_num
	from (
		SELECT upper(id_card_num) as id_card_num, work_card_no, entry_dt, ent_id, max(leave_dt) as leave_dt, count(1) as num 
		FROM `ods_zxx_2.0`.bill_monthly_batch_import_detail
		where entry_dt > '1970-01-01'
		group by ent_id, upper(id_card_num), work_card_no, entry_dt
		order by ent_id, id_card_num, entry_dt, num desc
	)temp,
	(select @entry_dt := null, @id_card_num := null, @rank := 0) r
) b on b.rank = 1 and a.id_card_num = b.id_card_num and coalesce(a.fixed_work_card_num, a.work_card_num) = b.work_card_num
set a.hire_status = 1
where a.interview_date >= '${MIN_INTV_DT}';'''
    },
    {
        'TITLE': '2.2 根据周薪薪的周月薪名单补充入职状态, 月+周账单里存在的，就是在职，没有的就是离职',
        'SQL': '''
        update jff_ent_tmp.zhouxinxin_league_name_list a
inner join (
	select ent_id, if(work_card_no='', null, work_card_no) as work_card_num, id_card_num, entry_dt as entry_date, leave_dt as leave_date,
	if(@entry_dt=temp.entry_dt and @id_card_num=temp.id_card_num, @rank := @rank+1, @rank := 1) as rank,
	@entry_dt := temp.entry_dt, @id_card_num := temp.id_card_num
	from (
		SELECT upper(id_card_num) as id_card_num, work_card_no, entry_dt, ent_id, max(leave_dt) as leave_dt, count(1) as num 
		FROM `ods_zxx_2.0`.bill_weekly_batch_import_detail
		where entry_dt > '1970-01-01'
		group by ent_id, upper(id_card_num), work_card_no, entry_dt
		order by ent_id, id_card_num, entry_dt, num desc
	)temp,
	(select @entry_dt := null, @id_card_num := null, @rank := 0) r
) b on b.rank = 1 and a.id_card_num = b.id_card_num and coalesce(a.fixed_work_card_num, a.work_card_num) = b.work_card_num
set a.hire_status = 1
where a.interview_date >= '${MIN_INTV_DT}';

update jff_ent_tmp.zhouxinxin_league_name_list
set hire_status = 0
where hire_status is null
and interview_date >= '${MIN_INTV_D}' '''
    },
    {
        'TITLE': '3 抽取字段生成简易加盟店名单jff_ent_tmp.jff_cb_name_list',
        'SQL': '''
        insert data into jff_cb_name_list：
SELECT distinct a.name_list_id, a.mbr_id_card_num,
c.sp_id,  a.intv_dt, a.rcrt_typ,
a.intv_sts, b.hire_status
FROM `ods_jff`.name_list a
left join ods_jff.league_store c on a.sp_id = c.sp_id
left join jff_ent_tmp.zhouxinxin_league_name_list b
on upper(a.mbr_id_card_num)=b.id_card_num and a.intv_dt=b.interview_date
where a.intv_dt >= c.coop_start_dt
and a.intv_dt <= CURRENT_DATE
and c.sp_id != 1006 and a.is_deleted = 0; '''
    },
    {
        'TITLE': '4 生成统计结果',
        'SQL': '''
        select intv_dt, DATE_FORMAT(intv_dt, '%Y-%m') as interview_month, 
count(*) as d_total,
count(rcrt_typ=2 or null) as d_zxx_total, 
count(intv_sts=2 or null) as d_intv,
count(rcrt_typ=2 and intv_sts=2 or null) as d_zxx_intv, 
count(rcrt_typ=2 and intv_sts=2 and hire_status=1 or null) as d_zxx_hire, 
count(rcrt_typ=0 or null) as d_nzxx_total, 
count(rcrt_typ=0 and intv_sts=2 or null) as d_nzxx_intv
from jff_ent_tmp.jff_cb_name_list
group by intv_dt '''
    }

]
