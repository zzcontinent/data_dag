# !/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import traceback
from pyutils.misc import func_name

reload(sys)
sys.setdefaultencoding('utf-8')


# 输入: sql
# 输出: select语句格式
# node :
# {
#   ods_jff : { 'desc':'数据库'},
#   transit_jff : { 'desc':'数据库'},

#   direct_store_name_list_sum : { 'desc':'表'},

#   name_list : { 'desc':'表'},
#   league_store : { 'desc':'表'},
#   sp_ent : { 'desc':'表'},
#   ent : { 'desc':'表'},
#   insert ----------------------------------------------
#   sp_id : { 'desc':'字段'},
#   league_id : { 'desc':'字段'},
#   league_name : { 'desc':'字段'},
#   ent_id : { 'desc':'字段'},
#   ent_short_name : { 'desc':'字段'},
#   sp_ent_id : { 'desc':'字段'},
#   sum_wage_min : { 'desc':'字段'},
#   sum_wage_max : { 'desc':'字段'},
#   intv_cnt : { 'desc':'字段'},
#   select ----------------------------------------------
#   sp_id : { 'desc':'字段'},
#   league_id : { 'desc':'字段'},
#   league_name : { 'desc':'字段'},
#   ent_id : { 'desc':'字段'},
#   ent_short_name : { 'desc':'字段'},
#   sp_ent_id : { 'desc':'字段'},
#   sum_wage_min : { 'desc':'字段'},
#   sum_wage_max : { 'desc':'字段'},
#   count(1) : { 'desc':'字段'},
# }
# 输出: insert语句格式 ods_jff.league_store.
@func_name('解析sql输入输出')
def parse(sql):
    sql = '''
insert into transit_jff.direct_store_name_list_sum(sp_id, league_id, league_name,
ent_id, ent_short_name, sp_ent_id, sum_wage_min, sum_wage_max, intv_cnt)
select a.sp_id, b.league_id, b.league_name, d.ent_id, d.ent_short_name, a.sp_ent_id,
d.sum_wage_min, d.sum_wage_max, count(1) as intv_cnt
from ods_jff.name_list a
left join ods_jff.league_store b on a.sp_id = b.sp_id
left join ods_jff.sp_ent c on a.sp_ent_id = c.sp_ent_id
left join ods_jff.ent d on c.ent_id = d.ent_id
where a.is_deleted = 0 -- and b.coop_mode = 3 
and b.is_deleted = 0 and d.ent_id > 0 and a.intv_dt >= date_sub(current_date(), interval 1 month)
group by a.sp_id, b.league_id, b.league_name, d.ent_id, d.ent_short_name, a.sp_ent_id,
d.sum_wage_min, d.sum_wage_max;'''


def main():
    pass


if __name__ == '__main__':
    try:
        main()
        print("处理成功")
    except Exception as e:
        print(e)
        print(traceback.format_exc())
        print("处理失败!!!")
        raise
    finally:
        pass
