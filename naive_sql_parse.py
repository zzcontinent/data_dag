# !/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import traceback
from pyutils.misc import func_name
import sqlparse

reload(sys)
sys.setdefaultencoding('utf-8')

# 输入: sql
# 输出: select语句格式
# node :
# {
# ----------------------------------------------------------表
#   direct_store_name_list_sum : { 'desc':'表'},
#   name_list : { 'desc':'表'},
#   league_store : { 'desc':'表'},
#   sp_ent : { 'desc':'表'},
#   ent : { 'desc':'表'},
# ----------------------------------------------------------字段
#   ----------------------------------------------insert
#   sp_id : { 'desc':'字段'},
#   league_id : { 'desc':'字段'},
#   league_name : { 'desc':'字段'},
#   ent_id : { 'desc':'字段'},
#   ent_short_name : { 'desc':'字段'},
#   sp_ent_id : { 'desc':'字段'},
#   sum_wage_min : { 'desc':'字段'},
#   sum_wage_max : { 'desc':'字段'},
#   intv_cnt : { 'desc':'字段'},
#   ----------------------------------------------select
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
str_sql = '''
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


#
# str_sql = '''SELECT
# 	a.area_id,
# 	a.area_code,
# 	a.area_name,
# 	b.area_id,
# 	b.area_code,
# 	b.area_name
# FROM
# 	area as a
# 	JOIN area as b ON ( a.area_id=b.area_id)
# 	limit 10'''


@func_name()
def meta_select_parse(sql):
    sql_format = sqlparse.format(sql, reindent=True, keyword_case='upper')
    sql_format = sql_format.replace('LEFT', '')
    sql_format = sql_format.replace('RIGHT', '')
    sql_format = sql_format.replace('INNER', '')
    sql_sep = sql_format.split('\n')
    print(sql_format)

    COLUMN = []
    TABLE = []

    TABLE_AS = {}
    COLUMN_AS = {}

    STATE_WORDS_PASS = ['SELECT', 'FROM', 'JOIN', 'AS']
    STATE_WORDS_IGNORE = ['ON', 'WHERE', 'ORDER BY', 'DESC', 'ASC']
    STACK_STATE = []
    STACK_WORDS = []
    NOW_STATE = ''
    LAST_STATE = ''

    for sql_line in sql_sep:
        line_words = sql_line.strip().strip(',').split(' ')
        for v in line_words:
            if v in STATE_WORDS_PASS or v in STATE_WORDS_IGNORE:
                STACK_STATE.append(v)
                LEN_STATE = len(STACK_STATE)
                NOW_STATE = STACK_STATE[LEN_STATE - 1]
                LAST_STATE = NOW_STATE if LEN_STATE - 2 < 0 else STACK_STATE[LEN_STATE - 2]
                continue
            STACK_WORDS.append(v)
            LEN_WORD = len(STACK_WORDS)
            NOW_WORD = STACK_WORDS[LEN_WORD - 1]
            LAST_WORD = NOW_WORD if LEN_WORD - 2 < 0 else STACK_WORDS[LEN_WORD - 2]
            if NOW_STATE in STATE_WORDS_IGNORE:
                continue
            if NOW_STATE == 'SELECT':
                COLUMN.append(NOW_WORD)
            elif NOW_STATE == 'AS' and LAST_STATE == 'SELECT':
                COLUMN_AS[NOW_WORD] = LAST_WORD
            elif NOW_STATE == 'FROM':
                TABLE.append(NOW_WORD)
            elif NOW_STATE == 'JOIN':
                TABLE.append(NOW_WORD)
            elif NOW_STATE == 'AS' and LAST_STATE == 'FROM':
                TABLE_AS[NOW_WORD] = LAST_WORD
            elif NOW_STATE == 'AS' and LAST_STATE == 'JOIN':
                TABLE_AS[NOW_WORD] = LAST_WORD

    # print(STACK_STATE)
    # print('-' * 20)
    # print(STACK_WORDS)
    # print('-' * 20)
    print(COLUMN)
    print('-' * 20)
    print(TABLE)
    print('-' * 20)
    print(TABLE_AS)
    print('-' * 20)
    print(COLUMN_AS)


def main():
    meta_select_parse(str_sql)
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
