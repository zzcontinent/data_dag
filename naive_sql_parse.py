# !/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import traceback
from pyutils.misc import func_name
import sqlparse
import dag_conf
import datetime

reload(sys)
sys.setdefaultencoding('utf-8')

str_sql = ['''
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
group by IDCardNum, AdscriptionMonth''']


@func_name()
def meta_select_parse(sql):
    sql_format = sqlparse.format(sql, reindent=True, keyword_case='upper')
    sql_format = sql_format.replace('LEFT', '')
    sql_format = sql_format.replace('RIGHT', '')
    sql_format = sql_format.replace('INNER', '')
    sql_sep = sql_format.split('\n')
    print(sql_format)

    STACK_ALL = []
    i_STATE = []

    COLUMN = []
    TABLE = []
    SQL = sql[:len(sql) / 4]

    TABLE_AS = {}
    COLUMN_AS = {}
    MAP_COLUMN_TABLE = {}
    COLUMN_BELONG_SQL = []

    STATE_WORDS_PASS = ['SELECT', 'AS', 'FROM', 'JOIN', 'ON', 'WHERE']
    STATE_WORDS_IGNORE = ['WHERE', 'ORDER BY', 'DESC', 'ASC']

    # 提取出单词+状态位置
    for sql_line in sql_sep:
        line_words = sql_line.strip().strip(',').split(' ')
        # 因为括号内允许空格，所以要合并有基数个括号的相邻word
        line_words_balance = []
        balance_cnt = 0
        balance_word = ''
        for i in range(len(line_words)):
            for cha in line_words[i]:
                if cha == '(':
                    balance_cnt += 1
                elif cha == ')':
                    balance_cnt -= 1
            balance_word += line_words[i]
            if balance_cnt == 0:
                line_words_balance.append(balance_word)
                balance_word = ''

        for v in line_words_balance:
            STACK_ALL.append(v)
            if v in STATE_WORDS_PASS:
                i_STATE.append(len(STACK_ALL) - 1)

    # 提取出COLUMN位置，字段别名，计算字段别名映射
    for i in range(len(i_STATE) - 1):
        i_state_x = i_STATE[i]
        i_state_y = i_STATE[i + 1]
        state_x = STACK_ALL[i_state_x]
        state_y = STACK_ALL[i_state_y]
        seg = STACK_ALL[i_state_x + 1:i_state_y]
        if state_x == 'SELECT' and state_y == 'FROM':
            COLUMN += seg
        elif state_x == 'SELECT' and state_y == 'AS':
            COLUMN += seg
        elif state_x == 'AS' and state_y == 'AS':
            COLUMN += seg
            COLUMN_AS[STACK_ALL[i_state_x - 1]] = STACK_ALL[i_state_x + 1]
        elif state_x == 'AS' and state_y == 'FROM':
            COLUMN += seg
            COLUMN_AS[STACK_ALL[i_state_x - 1]] = STACK_ALL[i_state_x + 1]

    # 提取出TABLE位置，别名，计算表与别名映射
    for i in range(len(i_STATE) - 1):
        i_state_x = i_STATE[i]
        i_state_y = i_STATE[i + 1]
        state_x = STACK_ALL[i_state_x]
        state_y = STACK_ALL[i_state_y]
        seg = STACK_ALL[i_state_x + 1:i_state_y]
        if state_x == 'FROM' and state_y == 'WHERE':
            TABLE += seg
            if len(seg) > 1:
                TABLE_AS[seg[0]] = seg[1]
        elif state_x == 'FROM' and state_y == 'JOIN':
            TABLE += seg
            if len(seg) > 1:
                TABLE_AS[seg[0]] = seg[1]
        elif state_x == 'AS' and state_y == 'JOIN':
            TABLE += seg
            TABLE_AS[STACK_ALL[i_state_x - 1]] = STACK_ALL[i_state_x + 1]
        elif state_x == 'FROM' and state_y == 'AS':
            TABLE += seg
        elif state_x == 'AS' and state_y == 'WHERE':
            TABLE += seg
            TABLE_AS[STACK_ALL[i_state_x - 1]] = STACK_ALL[i_state_x + 1]
        elif state_x == 'JOIN' and state_y == 'AS':
            TABLE += seg
        elif state_x == 'AS' and state_y == 'ON':
            TABLE += seg
            TABLE_AS[STACK_ALL[i_state_x - 1]] = STACK_ALL[i_state_x + 1]
        elif state_x == 'JOIN' and state_y == 'ON':
            TABLE += seg
            if len(seg) > 1:
                TABLE_AS[seg[0]] = seg[1]
    # 1删除select字段表名
    COLUMN_PURE = []
    for i in range(len(COLUMN)):
        pos = COLUMN[i].find('.')
        COLUMN_PURE.append(COLUMN[i][pos + 1:])
    # 2删除表名中字段别名
    TABLE_PURE = []
    for tmp_table in TABLE:
        find_cnt = 0
        for k, v in TABLE_AS.items():
            if v == tmp_table:
                find_cnt += 1
        if find_cnt == 0:
            TABLE_PURE.append(tmp_table)

    # 提取表与字段映射 MAP_TABLE_COLUMN
    for col in COLUMN:
        cols = col.split('.')
        if len(cols) > 1:
            col_k = cols[0]
            col_v = cols[1]
            for k, v in TABLE_AS.items():
                if v == col_k:
                    # # 如果是别名，选择一个到字段->表映射中
                    if col_v not in COLUMN_AS.values():
                        MAP_COLUMN_TABLE[col_v] = k

    # 3计算非来自TABLE的字段关系映射为整个SQL
    for v in COLUMN_PURE:
        if v not in MAP_COLUMN_TABLE.keys() and v not in COLUMN_AS.values():
            COLUMN_BELONG_SQL.append(v)
    # 4如果只有一张表，则所有非关联字段属于这张表
    if len(TABLE_PURE) == 1:
        for v in COLUMN_BELONG_SQL:
            MAP_COLUMN_TABLE[v] = TABLE_PURE[0]
        COLUMN_BELONG_SQL = []

    print('COLUMN', COLUMN)
    print('COLUMN_PURE', COLUMN_PURE)
    print('COLUMN_BELONG_SQL', COLUMN_BELONG_SQL)
    print('COLUMN_AS', COLUMN_AS)
    print('-' * 20)

    print('TABLE', TABLE)
    print('TABLE_PURE', TABLE_PURE)
    print('TABLE_AS', TABLE_AS)
    print('-' * 20)

    print('MAP_COLUMN_TABLE', MAP_COLUMN_TABLE)
    return COLUMN_PURE, COLUMN_AS, COLUMN_BELONG_SQL, TABLE_PURE, MAP_COLUMN_TABLE


@func_name()
def select_sql_to_conf(sql, colums_nodes, table_nodes, map_colmn_column, map_column_table, column_belong_sql):
    add_node = dict()
    node_dict = dict()

    node_dict['ts'] = {
        'name': '开始',
        'desc': '''开始''',
        'color': 'red'
    }
    add_node['dag'] = '''ts >> 1
    '''

    add_node['title'] = 'sql_%s' % datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    node_dict[1] = {
        'name': 'SQL',
        'desc': sql,
        'color': 'lightblue'
    }
    node_id = 2
    map_table_nodes_id = {}
    map_column_nodes_id = {}
    dag_str = ''
    for table in table_nodes:
        node_dict[node_id] = {'name': table, 'desc': '表', 'color': 'blue'}
        map_table_nodes_id[table] = node_id
        dag_str += '\n' + '1 >> %d' % node_id
        node_id += 1
    for col in colums_nodes:
        node_dict[node_id] = {'name': col, 'desc': '字段', 'color': 'orange'}
        map_column_nodes_id[col] = node_id
        node_id += 1

    for k, v in map_colmn_column.items():
        dag_str += '\n' + '%d >> %d' % (map_column_nodes_id[k], map_column_nodes_id[v])
        node_dict[map_column_nodes_id[v]]['desc'] = '字段别名'
    for k, v in map_column_table.items():
        dag_str += '\n' + '%d >> %d' % (map_table_nodes_id[v], map_column_nodes_id[k])

    for col in column_belong_sql:
        dag_str += '\n' + '%d >> 1' % map_column_nodes_id[col]

    add_node['dag'] += dag_str
    add_node['nodes'] = node_dict

    with open('./dag_conf.py', 'r+') as fp:
        reads = fp.read().strip().strip(']') + '\n %s' % add_node + ',]'
        fp.seek(0)
        fp.truncate()
        fp.write(reads)
    print add_node


def main():
    sql_str_tmp = str_sql[1]
    COLUMN_PURE, COLUMN_AS, COLUMN_BELONG_SQL, TABLE_PURE, MAP_COLUMN_TABLE = meta_select_parse(sql_str_tmp)
    select_sql_to_conf(sql_str_tmp, COLUMN_PURE, TABLE_PURE, COLUMN_AS, MAP_COLUMN_TABLE, COLUMN_BELONG_SQL)
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
