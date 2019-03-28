# !/usr/bin/env python
# -*- coding: utf-8 -*-
'''
1.解析sql，得到SELECT语句的表+字段+别名+依赖关系
2.通过1，生成dag_conf.py里的配置文件
3.通过gen_html.py检测dag_conf.py是否变动，从而重新生成./dist/demo/血缘分析DAG.html
'''
import sys
import traceback
from pyutils.misc import func_name
import sqlparse
import datetime
import sqls_to_do

reload(sys)
sys.setdefaultencoding('utf-8')


@func_name()
def meta_select_parse(sql):
    sql_format = sqlparse.format(sql, reindent=True, keyword_case='upper')
    sql_format = sql_format.replace('LEFT', '')
    sql_format = sql_format.replace('RIGHT', '')
    sql_format = sql_format.replace('INNER', '')
    sql_format = sql_format.strip().strip('\n').strip()
    sql_sep = sql_format.split('\n')
    print(sql_format)

    STACK_ALL = []
    i_STATE = []

    COLUMN = []
    TABLE = []
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
                line_words_balance.append(balance_word.strip().strip(';'))
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
        seg_x_y = STACK_ALL[i_state_x + 1:i_state_y]
        if state_x == 'SELECT' and state_y == 'FROM':
            COLUMN += seg_x_y
        elif state_x == 'SELECT' and state_y == 'AS':
            COLUMN += seg_x_y
        elif state_x == 'AS' and state_y == 'AS':
            COLUMN += seg_x_y
            COLUMN_AS[STACK_ALL[i_state_x - 1]] = STACK_ALL[i_state_x + 1]
        elif state_x == 'AS' and state_y == 'FROM':
            COLUMN += seg_x_y
            COLUMN_AS[STACK_ALL[i_state_x - 1]] = STACK_ALL[i_state_x + 1]

    # 提取出TABLE位置，别名，计算表与别名映射
    for i in range(len(i_STATE) - 1):
        i_state_x = i_STATE[i]
        i_state_y = i_STATE[i + 1]
        state_x = STACK_ALL[i_state_x]
        state_y = STACK_ALL[i_state_y]
        seg_x_y = STACK_ALL[i_state_x + 1:i_state_y]
        seg_y = STACK_ALL[i_state_y+1:]
        if i == len(i_STATE) - 2 and state_y == 'FROM':
            TABLE += seg_y
            if len(seg_y) > 1:
                TABLE_AS[seg_y[0]] = seg_y[1]
        elif state_x == 'FROM' and state_y == 'WHERE':
            TABLE += seg_x_y
            if len(seg_x_y) > 1:
                TABLE_AS[seg_x_y[0]] = seg_x_y[1]
        elif state_x == 'FROM' and state_y == 'JOIN':
            TABLE += seg_x_y
            if len(seg_x_y) > 1:
                TABLE_AS[seg_x_y[0]] = seg_x_y[1]
        elif state_x == 'AS' and state_y == 'JOIN':
            TABLE += seg_x_y
            TABLE_AS[STACK_ALL[i_state_x - 1]] = STACK_ALL[i_state_x + 1]
        elif state_x == 'FROM' and state_y == 'AS':
            TABLE += seg_x_y
        elif state_x == 'AS' and state_y == 'WHERE':
            TABLE += seg_x_y
            TABLE_AS[STACK_ALL[i_state_x - 1]] = STACK_ALL[i_state_x + 1]
        elif state_x == 'JOIN' and state_y == 'AS':
            TABLE += seg_x_y
        elif state_x == 'AS' and state_y == 'ON':
            TABLE += seg_x_y
            TABLE_AS[STACK_ALL[i_state_x - 1]] = STACK_ALL[i_state_x + 1]
        elif state_x == 'JOIN' and state_y == 'ON':
            TABLE += seg_x_y
            if len(seg_x_y) > 1:
                TABLE_AS[seg_x_y[0]] = seg_x_y[1]
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
        dag_str += '\n' + '1 >> %d ' % map_column_nodes_id[col]

    add_node['dag'] += dag_str
    add_node['nodes'] = node_dict

    with open('./dag_conf.py', 'r+') as fp:
        reads = fp.read().strip().strip(']') + '\n %s' % add_node + ',]\n'
        fp.seek(0)
        fp.truncate()
        fp.write(reads)
    print add_node


@func_name()
def reset_dag_conf():
    with open('./dag_conf.py', 'r+') as fp:
        reads = ''.join(fp.readlines()[:330]) + ']\n'
        fp.seek(0)
        fp.truncate()
        fp.write(reads)


@func_name()
def auto_gen():
    reload(sqls_to_do)
    reset_dag_conf()
    for v in sqls_to_do.str_sql:
        COLUMN_PURE, COLUMN_AS, COLUMN_BELONG_SQL, TABLE_PURE, MAP_COLUMN_TABLE = meta_select_parse(v)
        select_sql_to_conf(v, COLUMN_PURE, TABLE_PURE, COLUMN_AS, MAP_COLUMN_TABLE, COLUMN_BELONG_SQL)
    pass


def main():
    auto_gen()


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
