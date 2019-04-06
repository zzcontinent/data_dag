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

meta_sql_words = []
meta_map = {}

# 用于检索sql语句的关键字
STATE_WORDS_PASS = ['SELECT', 'AS', 'FROM', 'JOIN', 'ON', 'WHERE', 'GROUP', 'BY']
# 用于检索前缀表达式的关键字
OPERATOR_BEFORE = ['IF', 'COUNT', 'if', 'count', 'in', 'IN', 'CURRENT_DATE', 'current_date', 'concat', 'sum',
                   'floor', '@', 'ROUND', 'round', 'MIN', 'min', 'MAX', 'max', 'date_part', 'DATE_FORMAT', 'upper']
# 用于检索中缀表达式的关键字
OPERATOR_MIDDLE = [':=', '=', '/', '+', '-', '::']

# OPERATOR_RANGE = ['CASE', 'case', 'end', 'END']
rename_col_cnt = 0
MAP_TABLE_TABLE = {}
COLUMN_AS = {}
COLUMN_PURE = []
MAP_COLUMN_TABLE = {}
TABLE_PURE = []
merge_meta_map = {}


@func_name()
def sql_to_meta_words(sql):
    global meta_sql_words, meta_map
    sql_format = sqlparse.format(sql, reindent=True, keyword_case='upper')
    sql_format = sql_format.replace('"', '')
    sql_format = sql_format.replace('LEFT', '')
    sql_format = sql_format.replace('RIGHT', '')
    sql_format = sql_format.replace('INNER', '')
    sql_format = sql_format.replace('DISTINCT', '')
    sql_format = sql_format.replace('ALL', '')
    sql_format = sql_format.strip().strip('\n').strip()

    for v in OPERATOR_MIDDLE:
        sql_format = sql_format.replace(v, ' %s ' % v)
    sql_format = sql_format.replace(': =', ':=')
    sql_sep = sql_format.replace('\n', ' ').replace(';', ' ').replace(',', ' ').replace('(', ' ( ').replace(')',
                                                                                                            ' ) ').split(
        ' ')
    sql_words = []
    for v in sql_sep:
        if len(v.strip()) > 0:
            sql_words.append(v.strip())

    # 处理括号,存在子查询时需要标记下一次分解
    meta_id = 0
    for word in sql_words:
        meta_sql_words.append(word)
        if ')' == word:
            meta_id += 1
            meta_map_id = '#meta%d' % meta_id
            meta_list = []
            while True:
                if len(meta_sql_words) == 0:
                    break
                top = meta_sql_words.pop()
                meta_list.append(top)
                if '(' == top:
                    meta_map[meta_map_id] = list(reversed(meta_list))
                    meta_sql_words.append(meta_map_id)
                    break
    # # 处理UNION关键字，分割两张表
    # for word in meta_sql_words:
    #     if 'UNION' == word:
    #         meta_id += 1
    #         meta_map_id = '#meta%d' % meta_id
    #         meta_list = []
    #
    pass


@func_name()
def merge_meta_words_to_operator(meta_sql_words, meta_map):
    # 把运算符号相连接的a op b 组合在一起
    global OPERATOR_BEFORE, OPERATOR_MIDDLE

    merge_sql_words = []
    merge_meta_map = meta_map
    i = 0
    while i < len(meta_sql_words):
        merge_sql_words.append(meta_sql_words[i])
        if meta_sql_words[i] in OPERATOR_MIDDLE:
            seg_tmp = meta_sql_words[i - 1:i + 2]
            merge_one = ' '.join(seg_tmp)
            merge_sql_words.pop()
            merge_sql_words.pop()
            is_meta_in = 0
            # 单元与运算符结合，需要单独判断
            if '#meta' in seg_tmp[0]:
                is_meta_in += 1
                merge_sql_words.append(seg_tmp[0])
                merge_meta_map[seg_tmp[0]] = merge_meta_map[seg_tmp[0]] + seg_tmp[1:]
            elif '#meta' in seg_tmp[2]:
                is_meta_in += 1
                merge_sql_words.append(seg_tmp[2])
                merge_meta_map[seg_tmp[2]] = seg_tmp[:2] + merge_meta_map[seg_tmp[2]]

            if is_meta_in == 0:
                merge_sql_words.append(merge_one)

            i += 2
            continue
        elif meta_sql_words[i] in OPERATOR_BEFORE:
            seg_tmp = meta_sql_words[i:i + 2]
            merge_one = ' '.join(seg_tmp)
            merge_sql_words.pop()
            is_meta_in = 0
            for v in seg_tmp:
                if '#meta' in v:
                    is_meta_in += 1
                    merge_sql_words.append(v)
                    merge_meta_map[v] = [meta_sql_words[i]] + merge_meta_map[v]
            if is_meta_in == 0:
                merge_sql_words.append(merge_one)

            i += 2
            continue
        # case end 语句
        elif meta_sql_words[i] == 'END' or meta_sql_words[i] == 'end':
            merge_list = []
            while True:
                if len(merge_sql_words) == 0:
                    break
                top = merge_sql_words.pop()
                merge_list.append(top)
                if 'CASE' in top:
                    # 去掉首尾2个运算符号
                    seg_tmp = list(reversed(merge_list))
                    merge_one = ' '.join(seg_tmp)
                    is_meta_in = 0
                    # seg_tmp中可能有多个#meta，需要生成新的#meta?
                    for v in seg_tmp:
                        if '#meta' in v:
                            is_meta_in += 1
                    if is_meta_in == 1:
                        for v in seg_tmp:
                            if '#meta' in v:
                                merge_sql_words.append(v)
                                merge_meta_map[v] += seg_tmp
                    elif is_meta_in == 0:
                        merge_sql_words.append(merge_one)
                    elif is_meta_in > 1:
                        new_meta_id = '#meta%d' % (len(merge_meta_map) + 1)
                        merge_sql_words.append(new_meta_id)
                        merge_meta_map[new_meta_id] = seg_tmp
                    break
            i += 1
            continue
        else:
            i += 1
            continue

    return merge_sql_words, merge_meta_map


# 递归调用解析sql


@func_name()
def parse_meta_select_v2(node_name, sql_words):
    STACK_ALL = sql_words
    i_STATE = []

    COLUMN = []
    TABLE = []
    TABLE_AS = {}
    COLUMN_AS = {}
    MAP_COLUMN_TABLE = {}

    global STATE_WORDS_PASS
    # 提取出单词+状态位置
    for i in range(len(STACK_ALL)):
        if STACK_ALL[i] in STATE_WORDS_PASS:
            i_STATE.append(i)
    if len(i_STATE) == 0:
        COLUMN.append(' '.join(STACK_ALL))
    # 提取出COLUMN位置，字段别名，计算字段别名映射
    if 1 == len(i_STATE) and 'SELECT' == STACK_ALL[i_STATE[0]]:
        # 只对应 临时表()
        for v in STACK_ALL[i_STATE[0] + 1:]:
            if v not in ['(', ')']:
                COLUMN.append(v)

    for i in range(len(i_STATE) - 1):
        i_state_x = i_STATE[i]
        i_state_y = i_STATE[i + 1]
        state_x = STACK_ALL[i_state_x]
        state_y = STACK_ALL[i_state_y]
        seg_x_y = STACK_ALL[i_state_x + 1:i_state_y]
        if 'SELECT' == state_x and 'FROM' == state_y:
            COLUMN += seg_x_y
        elif 'SELECT' == state_x and 'AS' == state_y:
            COLUMN += seg_x_y
        elif 'AS' == state_x and 'AS' == state_y:
            rename_col = '%s' % (STACK_ALL[i_state_x + 1])
            COLUMN_AS[STACK_ALL[i_state_x - 1]] = rename_col
            COLUMN += STACK_ALL[i_state_x + 2:i_state_y]
        elif 'AS' == state_x and 'FROM' == state_y:
            rename_col = '%s' % (STACK_ALL[i_state_x + 1])
            COLUMN_AS[STACK_ALL[i_state_x - 1]] = rename_col
            COLUMN += STACK_ALL[i_state_x + 2:i_state_y]

    # 提取出TABLE位置，别名，计算表与别名映射
    for i in range(len(i_STATE) - 1):
        i_state_x = i_STATE[i]
        i_state_y = i_STATE[i + 1]
        state_x = STACK_ALL[i_state_x]
        state_y = STACK_ALL[i_state_y]
        seg_x_y = STACK_ALL[i_state_x + 1:i_state_y]
        seg_y = STACK_ALL[i_state_y + 1:]

        if i == len(i_STATE) - 2 and 'FROM' == state_y:
            if seg_y[len(seg_y) - 1] == ')':
                seg_y = seg_y[:len(seg_y) - 1]
            TABLE += seg_y
            i_tmp = 0
            while i_tmp < len(seg_y) - 1:
                TABLE_AS[seg_y[i_tmp]] = seg_y[i_tmp + 1]
                i_tmp += 2
        elif 'FROM' == state_x and 'WHERE' == state_y:
            TABLE += seg_x_y
            i_tmp = 0
            while i_tmp < len(seg_x_y) - 1:
                TABLE_AS[seg_x_y[i_tmp]] = seg_x_y[i_tmp + 1]
                i_tmp += 2
        elif 'FROM' == state_x and 'JOIN' == state_y:
            TABLE += seg_x_y
            i_tmp = 0
            while i_tmp < len(seg_x_y) - 1:
                TABLE_AS[seg_x_y[i_tmp]] = seg_x_y[i_tmp + 1]
                i_tmp += 2
        elif 'AS' == state_x and 'JOIN' == state_y:
            TABLE += seg_x_y
            TABLE_AS[STACK_ALL[i_state_x - 1]] = STACK_ALL[i_state_x + 1]
        elif 'FROM' == state_x and 'AS' == state_y:
            TABLE += seg_x_y
        elif 'AS' == state_x and 'WHERE' == state_y:
            TABLE += seg_x_y
            TABLE_AS[STACK_ALL[i_state_x - 1]] = STACK_ALL[i_state_x + 1]
        elif 'JOIN' == state_x and 'AS' == state_y:
            TABLE += seg_x_y
        elif 'AS' == state_x and 'ON' == state_y:
            TABLE += seg_x_y
            TABLE_AS[STACK_ALL[i_state_x - 1]] = STACK_ALL[i_state_x + 1]
        elif 'JOIN' == state_x and 'ON' == state_y:
            TABLE += seg_x_y
            i_tmp = 0
            while i_tmp < len(seg_x_y) - 1:
                TABLE_AS[seg_x_y[i_tmp]] = seg_x_y[i_tmp + 1]
                i_tmp += 2

    # 1删除select字段表名
    COLUMN_PURE = []
    for i in range(len(COLUMN)):
        # 如果是表达式 @id_card_num := temp.mbr_id_card_num AS id_card_num，它不属于具体表，但是能够命中 find('.')
        is_point_sep_table_column = 1
        for v_op in OPERATOR_MIDDLE:
            if v_op in COLUMN[i]:
                is_point_sep_table_column -= 1
        if is_point_sep_table_column > 0:
            pos = COLUMN[i].find('.')
            COLUMN_PURE.append(COLUMN[i][pos + 1:])
            if COLUMN[i] in COLUMN_AS.keys():
                COLUMN_AS[COLUMN[i][pos + 1:]] = COLUMN_AS.pop(COLUMN[i])
        else:
            COLUMN_PURE.append(COLUMN[i])
    # 2删除表名中字段别名
    TABLE_PURE = []
    for tmp_table in TABLE:
        find_cnt = 0
        for k, v in TABLE_AS.items():
            if v == tmp_table:
                find_cnt += 1
        if find_cnt == 0:
            if tmp_table not in TABLE_PURE:
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
            if len(TABLE_PURE) == 1:
                MAP_COLUMN_TABLE[v] = TABLE_PURE[0]
            else:
                MAP_COLUMN_TABLE[v] = node_name

    return COLUMN_AS, MAP_COLUMN_TABLE, TABLE_PURE


@func_name()
def flatten_column_meta(meta_id, meta_map):
    column_str = ''
    if meta_id not in meta_map:
        return column_str
    else:
        for v in meta_map[meta_id]:
            if '#meta' not in v:
                column_str += v + ' '
            else:
                column_str += flatten_column_meta(v, meta_map)
        return column_str


@func_name()
def select_sql_to_conf(title, sql, map_colmn_column, map_column_table, table_pure,
                       map_table_table, node_commnets):
    add_node = dict()
    node_dict = dict()

    node_dict['ts'] = {
        'name': '开始',
        'desc': '''开始''',
        'color': 'red'
    }
    add_node['dag'] = '''ts >> 1 '''
    if not title:
        add_node['title'] = 'sql_%s' % datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    else:
        add_node['title'] = title
    node_dict[1] = {
        'name': 'SQL',
        'desc': sql,
        'color': 'lightblue'
    }

    node_id = 2
    map_table_nodes_id = {}
    map_column_nodes_id = {}
    dag_str = ''
    COLUMN_PURE = []
    for col1, col2 in map_colmn_column.items():
        if col1 not in COLUMN_PURE:
            COLUMN_PURE.append(col1)

    for k, v in map_column_table.items():
        if k not in COLUMN_PURE:
            COLUMN_PURE.append(k)

    map_table_nodes_id['SQL'] = 1
    for table in table_pure:
        desc = '表'
        if table in node_commnets:
            desc = '表 | ' + ' '.join(node_commnets[table])
        node_dict[node_id] = {'name': table, 'desc': desc, 'color': 'blue'}
        map_table_nodes_id[table] = node_id
        if table not in map_table_table.keys():
            dag_str += '\n' + '1 >> %d' % node_id
        node_id += 1
    for col in map_column_table.keys():
        desc = '字段'
        if col in node_commnets:
            desc = '字段 | ' + flatten_column_meta(col, merge_meta_map)
        # print(flatten_column_meta(col, merge_meta_map))
        node_dict[node_id] = {'name': col, 'desc': desc, 'color': 'orange'}
        map_column_nodes_id[col] = node_id
        node_id += 1
    # for col in node_commnets.keys():
    #     if col not in COLUMN_PURE:
    #         desc = '字段(子) | ' + ' '.join(node_commnets[col])
    #         node_dict[node_id] = {'name': col, 'desc': desc, 'color': 'lightgreen'}
    #         map_column_nodes_id[col] = node_id
    #         node_id += 1

    COLUMN_AS_VALUE = []
    for v in COLUMN_PURE:
        for k1, v1 in map_colmn_column.items():
            if k1 == v:
                desc = '字段别名'
                if v1 not in COLUMN_AS_VALUE:
                    node_dict[node_id] = {'name': v1, 'desc': desc, 'color': 'pink'}
                    map_column_nodes_id[v1] = node_id
                    node_id += 1
                COLUMN_AS_VALUE.append(v1)

    for k, v in map_colmn_column.items():
        if k in map_column_nodes_id and v in map_column_nodes_id:
            dag_str += '\n' + '%d >> %d' % (map_column_nodes_id[k], map_column_nodes_id[v])
    for k, v in map_column_table.items():
        if k in map_column_nodes_id and v in map_table_nodes_id:
            dag_str += '\n' + '%d >> %d' % (map_table_nodes_id[v], map_column_nodes_id[k])
    for k, v in map_table_table.items():
        if k in map_table_nodes_id and v in map_table_nodes_id:
            dag_str += '\n' + '%d >> %d' % (map_table_nodes_id[v], map_table_nodes_id[k])
    # 字段备注

    add_node['dag'] += dag_str
    add_node['nodes'] = node_dict

    with open('./dag_conf.py', 'r+') as fp:
        reads = fp.read().strip().strip(']') + '\n %s' % add_node + ',]\n'
        fp.seek(0)
        fp.truncate()
        fp.write(reads)


@func_name()
def reset_dag_conf():
    with open('./dag_conf.py', 'r+') as fp:
        reads = ''.join(fp.readlines()[:8]) + ']\n'
        fp.seek(0)
        fp.truncate()
        fp.write(reads)


def recursion_gen(node_name, meta_sql_words, meta_map):
    global MAP_TABLE_TABLE, COLUMN_AS, MAP_COLUMN_TABLE, TABLE_PURE, COLUMN_PURE, merge_meta_map

    merge_sql_words, merge_meta_map = merge_meta_words_to_operator(meta_sql_words, meta_map)
    TMP_COLUMN_AS, TMP_MAP_COLUMN_TABLE, TMP_TABLE_PURE = parse_meta_select_v2(node_name, merge_sql_words)

    # 层级关系
    for v1 in TMP_TABLE_PURE:
        if v1 not in MAP_TABLE_TABLE.keys():
            MAP_TABLE_TABLE[v1] = node_name

    # 改名，字段重名 or 表重名
    TMP_COLUMN_AS_copy = {}
    TMP_TABLE_PURE_copy = []
    TMP_COLUMN_PURE = []
    for v in TMP_TABLE_PURE:
        TMP_TABLE_PURE_copy.append(v)
    for k_tmp, v_tmp in TMP_COLUMN_AS.items():
        TMP_COLUMN_AS_copy[k_tmp] = v_tmp

    # 获取当前无重复字段
    for k, v in TMP_COLUMN_AS.items():
        if k not in TMP_COLUMN_PURE:
            TMP_COLUMN_PURE.append(k)
        if v not in TMP_COLUMN_PURE:
            TMP_COLUMN_PURE.append(v)
    for k in TMP_MAP_COLUMN_TABLE.keys():
        if k not in TMP_COLUMN_PURE:
            TMP_COLUMN_PURE.append(k)

    # 拉平所有数据----------------------------------------
    # 重命名+更新
    # ----------------------------------------------表重名
    for v1 in TMP_TABLE_PURE:
        if v1 not in TABLE_PURE:
            TABLE_PURE.append(v1)
    for k, v in TMP_COLUMN_AS.items():
        COLUMN_AS[k] = v
    # ----------------------------------------------字段重名
    for v1 in TMP_COLUMN_PURE:
        if v1 not in COLUMN_PURE:
            COLUMN_PURE.append(v1)
            if v1 in TMP_MAP_COLUMN_TABLE.keys() and v1 not in MAP_COLUMN_TABLE.keys():
                MAP_COLUMN_TABLE[v1] = TMP_MAP_COLUMN_TABLE[v1]
        elif v1 in TMP_MAP_COLUMN_TABLE.keys() and v1 in MAP_COLUMN_TABLE.keys():
            MAP_COLUMN_TABLE.pop(v1)
            MAP_COLUMN_TABLE[v1] = TMP_MAP_COLUMN_TABLE[v1]
    # 如果是字段别名，也要剔除上层字段中重名的字段表映射关系
    for v1 in TMP_COLUMN_AS.values():
        if v1 in MAP_COLUMN_TABLE.keys():
            MAP_COLUMN_TABLE.pop(v1)
    #   递归子查询的表+字段+关系
    for v in TMP_TABLE_PURE:
        if '#meta' in v:
            recursion_gen(v, merge_meta_map[v], merge_meta_map)


@func_name()
def auto_gen():
    global MAP_TABLE_TABLE, COLUMN_AS, MAP_COLUMN_TABLE, TABLE_PURE, merge_meta_map
    reload(sqls_to_do)
    reset_dag_conf()
    for dict_one in sqls_to_do.str_sql:
        global rename_col_cnt, MAP_TABLE_TABLE, COLUMN_AS, COLUMN_PURE, MAP_COLUMN_TABLE, TABLE_PURE, merge_meta_map, meta_sql_words, meta_map
        # 清空
        rename_col_cnt = 0
        MAP_TABLE_TABLE = {}
        COLUMN_AS = {}
        COLUMN_PURE = []
        MAP_COLUMN_TABLE = {}
        TABLE_PURE = []
        merge_meta_map = {}
        meta_sql_words = []
        meta_map = {}
        # 计算
        sql_to_meta_words(dict_one['SQL'])
        # 递归计算
        recursion_gen('SQL', meta_sql_words, meta_map)
        # 把全局关系映射到dag_conf.py
        select_sql_to_conf(dict_one['TITLE'], dict_one['SQL'].replace('"', ''), COLUMN_AS, MAP_COLUMN_TABLE, TABLE_PURE,
                           MAP_TABLE_TABLE,
                           merge_meta_map)


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
