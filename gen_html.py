# !/usr/bin/env python
# -*- coding: utf-8 -*-
'''
1.解析sql，得到SELECT语句的表+字段+别名+依赖关系
2.通过1，生成dag_conf.py里的配置文件
3.通过gen_html.py检测sqls_to_do.py + dag_conf.py是否变动，从而重新生成./dist/demo/血缘分析DAG.html
'''
import traceback
import dag_conf
import dag_template
from pyutils.misc import func_name
import sys
import time
import filecmp
import shutil
import gen_dag_from_sql

reload(sys)
sys.setdefaultencoding('utf-8')


@func_name('解析dag流')
def parse_dag(dag, nodes):
    cmds = dag.split('\n')
    edge_strs = []
    for cmd in cmds:
        dags = cmd.split('>>')
        edge_src_dst = ' "id_%s", "id_%s" '
        dags_new = []
        for v in dags:
            dags_new.append(v.strip())
        j = len(dags_new)
        if j <= 1:
            continue
        for i in range(j - 1):
            start_one = dags_new[i]
            end_one = dags_new[i + 1]

            start_ones = start_one.strip().strip('[').strip(']').split(',')
            end_ones = end_one.strip().strip('[').strip(']').split(',')

            for vs in start_ones:
                for ve in end_ones:
                    edge_strs.append(edge_src_dst % (vs, ve))
    return edge_strs


@func_name('生成edge模板')
def gen_edge_template(graph_id, edge_strs):
    edge_str = ''
    for v in edge_strs:
        # edge_one = '''%s.setEdge(%s, { label: "", curve: d3.curveBasis });''' % (graph_id, v)
        edge_one = '''%s.setEdge(%s, { label: "" });''' % (graph_id, v)
        edge_str += edge_one
        edge_str += '\n'
    return edge_str.strip()


@func_name('生成节点模板')
def gen_nodes_template(nodes):
    node_one_color_template = '''"id_%s": {
    id: "id_%s",
    label: "%s",
    description: "%s",
    style: "fill: %s"
  },
  '''
    node_one_template = '''"id_%s": {
    id: "id_%s",
    label: "%s",
    description: "%s"
  },
  '''
    node_ones = ''
    for k, node_one in nodes.iteritems():
        if 'color' in node_one:
            node_one_str = node_one_color_template % (
                k, k, node_one['name'].replace('\n', ' | '), node_one['desc'].replace('\n', '<br>'),
                node_one['color'])
        else:
            node_one_str = node_one_template % (
                k, k, node_one['name'].replace('\n', ' | '), node_one['desc'].replace('\n', '<br>'))
        node_ones += node_one_str
    return node_ones.strip().strip(',')


@func_name('生成xxx.html')
def auto_gen(file_name):
    fp = open('./dist/demo/%s.html' % file_name, 'w')
    svg_all = ''
    id = 1

    for node in dag_conf.NODES:
        print node['title']
        str_svg_id = 'svg%d' % id
        str_graph_id = 'g%d' % id
        str_inner_id = 'inner%d' % id
        id += 1
        # 生成节点信息
        nodes_template_str = gen_nodes_template(node['nodes'])
        # 生成标题+连接
        edge_strs = parse_dag(node['dag'], node['nodes'])
        edge_template_str = gen_edge_template(str_graph_id, edge_strs)
        # 合并
        svg_all += dag_template.str_header % {'HEADER': node['title']} + '\n'
        svg_all += dag_template.str_svg % {'SVG_ID': str_svg_id} + '\n'
        svg_all += dag_template.str_script % {
            'NODES_TEMPLATE': nodes_template_str, 'EDGE_TEMPLATE': edge_template_str,
            'SVG_ID': str_svg_id, 'INNER_ID': str_inner_id, 'GRAPH': str_graph_id} + '\n'

    fp.write(dag_template.str_template % (file_name, svg_all))
    fp.flush()
    pass


def main():
    while True:
        if not filecmp.cmp('sqls_to_do.py', 'sqls_to_do_last.py'):
            gen_dag_from_sql.auto_gen()
            shutil.copyfile('sqls_to_do.py', 'sqls_to_do_last.py')

        if not filecmp.cmp('dag_conf.py', 'dag_conf_last.py'):
            reload(dag_conf)
            reload(dag_template)
            auto_gen('血缘分析DAG')
            shutil.copyfile('dag_conf.py', 'dag_conf_last.py')
        time.sleep(5)


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
