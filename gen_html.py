# !/usr/bin/env python
# -*- coding: utf-8 -*-
import traceback
import dag_conf
import dag_template
from pyutils.misc import func_name
import sys
import time
import filecmp
import shutil

reload(sys)
sys.setdefaultencoding('utf-8')


@func_name(u'解析dag流')
def parse_dag(dag):
    cmds = dag.split('\n')
    edge_strs = []
    for cmd in cmds:
        dags = cmd.split('>>')
        edge_src_dst = ' "%s", "%s" '
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
    print edge_strs
    return edge_strs


@func_name()
def gen_edge_template(graph_id, edge_strs):
    edge_str = ''
    for v in edge_strs:
        edge_one = '''%s.setEdge(%s, { label: "", curve: d3.curveBasis });''' % (graph_id, v)
        edge_str += edge_one
        edge_str += '\n'
    return edge_str.strip()


@func_name()
def gen_nodes_template(nodes):
    node_one_color_template = '''"%s": {
    description: "%s",
    style: "fill: %s"
  },
  '''
    node_one_template = '''"%s": {
    description: "%s"
  },
  '''
    node_ones = ''
    for k, v in nodes.iteritems():
        node = k
        desc = ''
        color = ''
        if 'desc' in v:
            desc = v['desc']
            desc = desc.replace('\n', '<br>')
        if 'color' in v:
            color = v['color']
        if color != '':
            node_one = node_one_color_template % (node, desc, color)
        else:
            node_one = node_one_template % (node, desc)
        node_ones += node_one
    return node_ones.strip().strip(',')


@func_name()
def gen_html(title='', node='', edges=''):
    return dag_template.str_template % (title, title, node, edges)


# 生成title.html
def auto_gen(file_name):
    fp = open('./dist/demo/%s.html' % file_name, 'w')
    svg_all = ''
    id = 1

    for node in dag_conf.NODES:
        str_svg_id = 'svg%d' % id
        str_graph_id = 'g%d' % id
        str_inner_id = 'inner%d' % id
        id += 1
        # 生成节点信息
        nodes_template_str = gen_nodes_template(node['nodes'])
        # 生成标题+连接
        edge_strs = parse_dag(node['dag'])
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
