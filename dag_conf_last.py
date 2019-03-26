# !/usr/bin/env python
# -*- coding: utf-8 -*-
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

NODES = [
    # ods_wait_trigger图
    {
        'title': 'ods_wait_trigger',
        'nodes': {
            '开始': {'desc': '开始', 'color': 'red'},
            '结束': {'desc': '结束', 'color': 'lightblue'},
            '概述': {
                'desc': '''airflow中的定时任务(03:00)，用于等待触发。ods报表等离线增量任务，统一放在ods_wait_tigger中配置'''
            },
            'wait_until_success_ods_woda': {
                'desc': '/home/datas/.virtualenvs/env27/bin/python /home/datas/ods_controller/ods_controller_woda/task5_wait_until_success.py',
                'color': 'green'},
            'wait_until_success_ods_jff': {
                'desc': '/home/datas/.virtualenvs/env27/bin/python /home/datas/ods_controller/ods_controller_jff/task5_wait_until_success.py',
                'color': 'green'},
            'wait_until_success_ods_zxx': {
                'desc': '/home/datas/.virtualenvs/env27/bin/python /home/datas/ods_controller/ods_controller_zxx/task5_wait_until_success.py',
                'color': 'green'},
            'interface_v2_daily_job_reguser_channel_operation': {
                'desc': 'source /home/datas/interface_v2/cmd_day',
                'color': 'orange'
            },
            'zxx2_etl_service': {
                'desc': '/usr/local/data-integration/kitchen.sh -file=/datas/kettle/jobs/dw-etl/zxx2_etl_service/zxx2_etl_service.kjb',
                'color': 'orange'
            },
            'jff_etl_service': {
                'desc': '/usr/local/data-integration/kitchen.sh -file=/datas/kettle/jobs/dw-etl/jff_etl_service/jff_etl_service.kjb',
                'color': 'orange'
            },
            'zhouxin_etl_service_cb_operation_data': {
                'desc': 'ssh 192.168.10.29 < /datas/airflow/dags/zhouxin_jff_etl_service_cb_operation_data.cmd ',
                'color': 'orange'
            },
            'zhouxin_woda1_user_exam_result_server_all_total': {
                'desc': 'ssh 192.168.10.29 < /datas/airflow/dags/zhouxin_woda1_user_exam_result_server_all_total.cmd ',
                'color': 'orange'
            },
            'zhouxin_woda2_cheat_detect': {
                'desc': 'ssh 192.168.10.29 < /datas/airflow/dags/zhouxin_woda2_cheat_detect.cmd ',
                'color': 'orange'
            },
            'zhouxin_woda3_recommend': {
                'desc': 'ssh 192.168.10.29 < /datas/airflow/dags/zhouxin_woda3_recommend.cmd ',
                'color': 'orange'
            },
            'zhouxin_jff1_sp_credict': {
                'desc': 'ssh 192.168.10.29 < /datas/airflow/dags/zhouxin_jff1_sp_credict.cmd ',
                'color': 'orange'
            },
            'zxx_etl_service': {
                'desc': '/usr/local/data-integration/kitchen.sh -file=/datas/kettle/jobs/dw-etl/zxx_etl_service/zxx_etl_service.kjb',
                'color': 'orange'
            },
            'zxx2_credict': {
                'desc': '/usr/local/data-integration/kitchen.sh -file=/datas/kettle/jobs/dw-etl/zxx_credict/zxx2_credict.kjb',
                'color': 'orange'
            },
            'user_credict_month_day_work': {
                'desc': '/bin/bash /datas/airflow/dags/user_credict_month_day_work.sh ',
                'color': 'orange'
            },
            # dim_wd
            'load_dim': {
                'desc': '/usr/local/data-integration/kitchen.sh -file=/datas/kettle/jobs/dw-etl/load_dim/load_dim_job.kjb',
                'color': 'orange'
            },
            'wd_broker': {
                'desc': '/usr/local/data-integration/pan.sh -file=/datas/kettle/jobs/dw-etl/wd_broker/wd_broker.ktr',
                'color': 'orange'
            },
            'wd_user': {
                'desc': '/usr/local/data-integration/kitchen.sh -file=/datas/kettle/jobs/dw-etl/wd_user/wd_user_job.kjb',
                'color': 'orange'
            },
            'user_broker_relation': {
                'desc': '/usr/local/data-integration/kitchen.sh -file=/datas/kettle/jobs/dw-etl/user_broker_relation/stg_user_broker_relation.kjb',
                'color': 'orange'
            },
            'wd_preorder': {
                'desc': '/usr/local/data-integration/kitchen.sh -file=/datas/kettle/jobs/dw-etl/wd_preorder/wd_preorder.kjb',
                'color': 'orange'
            },
            'wd_interview': {
                'desc': '/usr/local/data-integration/kitchen.sh -file=/datas/kettle/jobs/dw-etl/wd_interview/wd_interview.kjb',
                'color': 'orange'
            },
            'wd_hire': {
                'desc': '/usr/local/data-integration/kitchen.sh -file=/datas/kettle/jobs/dw-etl/wd_hire/wd_hire.kjb',
                'color': 'orange'
            },
            'wd_invite': {
                'desc': '/usr/local/data-integration/kitchen.sh -file=/datas/kettle/jobs/dw-etl/wd_invite/wd_invite.kjb',
                'color': 'orange'
            },
            'wd_subsidy': {
                'desc': '/usr/local/data-integration/kitchen.sh -file=/datas/kettle/jobs/dw-etl/wd_subsidy/wd_subsidy.kjb',
                'color': 'orange'
            },
            'wd_interaction': {
                'desc': '/usr/local/data-integration/kitchen.sh -file=/datas/kettle/jobs/dw-etl/wd_interaction/wd_interaction.kjb',
                'color': 'orange'
            },
            'wd_labor_order': {
                'desc': '/usr/local/data-integration/kitchen.sh -file=/datas/kettle/jobs/dw-etl/wd_labor_order/wd_labor_order.kjb',
                'color': 'orange'
            },
            'wd_recruit_demand': {
                'desc': '/usr/local/data-integration/kitchen.sh -file=/datas/kettle/jobs/dw-etl/wd_recruit_demand/wd_recruit_demand.kjb',
                'color': 'orange'
            },
            'wd_ent_interview_cnt': {
                'desc': '/usr/local/data-integration/kitchen.sh -file=/datas/kettle/jobs/dw-etl/wd_ent_interview_cnt/wd_ent_interview_cnt.kjb',
                'color': 'orange'
            },
            'dim_broker_cliff': {
                'desc': '/usr/local/data-integration/kitchen.sh -file=/datas/kettle/jobs/dw-etl/dim_broker/broker/broker.kjb',
                'color': 'orange'
            },
            'zhouxin_name_list_analysis': {
                'desc': 'ssh 192.168.10.29 < /datas/airflow/dags/zhouxin_name_list_analysis.cmd ',
                'color': 'orange'
            },
            'zhouxin_performance_data': {
                'desc': 'ssh 192.168.10.29 < /datas/airflow/dags/zhouxin_performance_data.cmd ',
                'color': 'orange'
            },
            'zhouxin_operator_data': {
                'desc': 'ssh 192.168.10.29 < /datas/airflow/dags/zhouxin_operator_data.cmd ',
                'color': 'orange'
            },
            'financial': {
                'desc': 'source /home/datas/financial/cmd_day',
                'color': 'orange'
            },

        },
        'dag': '''
                开始 >> 概述 >> [wait_until_success_ods_woda,wait_until_success_ods_jff,wait_until_success_ods_zxx] 
                [wait_until_success_ods_jff,wait_until_success_ods_zxx] >> zxx2_etl_service
                wait_until_success_ods_jff >> [jff_etl_service,zhouxin_jff1_sp_credict]
                zhouxin_jff1_sp_credict >> 结束
                [zxx2_etl_service,jff_etl_service] >> zhouxin_etl_service_cb_operation_data
                [wait_until_success_ods_woda,zhouxin_etl_service_cb_operation_data] >> interface_v2_daily_job_reguser_channel_operation >> 结束
                wait_until_success_ods_woda >> zhouxin_woda1_user_exam_result_server_all_total >> zhouxin_woda2_cheat_detect >> zhouxin_woda3_recommend >> 结束
                wait_until_success_ods_zxx >> zxx_etl_service >> user_credict_month_day_work >> zxx2_credict >> 结束
                wait_until_success_ods_woda >> dim_broker_cliff >> 结束
                
                [wait_until_success_ods_woda,wait_until_success_ods_jff,wait_until_success_ods_zxx,zxx_etl_service] >> zhouxin_name_list_analysis >> 结束
                
                [wait_until_success_ods_jff,wait_until_success_ods_zxx,zxx2_etl_service,jff_etl_service] >> zhouxin_performance_data >> 结束
                [wait_until_success_ods_jff,wait_until_success_ods_zxx,zxx2_etl_service,jff_etl_service] >> zhouxin_operator_data >> 结束
                
                wait_until_success_ods_woda >> load_dim >> wd_broker >> wd_user >> user_broker_relation >> wd_recruit_demand >> wd_preorder >> wd_interview >> wd_hire >> wd_invite >> wd_subsidy >> wd_interaction >> wd_labor_order  >> wd_ent_interview_cnt >> 结束
                
                [wait_until_success_ods_woda,wait_until_success_ods_zxx] >>  financial >> 结束'''
    },
    {
        'title': 'jff_online_service出错重跑方法',
        'nodes': {
            '开始': {'desc': '开始', 'color': 'red'},
            '概述': {
                'desc': '''如果数据同步出现错误：ods同步错误 or etl出错，需要修改DW/stg.cdc_time中current_sync_date 字段，具体table名称见下CDC_TABLE_NAME'''
            },
            'jff_online_service': {'desc': 'airflow的任务名', 'color': 'orange'},
            'jff_enterprise_service': {'desc': '''etl工程目录 /usr/local/data-integration/kitchen.sh -file=/datas/kettle/jobs/dw-etl/
                                               jff_enterprise_service/jff_enterprise_service.kjb''',
                                       'color': 'orange'},
            'direct_store_name_list_sum': {
                'desc': '''CDC_TABLE_NAME : direct_store_name_list_sum''',
                'color': 'orange'},
            'market_prc_max': {'desc': '''CDC_TABLE_NAME : market_prc_max''',
                               'color': 'orange'},
            'area_prc_max': {'desc': '''CDC_TABLE_NAME : area_prc_max''',
                             'color': 'orange'},
            'ent_name_list_sum': {'desc': '''CDC_TABLE_NAME : ent_name_list_sum''',
                                  'color': 'orange'},
            'ent_name_list_trend': {'desc': '''CDC_TABLE_NAME : ent_name_list_trend''',
                                    'color': 'orange'},
            'ent_name_list_unique_sum': {
                'desc': '''CDC_TABLE_NAME : ent_name_list_unique_sum''', 'color': 'orange'},
            'ent_name_list_unique_trend': {
                'desc': '''CDC_TABLE_NAME : ent_name_list_unique_trend''', 'color': 'orange'},
            'zhouxinxin_retation': {'desc': '''CDC_TABLE_NAME : zhouxinxin_retation''',
                                    'color': 'orange'},
            'stg_zhouxinxin_name_list': {
                'desc': '''CDC_TABLE_NAME : stg_zhouxinxin_name_list''', 'color': 'orange'},
            '结束': {'desc': '结束', 'color': 'lightblue'},
        },
        'dag': '''开始 >> 概述 >> jff_online_service >> jff_enterprise_service
        jff_enterprise_service >> [direct_store_name_list_sum,market_prc_max,area_prc_max,ent_name_list_sum,ent_name_list_trend,ent_name_list_unique_sum,ent_name_list_unique_trend,zhouxinxin_retation,stg_zhouxinxin_name_list] >>  结束
        '''
    },
    {
        'title': 'zhouxinxin_name_list 跨库管理名单状态',
        'nodes': {
            '开始': {'desc': '开始', 'color': 'red'},
            '结束': {'desc': '结束', 'color': 'lightblue'},
            '概述': {
                'desc': '''Git目录： 1）http://git.woda.ink/dw/dw-etl/ jff_enterprise_service/zhouxinxin_retention
                                    2）http://git.woda.ink/dw/dw-etl/ jff_enterprise_service/zhouxinxin_name_list_syn
                        airflow定时任务:jff_online_service/zhouxinxin_retention
                        pg同步任务：jff_online_service/zhouxinxin_name_list_syn
                        '''
            },
            'PG数据库': {
                'desc': 'DW/stg.stg_zhouxinxin_name_list',
                'color': 'green'
            },
            'MySQL数据库': {
                'desc': 'transit_jff.zhouxinxin_name_list',
                'color': 'green'
            },
            'cdc_time': {
                'desc': '''PG的dw.cdc_time用与增量同步对应的table_name : 1.zhouxinxin_retention 
                        2.stg_zhouxinxin_name_list ''',
                'color': 'orange'
            },
        },
        'dag': '''
        开始 >> 概述 >> [PG数据库,MySQL数据库] >> cdc_time >> 结束
        
        '''
    }
]
