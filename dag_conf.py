# !/usr/bin/env python
# -*- coding: utf-8 -*-
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

NODES = [
    # ods_wait_trigger图
    {
        'title': 'ods_wait_trigger 03:00开始执行',
        'nodes': {
            '开始': {'desc': '开始', 'color': 'red'},
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
            '结束': {'desc': '结束', 'color': 'lightblue'},
        },
        'dag': '''
                开始>>[wait_until_success_ods_woda,wait_until_success_ods_jff,wait_until_success_ods_zxx] 
                [wait_until_success_ods_jff,wait_until_success_ods_zxx]>>zxx2_etl_service
                wait_until_success_ods_jff>>[jff_etl_service,zhouxin_jff1_sp_credict]
                zhouxin_jff1_sp_credict >> 结束
                [zxx2_etl_service,jff_etl_service]>>zhouxin_etl_service_cb_operation_data
                [wait_until_success_ods_woda,zhouxin_etl_service_cb_operation_data]>>interface_v2_daily_job_reguser_channel_operation >> 结束
                wait_until_success_ods_woda>>zhouxin_woda1_user_exam_result_server_all_total>>zhouxin_woda2_cheat_detect>>zhouxin_woda3_recommend >> 结束
                wait_until_success_ods_zxx>>zxx_etl_service>>user_credict_month_day_work>>zxx2_credict >> 结束
                wait_until_success_ods_woda>>dim_broker_cliff >> 结束
                
                [wait_until_success_ods_woda,wait_until_success_ods_jff,wait_until_success_ods_zxx,zxx_etl_service]>>zhouxin_name_list_analysis >> 结束
                
                [wait_until_success_ods_jff,wait_until_success_ods_zxx,zxx2_etl_service,jff_etl_service]>>zhouxin_performance_data >> 结束
                [wait_until_success_ods_jff,wait_until_success_ods_zxx,zxx2_etl_service,jff_etl_service]>>zhouxin_operator_data >> 结束
                
                wait_until_success_ods_woda>>load_dim >> wd_broker >> wd_user >> user_broker_relation >> wd_recruit_demand >> wd_preorder >> wd_interview >> wd_hire >> wd_invite >> wd_subsidy >> wd_interaction >> wd_labor_order  >> wd_ent_interview_cnt >> 结束
                
                [wait_until_success_ods_woda,wait_until_success_ods_zxx]>> financial >> 结束'''
    },
    {
        'title': 'ods_wait_trigger 03:00开始执行',
        'nodes': {
            '开始': {'desc': '开始', 'color': 'red'},
            '结束': {'desc': '结束', 'color': 'lightblue'},
        },
        'dag': '''开始 >> 结束'''
    }
]
