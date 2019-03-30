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
            1: {
                'name': '开始',
                'desc': '开始',
                'color': 'red'
            },
            2: {
                'name': '结束',
                'desc': '结束',
                'color': 'lightblue'
            },
            3: {
                'name': '概述',
                'desc': '''airflow中的定时任务(03:00)，用于等待触发。ods报表等离线增量任务，统一放在ods_wait_tigger中配置''',
                'color': 'lightblue'
            },
            4: {
                'name': 'wait_until_success_ods_woda',
                'desc': '''/home/datas/.virtualenvs/env27/bin/python /home/datas/ods_controller/ods_controller_woda/task5_wait_until_success.py''',
                'color': 'green'
            },
            5: {
                'name': 'wait_until_success_ods_jff',
                'desc': '''/home/datas/.virtualenvs/env27/bin/python /home/datas/ods_controller/ods_controller_jff/task5_wait_until_success.py''',
                'color': 'green'
            },
            6: {
                'name': 'wait_until_success_ods_zxx',
                'desc': '''/home/datas/.virtualenvs/env27/bin/python /home/datas/ods_controller/ods_controller_zxx/task5_wait_until_success.py''',
                'color': 'green'
            },
            7: {
                'name': 'interface_v2_daily_job_reguser_channel_operation',
                'desc': '''source /home/datas/interface_v2/cmd_day''',
                'color': 'orange'
            },
            8: {
                'name': 'zxx2_etl_service',
                'desc': '''/usr/local/data-integration/kitchen.sh -file=/datas/kettle/jobs/dw-etl/zxx2_etl_service/zxx2_etl_service.kjb''',
                'color': 'orange'
            },
            9: {
                'name': 'jff_etl_service',
                'desc': '''/usr/local/data-integration/kitchen.sh -file=/datas/kettle/jobs/dw-etl/jff_etl_service/jff_etl_service.kjb''',
                'color': 'orange'
            },
            10: {
                'name': 'zhouxin_etl_service_cb_operation_data',
                'desc': '''ssh 192.168.10.29 < /datas/airflow/dags/zhouxin_jff_etl_service_cb_operation_data.cmd ''',
                'color': 'orange'
            },
            11: {
                'name': 'zhouxin_woda1_user_exam_result_server_all_total',
                'desc': ''''ssh 192.168.10.29 < /datas/airflow/dags/zhouxin_woda1_user_exam_result_server_all_total.cmd''',
                'color': 'orange'
            },
            12: {
                'name': 'zhouxin_woda2_cheat_detect',
                'desc': ''''ssh 192.168.10.29 < /datas/airflow/dags/zhouxin_woda2_cheat_detect.cmd''',
                'color': 'orange'
            },
            13: {
                'name': 'zhouxin_woda3_recommend',
                'desc': ''''ssh 192.168.10.29 < /datas/airflow/dags/zhouxin_woda3_recommend.cmd''',
                'color': 'orange'
            },
            14: {
                'name': 'zhouxin_jff1_sp_credict',
                'desc': ''''ssh 192.168.10.29 < /datas/airflow/dags/zhouxin_jff1_sp_credict.cmd''',
                'color': 'orange'
            },
            15: {
                'name': 'zxx_etl_service',
                'desc': '''/usr/local/data-integration/kitchen.sh -file=/datas/kettle/jobs/dw-etl/zxx_etl_service/zxx_etl_service.kjb''',
                'color': 'orange'
            },
            16: {
                'name': 'zxx2_credict',
                'desc': '''/usr/local/data-integration/kitchen.sh -file=/datas/kettle/jobs/dw-etl/zxx_credict/zxx2_credict.kjb''',
                'color': 'orange'
            },
            17: {
                'name': 'user_credict_month_day_work',
                'desc': '''/bin/bash /datas/airflow/dags/user_credict_month_day_work.sh ''',
                'color': 'orange'
            },
            18: {
                'name': 'dim_wd.load_dim',
                'desc': '''/usr/local/data-integration/kitchen.sh -file=/datas/kettle/jobs/dw-etl/load_dim/load_dim_job.kjb''',
                'color': 'orange'
            },
            19: {
                'name': 'wd_broker',
                'desc': '''/usr/local/data-integration/pan.sh -file=/datas/kettle/jobs/dw-etl/wd_broker/wd_broker.ktr''',
                'color': 'orange'
            },
            20: {
                'name': 'wd_user',
                'desc': '''/usr/local/data-integration/kitchen.sh -file=/datas/kettle/jobs/dw-etl/wd_user/wd_user_job.kjb''',
                'color': 'orange'
            },
            21: {
                'name': 'user_broker_relation',
                'desc': '''/usr/local/data-integration/kitchen.sh -file=/datas/kettle/jobs/dw-etl/user_broker_relation/stg_user_broker_relation.kjb''',
                'color': 'orange'
            },
            22: {
                'name': 'wd_preorder',
                'desc': '''/usr/local/data-integration/kitchen.sh -file=/datas/kettle/jobs/dw-etl/wd_preorder/wd_preorder.kjb''',
                'color': 'orange'
            },
            23: {
                'name': 'wd_interview',
                'desc': '''/usr/local/data-integration/kitchen.sh -file=/datas/kettle/jobs/dw-etl/wd_interview/wd_interview.kjb''',
                'color': 'orange'
            },
            24: {
                'name': 'wd_hire',
                'desc': '''/usr/local/data-integration/kitchen.sh -file=/datas/kettle/jobs/dw-etl/wd_hire/wd_hire.kjb''',
                'color': 'orange'
            },
            25: {
                'name': 'wd_invite',
                'desc': '''/usr/local/data-integration/kitchen.sh -file=/datas/kettle/jobs/dw-etl/wd_invite/wd_invite.kjb''',
                'color': 'orange'
            },
            26: {
                'name': 'wd_subsidy',
                'desc': '''/usr/local/data-integration/kitchen.sh -file=/datas/kettle/jobs/dw-etl/wd_subsidy/wd_subsidy.kjb''',
                'color': 'orange'
            },
            27: {
                'name': 'wd_interaction',
                'desc': '''/usr/local/data-integration/kitchen.sh -file=/datas/kettle/jobs/dw-etl/wd_interaction/wd_interaction.kjb''',
                'color': 'orange'
            },
            28: {
                'name': 'wd_labor_order',
                'desc': '''/usr/local/data-integration/kitchen.sh -file=/datas/kettle/jobs/dw-etl/wd_labor_order/wd_labor_order.kjb''',
                'color': 'orange'
            },
            29: {
                'name': 'wd_recruit_demand',
                'desc': '''/usr/local/data-integration/kitchen.sh -file=/datas/kettle/jobs/dw-etl/wd_recruit_demand/wd_recruit_demand.kjb''',
                'color': 'orange'
            },
            30: {
                'name': 'wd_ent_interview_cnt',
                'desc': '''/usr/local/data-integration/kitchen.sh -file=/datas/kettle/jobs/dw-etl/wd_ent_interview_cnt/wd_ent_interview_cnt.kjb''',
                'color': 'orange'
            },
            31: {
                'name': 'dim_broker_cliff',
                'desc': '''/usr/local/data-integration/kitchen.sh -file=/datas/kettle/jobs/dw-etl/dim_broker/broker/broker.kjb''',
                'color': 'orange'
            },
            32: {
                'name': 'zhouxin_name_list_analysis',
                'desc': '''ssh 192.168.10.29 < /datas/airflow/dags/zhouxin_name_list_analysis.cmd''',
                'color': 'orange'
            },
            33: {
                'name': 'zhouxin_performance_data',
                'desc': '''ssh 192.168.10.29 < /datas/airflow/dags/zhouxin_performance_data.cmd''',
                'color': 'orange'
            },
            34: {
                'name': 'zhouxin_operator_data',
                'desc': '''ssh 192.168.10.29 < /datas/airflow/dags/zhouxin_operator_data.cmd''',
                'color': 'orange'
            },
            35: {
                'name': 'financial',
                'desc': '''source /home/datas/financial/cmd_day''',
                'color': 'orange'
            }
        },
        'dag': '''
                1 >> 3 >> [4,5,6] 
                [5,6] >> 8
                5 >> [9,14]
                14 >> 2
                [8,9] >> 10
                [4,10] >> 7 >> 2
                4 >> 11 >> 12 >> 13 >> 2
                6 >> 15 >> 17 >> 16 >> 2
                4 >> 31 >> 2
                
                [4,5,6,15] >> 32 >> 2
                
                [5,6,8,9] >> 33 >> 2
                [5,6,8,9] >> 34 >> 2
                
                4 >> 18 >> 19 >> 20 >> 21 >> 29 >> 22 >> 23 >> 24 >> 25 >> 26 >> 27 >> 28  >> 30 >> 2
                
                [4,6] >> 35 >> 2'''
    },
    {
        'title': 'jff_online_service出错重跑方法',
        'nodes': {
            'ts': {
                'name': '开始',
                'desc': '''开始''',
                'color': 'red'
            },
            'te': {
                'name': '结束',
                'desc': '''结束''',
                'color': 'lightblue'
            },
            2: {
                'name': '概述',
                'desc': '''如果数据同步出现错误：ods同步错误 or etl出错，需要修改DW/stg.cdc_time中current_sync_date 字段，具体table名称见下CDC_TABLE_NAME''',
                'color': 'lightblue'
            },
            3: {
                'name': 'jff_online_service',
                'desc': '''airflow的任务名''',
                'color': 'orange'
            },
            4: {
                'name': 'jff_enterprise_service',
                'desc': '''etl工程目录 /usr/local/data-integration/kitchen.sh -file=/datas/kettle/jobs/dw-etl/
                                               jff_enterprise_service/jff_enterprise_service.kjb''',
                'color': 'orange'
            },
            5: {
                'name': 'direct_store_name_list_sum',
                'desc': '''dw.stg.cdc_time.cdc_table_name : direct_store_name_list_sum''',
                'color': 'orange'
            },
            6: {
                'name': 'market_prc_max',
                'desc': '''dw.stg.cdc_time.cdc_table_name : market_prc_max''',
                'color': 'orange'
            },
            7: {
                'name': 'area_prc_max',
                'desc': '''dw.stg.cdc_time.cdc_table_name : area_prc_max''',
                'color': 'orange'
            },
            8: {
                'name': 'ent_name_list_sum',
                'desc': '''dw.stg.cdc_time.cdc_table_name : ent_name_list_sum''',
                'color': 'orange'
            },
            9: {
                'name': 'ent_name_list_trend',
                'desc': '''dw.stg.cdc_time.cdc_table_name : ent_name_list_trend''',
                'color': 'orange'
            },

            10: {
                'name': 'ent_name_list_unique_sum',
                'desc': '''dw.stg.cdc_time.cdc_table_name : ent_name_list_unique_sum''',
                'color': 'orange'
            },
            11: {
                'name': 'ent_name_list_unique_trend',
                'desc': '''dw.stg.cdc_time.cdc_table_name : ent_name_list_unique_trend''',
                'color': 'orange'
            },
            12: {
                'name': 'zhouxinxin_retation',
                'desc': '''dw.stg.cdc_time.cdc_table_name : zhouxinxin_retation''',
                'color': 'orange'
            },
            13: {
                'name': 'stg_zhouxinxin_name_list',
                'desc': '''dw.stg.cdc_time.cdc_table_name : stg_zhouxinxin_name_list''',
                'color': 'orange'
            }
        },
        'dag': '''ts >> 2 >> 3 >> 4 >> [5,6,7,8,9,10,11,12,13] >>  te
        '''
    },
    {
        'title': '''zhouxinxin_name_list 跨库管理名单状态''',
        'nodes': {
            'ts': {
                'name': '开始',
                'desc': '''开始''',
                'color': 'red'
            },
            'te': {
                'name': '结束',
                'desc': '''结束''',
                'color': 'lightblue'
            },
            1: {
                'name': '概述',
                'desc': '''Git目录： 1）http://git.woda.ink/dw/dw-etl/ jff_enterprise_service/zhouxinxin_retention
                                    2）http://git.woda.ink/dw/dw-etl/ jff_enterprise_service/zhouxinxin_name_list_syn
                        airflow定时任务:jff_online_service/zhouxinxin_retention
                        pg同步任务：jff_online_service/zhouxinxin_name_list_syn''',
                'color': 'lightblue'
            },
            2: {
                'name': 'DW.stg.stg_zhouxinxin_name_list',
                'desc': '''PG数据库''',
                'color': 'green'
            },
            3: {
                'name': 'transit_jff.zhouxinxin_name_list',
                'desc': '''MySQL数据库''',
                'color': 'green'
            },
            4: {
                'name': '''cdc_time''',
                'desc': '''PG的dw.cdc_time用与增量同步对应的table_name : 1.zhouxinxin_retention
                        2.stg_zhouxinxin_name_list ''',
                'color': 'orange'
            },
        },
        'dag': '''
        ts >> 1 >> [2,3] >> 4 >> te
        '''
    },


 {'nodes': {1: {'color': 'lightblue', 'name': 'SQL', 'desc': '\n    INSERT INTO transit_jff.direct_store_name_list_sum(sp_id, league_id, league_name, ent_id, ent_short_name, sp_ent_id, sum_wage_min, sum_wage_max, intv_cnt)\nSELECT a.sp_id,\n       b.league_id,\n       b.league_name,\n       d.ent_id,\n       d.ent_short_name,\n       a.sp_ent_id,\n       d.sum_wage_min,\n       d.sum_wage_max,\n       count(1) AS intv_cnt\nFROM ods_jff.name_list a\nLEFT JOIN ods_jff.league_store b ON a.sp_id = b.sp_id\nLEFT JOIN ods_jff.sp_ent c ON a.sp_ent_id = c.sp_ent_id\nLEFT JOIN ods_jff.ent d ON c.ent_id = d.ent_id\nWHERE a.is_deleted = 0 -- and b.coop_mode = 3\nAND b.is_deleted = 0\n  AND d.ent_id > 0\n  AND a.intv_dt >= date_sub(current_date(), interval 1 MONTH)\nGROUP BY a.sp_id,\n         b.league_id,\n         b.league_name,\n         d.ent_id,\n         d.ent_short_name,\n         a.sp_ent_id,\n         d.sum_wage_min,\n         d.sum_wage_max;\n    '}, 2: {'color': 'blue', 'name': u'ods_jff.name_list', 'desc': '\xe8\xa1\xa8'}, 3: {'color': 'blue', 'name': u'ods_jff.league_store', 'desc': '\xe8\xa1\xa8'}, 4: {'color': 'blue', 'name': u'ods_jff.sp_ent', 'desc': '\xe8\xa1\xa8'}, 5: {'color': 'blue', 'name': u'ods_jff.ent', 'desc': '\xe8\xa1\xa8'}, 6: {'color': 'orange', 'name': u'sp_id', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 7: {'color': 'orange', 'name': u'league_id', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 8: {'color': 'orange', 'name': u'league_name', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 9: {'color': 'orange', 'name': u'ent_id', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 10: {'color': 'orange', 'name': u'ent_short_name', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 11: {'color': 'orange', 'name': u'sp_ent_id', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 12: {'color': 'orange', 'name': u'sum_wage_min', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 'ts': {'color': 'red', 'name': '\xe5\xbc\x80\xe5\xa7\x8b', 'desc': '\xe5\xbc\x80\xe5\xa7\x8b'}, 14: {'color': 'orange', 'name': '#meta2', 'desc': u'\u5b57\u6bb5 | count ( 1 )'}, 13: {'color': 'orange', 'name': u'sum_wage_max', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 15: {'color': 'orange', 'name': u'intv_cnt', 'desc': '\xe5\xad\x97\xe6\xae\xb5\xe5\x88\xab\xe5\x90\x8d'}}, 'dag': 'ts >> 1\n    \n1 >> 2\n1 >> 3\n1 >> 4\n1 >> 5\n14 >> 15\n5 >> 13\n3 >> 7\n5 >> 12\n5 >> 10\n2 >> 6\n2 >> 11\n5 >> 9\n3 >> 8\n1 >> 14 ', 'title': 'sql_2019-03-30 19:23:30'},
 {'nodes': {1: {'color': 'lightblue', 'name': 'SQL', 'desc': "select IDCardNum as id_card_num, concat(AdscriptionMonth, '-01') as bill_related_mo, floor(sum(Amount)*100) as wages, 1 as zxx_type\n from `ods_zhouxinxin`.wb_withdrawapply\n where AuditStatus=1 and AccountStatus=1 and ApplyType=1 and AdscriptionMonth<'2019-02-01'\n group by IDCardNum, AdscriptionMonth"}, 2: {'color': 'blue', 'name': u'`ods_zhouxinxin`.wb_withdrawapply', 'desc': '\xe8\xa1\xa8'}, 3: {'color': 'orange', 'name': u'IDCardNum', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 4: {'color': 'orange', 'name': u'id_card_num', 'desc': '\xe5\xad\x97\xe6\xae\xb5\xe5\x88\xab\xe5\x90\x8d'}, 5: {'color': 'orange', 'name': '#meta1', 'desc': u"\u5b57\u6bb5 | concat ( AdscriptionMonth '-01' )"}, 6: {'color': 'orange', 'name': u'bill_related_mo', 'desc': '\xe5\xad\x97\xe6\xae\xb5\xe5\x88\xab\xe5\x90\x8d'}, 7: {'color': 'orange', 'name': '#meta3', 'desc': u'\u5b57\u6bb5 | floor ( sum #meta2 *100 )'}, 8: {'color': 'orange', 'name': u'wages', 'desc': '\xe5\xad\x97\xe6\xae\xb5\xe5\x88\xab\xe5\x90\x8d'}, 9: {'color': 'orange', 'name': u'1', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 10: {'color': 'orange', 'name': u'zxx_type', 'desc': '\xe5\xad\x97\xe6\xae\xb5\xe5\x88\xab\xe5\x90\x8d'}, 'ts': {'color': 'red', 'name': '\xe5\xbc\x80\xe5\xa7\x8b', 'desc': '\xe5\xbc\x80\xe5\xa7\x8b'}}, 'dag': 'ts >> 1\n    \n1 >> 2\n9 >> 10\n7 >> 8\n5 >> 6\n3 >> 4\n2 >> 9\n2 >> 7\n2 >> 5\n2 >> 3', 'title': 'sql_2019-03-30 19:24:54'},
 {'nodes': {1: {'color': 'lightblue', 'name': 'SQL', 'desc': "truncate table transit_jff.ent_name_list_sum;\n\ninsert into transit_jff.ent_name_list_sum(ent_id, ent_short_name, id_card_num, sp_id, sp_short_name, intv_sts, intv_cnt)\nSELECT d.ent_id, d.ent_short_name, a.mbr_id_card_num as id_card_num, b.sp_id, b.sp_short_name, a.intv_sts, count(1) as intv_cnt\nFROM (\n\tselect intv_dt, mbr_id_card_num, sp_id, sp_ent_id, intv_sts, is_deleted,\n\tif(@id_card_num=temp.mbr_id_card_num and @sp_id=temp.sp_id and @intv_dt=temp.intv_dt, @rank := @rank+1, @rank := 1) as rank,\n\t@id_card_num := temp.mbr_id_card_num as id_card_num,  @intv_dt := temp.intv_dt, @sp_id := temp.sp_id\n\tfrom (\n\t\tselect a.intv_dt, a.mbr_id_card_num, a.sp_id, a.sp_ent_id, a.intv_sts, a.is_deleted \n\t\tfrom ods_jff.name_list a\n\t\tinner join (\n\t\t\tselect distinct mbr_id_card_num, sp_id, sp_ent_id from transit_jff.temp_name_list_updated\n\t\t) b\n\t\ton a.sp_id = b.sp_id and a.sp_ent_id = b.sp_ent_id and a.mbr_id_card_num = b.mbr_id_card_num\n\t\twhere a.is_deleted = 0 and a.mbr_id_card_num is not null and a.mbr_id_card_num != '' and a.intv_dt < current_date()\n\t\tand a.sp_id != 9172 and a.sp_id != 1006 and (a.trgt_sp_id = 0 or a.trgt_sp_id is null)\n\t\torder by a.mbr_id_card_num, a.sp_id, a.intv_dt, a.created_tm desc\n\t) temp,\n\t(select @id_card_num := null, @sp_id := null, @intv_dt := null, @rank := 0) r\n) a\ninner join ods_jff.sp b\non a.sp_id = b.sp_id\ninner join ods_jff.sp_ent c\non a.sp_ent_id = c.sp_ent_id\ninner join ods_jff.ent d\non c.ent_id = d.ent_id\nwhere a.rank = 1\ngroup by d.ent_id, d.ent_short_name, a.mbr_id_card_num, b.sp_id, b.sp_short_name, a.intv_sts;\n    "}, 2: {'color': 'blue', 'name': '#meta9', 'desc': u'\u8868 | ( SELECT intv_dt mbr_id_card_num sp_id sp_ent_id intv_sts is_deleted if #meta3 AS rank @id_card_num := temp.mbr_id_card_num AS id_card_num @intv_dt := temp.intv_dt @sp_id := temp.sp_id FROM #meta7 TEMP #meta8 r )'}, 3: {'color': 'blue', 'name': u'ods_jff.sp', 'desc': '\xe8\xa1\xa8'}, 4: {'color': 'blue', 'name': u'ods_jff.sp_ent', 'desc': '\xe8\xa1\xa8'}, 5: {'color': 'blue', 'name': u'ods_jff.ent', 'desc': '\xe8\xa1\xa8'}, 6: {'color': 'blue', 'name': '#meta7', 'desc': u"\u8868 | ( SELECT a.intv_dt a.mbr_id_card_num a.sp_id a.sp_ent_id a.intv_sts a.is_deleted FROM ods_jff.name_list a JOIN #meta4 b ON a.sp_id = b.sp_id AND a.sp_ent_id = b.sp_ent_id AND a.mbr_id_card_num = b.mbr_id_card_num WHERE a.is_deleted = 0 AND a.mbr_id_card_num IS NOT NULL AND a.mbr_id_card_num != '' AND a.intv_dt < current_date #meta5 AND a.sp_id != 9172 AND a.sp_id != 1006 AND #meta6 ORDER BY a.mbr_id_card_num a.sp_id a.intv_dt a.created_tm DESC )"}, 7: {'color': 'blue', 'name': '#meta8', 'desc': u'\u8868 | ( SELECT @id_card_num := NULL @sp_id := NULL @intv_dt := NULL @rank := 0 )'}, 8: {'color': 'blue', 'name': u'ods_jff.name_list', 'desc': '\xe8\xa1\xa8'}, 9: {'color': 'blue', 'name': '#meta4', 'desc': u'\u8868 | ( SELECT mbr_id_card_num sp_id sp_ent_id FROM transit_jff.temp_name_list_updated )'}, 10: {'color': 'blue', 'name': u'transit_jff.temp_name_list_updated', 'desc': '\xe8\xa1\xa8'}, 11: {'color': 'orange', 'name': u'ent_id', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 12: {'color': 'orange', 'name': u'ent_short_name', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 13: {'color': 'orange', 'name': u'mbr_id_card_num', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 14: {'color': 'orange', 'name': u'id_card_num', 'desc': '\xe5\xad\x97\xe6\xae\xb5\xe5\x88\xab\xe5\x90\x8d'}, 15: {'color': 'orange', 'name': u'sp_id', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 16: {'color': 'orange', 'name': u'sp_short_name', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 17: {'color': 'orange', 'name': u'intv_sts', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 18: {'color': 'orange', 'name': '#meta2', 'desc': u'\u5b57\u6bb5 | count ( 1 )'}, 19: {'color': 'orange', 'name': u'intv_cnt', 'desc': '\xe5\xad\x97\xe6\xae\xb5\xe5\x88\xab\xe5\x90\x8d'}, 20: {'color': 'orange', 'name': u'intv_dt', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 21: {'color': 'orange', 'name': u'sp_ent_id', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 22: {'color': 'orange', 'name': u'is_deleted', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 23: {'color': 'orange', 'name': '#meta3', 'desc': u'\u5b57\u6bb5 | if ( @id_card_num=temp.mbr_id_card_num AND @sp_id=temp.sp_id AND @intv_dt=temp.intv_dt @rank := @rank+1 @rank := 1 )'}, 24: {'color': 'orange', 'name': u'rank', 'desc': '\xe5\xad\x97\xe6\xae\xb5\xe5\x88\xab\xe5\x90\x8d'}, 25: {'color': 'orange', 'name': u'@id_card_num := temp.mbr_id_card_num', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 26: {'color': 'orange', 'name': u'@intv_dt := temp.intv_dt', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 27: {'color': 'orange', 'name': u'@sp_id := temp.sp_id', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 28: {'color': 'orange', 'name': u'( SELECT @id_card_num := NULL @sp_id := NULL @intv_dt := NULL @rank := 0 )', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 'ts': {'color': 'red', 'name': '\xe5\xbc\x80\xe5\xa7\x8b', 'desc': '\xe5\xbc\x80\xe5\xa7\x8b'}}, 'dag': 'ts >> 1\n    \n1 >> 2\n1 >> 3\n1 >> 4\n1 >> 5\n18 >> 19\n23 >> 24\n25 >> 14\n8 >> 22\n2 >> 27\n7 >> 28\n2 >> 26\n2 >> 25\n5 >> 12\n10 >> 13\n10 >> 15\n3 >> 16\n2 >> 23\n8 >> 17\n10 >> 21\n5 >> 11\n8 >> 20\n9 >> 10\n2 >> 7\n6 >> 8\n2 >> 6\n6 >> 9\n1 >> 18 ', 'title': 'sql_2019-03-30 19:24:56'},]
