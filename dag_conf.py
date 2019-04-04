# !/usr/bin/env python
# -*- coding: utf-8 -*-
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

NODES = [

 {'nodes': {1: {'color': 'lightblue', 'name': 'SQL', 'desc': "\n    insert into jff_ent_tmp.zhouxinxin_league_name_list(name_list_id, jff_name_list_id, ent_id,\nent_short_name, sp_id, sp_short_name, labor_sp_id, labor_sp_short_name, order_id, id_card_num,\ninterview_date, interview_status, work_card_num, work_status, entry_date, leave_date)\nSELECT a.name_list_id, a.jff_name_list_id, a.ent_id, d.ent_short_name, c.sp_id, c.league_name,\ne.real_b_sp_id, f.sp_short_name, a.rcrt_order_id, upper(a.id_card_num), a.intv_dt,\na.intv_sts, a.work_card_no, a.work_sts, a.entry_dt, a.leave_dt\nFROM (\n\tselect name_list_id, jff_name_list_id, ent_id, id_card_num, if(work_card_no='', null, work_card_no) as work_card_no,\n\tintv_dt, intv_sts, work_sts, entry_dt, leave_dt, rcrt_order_id, srce_sp_id, trgt_sp_id,\n\tif(@intv_dt=temp.intv_dt and @id_card_num=temp.id_card_num, @rank := @rank+1, @rank := 1) as rank,\n\t@intv_dt := temp.intv_dt,  @id_card_num := temp.id_card_num\n\tfrom (\n\t\tselect name_list_id, jff_name_list_id, ent_id, upper(id_card_num) as id_card_num, trgt_sp_id,\n\t\twork_card_no, intv_dt, intv_sts, work_sts, entry_dt, leave_dt, rcrt_order_id, srce_sp_id\n\t\tfrom `ods_zxx_2.0`.name_list\n\t\twhere is_deleted = 1 \n\t\torder by intv_dt, id_card_num, name_list_id desc\n\t\t) temp,\n\t\t(select @intv_dt := null, @id_card_num := null, @rank := 0) r\n) a\nleft join `ods_zxx_2.0`.sp_mapping b on a.srce_sp_id = b.virtual_sp_id\nleft join ods_jff.league_store c on b.real_sp_id = c.sp_id\nleft join `ods_zxx_2.0`.ent d on a.ent_id = d.ent_id\nleft join ods_jff.sp_real_coop e on a.trgt_sp_id = e.b_sp_id\nleft join ods_jff.sp f on e.real_b_sp_id = f.sp_id\nwhere a.intv_dt >= c.coop_start_dt and a.rank = 1\nand c.sp_id != 1006;\n    "}, 2: {'color': 'blue', 'name': '#meta8', 'desc': u'\u8868 | ( SELECT name_list_id jff_name_list_id ent_id id_card_num if #meta3 AS work_card_no intv_dt intv_sts work_sts entry_dt leave_dt rcrt_order_id srce_sp_id trgt_sp_id if #meta4 AS rank @intv_dt : = temp.intv_dt @id_card_num : = temp.id_card_num FROM #meta6 TEMP #meta7 r )'}, 3: {'color': 'blue', 'name': u'`ods_zxx_2.0`.sp_mapping', 'desc': '\xe8\xa1\xa8'}, 4: {'color': 'blue', 'name': u'ods_jff.league_store', 'desc': '\xe8\xa1\xa8'}, 5: {'color': 'blue', 'name': u'`ods_zxx_2.0`.ent', 'desc': '\xe8\xa1\xa8'}, 6: {'color': 'blue', 'name': u'ods_jff.sp_real_coop', 'desc': '\xe8\xa1\xa8'}, 7: {'color': 'blue', 'name': u'ods_jff.sp', 'desc': '\xe8\xa1\xa8'}, 8: {'color': 'blue', 'name': '#meta6', 'desc': u'\u8868 | ( SELECT name_list_id jff_name_list_id ent_id upper #meta5 AS id_card_num trgt_sp_id work_card_no intv_dt intv_sts work_sts entry_dt leave_dt rcrt_order_id srce_sp_id FROM `ods_zxx_2.0`.name_list WHERE is_deleted = 1 ORDER BY intv_dt id_card_num name_list_id DESC )'}, 9: {'color': 'blue', 'name': '#meta7', 'desc': u'\u8868 | ( SELECT @intv_dt : = NULL @id_card_num : = NULL @rank : = 0 )'}, 10: {'color': 'blue', 'name': u'`ods_zxx_2.0`.name_list', 'desc': '\xe8\xa1\xa8'}, 11: {'color': 'orange', 'name': u'upper', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 12: {'color': 'orange', 'name': u'@intv_dt', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 13: {'color': 'orange', 'name': u'jff_name_list_id', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 14: {'color': 'orange', 'name': u'work_sts', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 15: {'color': 'orange', 'name': u'sp_short_name', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 16: {'color': 'orange', 'name': u'leave_dt', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 17: {'color': 'orange', 'name': u' ', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 18: {'color': 'orange', 'name': u'ent_short_name', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 19: {'color': 'orange', 'name': u'sp_id', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 20: {'color': 'orange', 'name': u'@id_card_num', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 21: {'color': 'orange', 'name': u'trgt_sp_id', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 22: {'color': 'orange', 'name': u'real_b_sp_id', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 23: {'color': 'orange', 'name': u'@rank', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 24: {'color': 'orange', 'name': u'intv_dt', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 25: {'color': 'orange', 'name': u'srce_sp_id', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 26: {'color': 'orange', 'name': '#meta2', 'desc': u'\u5b57\u6bb5 | ( a.id_card_num ) '}, 27: {'color': 'orange', 'name': '#meta3', 'desc': u"\u5b57\u6bb5 | if ( work_card_no = '' NULL work_card_no ) "}, 28: {'color': 'orange', 'name': u'entry_dt', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 29: {'color': 'orange', 'name': '#meta4', 'desc': u'\u5b57\u6bb5 | if ( @intv_dt = temp.intv_dt AND @id_card_num = temp.id_card_num @rank : = @rank + 1 @rank : = 1 ) '}, 30: {'color': 'orange', 'name': '#meta5', 'desc': u'\u5b57\u6bb5 | ( id_card_num ) '}, 31: {'color': 'orange', 'name': u'name_list_id', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 32: {'color': 'orange', 'name': u'rcrt_order_id', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 33: {'color': 'orange', 'name': u'intv_sts', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 34: {'color': 'orange', 'name': u'league_name', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 35: {'color': 'orange', 'name': u'ent_id', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 36: {'color': 'pink', 'name': u'work_card_no', 'desc': '\xe5\xad\x97\xe6\xae\xb5\xe5\x88\xab\xe5\x90\x8d'}, 37: {'color': 'pink', 'name': u'rank', 'desc': '\xe5\xad\x97\xe6\xae\xb5\xe5\x88\xab\xe5\x90\x8d'}, 38: {'color': 'pink', 'name': u'id_card_num', 'desc': '\xe5\xad\x97\xe6\xae\xb5\xe5\x88\xab\xe5\x90\x8d'}, 'ts': {'color': 'red', 'name': '\xe5\xbc\x80\xe5\xa7\x8b', 'desc': '\xe5\xbc\x80\xe5\xa7\x8b'}}, 'dag': 'ts >> 1 \n27 >> 36\n29 >> 37\n30 >> 38\n10 >> 11\n9 >> 12\n10 >> 13\n10 >> 14\n7 >> 15\n10 >> 16\n9 >> 17\n5 >> 18\n4 >> 19\n9 >> 20\n10 >> 21\n6 >> 22\n9 >> 23\n10 >> 24\n10 >> 25\n1 >> 26\n2 >> 27\n10 >> 28\n2 >> 29\n10 >> 30\n10 >> 31\n10 >> 32\n10 >> 33\n4 >> 34\n10 >> 35\n8 >> 10\n1 >> 3\n1 >> 7\n2 >> 8\n2 >> 9\n1 >> 5\n1 >> 6\n1 >> 2\n1 >> 4', 'title': '1 \xe7\x94\x9f\xe6\x88\x90\xe5\x8a\xa0\xe7\x9b\x9f\xe5\xba\x97\xe5\x90\x8d\xe5\x8d\x95jff_ent_tmp.zhouxinxin_league_name_list'},
 {'nodes': {1: {'color': 'lightblue', 'name': 'SQL', 'desc': "\n        update jff_ent_tmp.zhouxinxin_league_name_list a\ninner join (\n\tselect ent_id, if(work_card_no='', null, work_card_no) as work_card_num, id_card_num, entry_dt as entry_date, leave_dt as leave_date,\n\tif(@entry_dt=temp.entry_dt and @id_card_num=temp.id_card_num, @rank := @rank+1, @rank := 1) as rank,\n\t@entry_dt := temp.entry_dt, @id_card_num := temp.id_card_num\n\tfrom (\n\t\tSELECT upper(id_card_num) as id_card_num, work_card_no, entry_dt, ent_id, max(leave_dt) as leave_dt, count(1) as num \n\t\tFROM `ods_zxx_2.0`.bill_monthly_batch_import_detail\n\t\twhere entry_dt > '1970-01-01'\n\t\tgroup by ent_id, upper(id_card_num), work_card_no, entry_dt\n\t\torder by ent_id, id_card_num, entry_dt, num desc\n\t)temp,\n\t(select @entry_dt := null, @id_card_num := null, @rank := 0) r\n) b on b.rank = 1 and a.id_card_num = b.id_card_num and coalesce(a.fixed_work_card_num, a.work_card_num) = b.work_card_num\nset a.hire_status = 1\nwhere a.interview_date >= '${MIN_INTV_DT}';"}, 2: {'color': 'blue', 'name': '#meta9', 'desc': u'\u8868 | ( SELECT ent_id if #meta1 AS work_card_num id_card_num entry_dt AS entry_date leave_dt AS leave_date if #meta2 AS rank @entry_dt : = temp.entry_dt @id_card_num : = temp.id_card_num FROM #meta7 TEMP #meta8 r )'}, 3: {'color': 'blue', 'name': '#meta7', 'desc': u"\u8868 | ( SELECT upper #meta3 AS id_card_num work_card_no entry_dt ent_id max #meta4 AS leave_dt count #meta5 AS num FROM `ods_zxx_2.0`.bill_monthly_batch_import_detail WHERE entry_dt > '1970 - 01 - 01' GROUP BY ent_id upper #meta6 work_card_no entry_dt ORDER BY ent_id id_card_num entry_dt num DESC )"}, 4: {'color': 'blue', 'name': '#meta8', 'desc': u'\u8868 | ( SELECT @entry_dt : = NULL @id_card_num : = NULL @rank : = 0 )'}, 5: {'color': 'blue', 'name': u'`ods_zxx_2.0`.bill_monthly_batch_import_detail', 'desc': '\xe8\xa1\xa8'}, 6: {'color': 'orange', 'name': '#meta2', 'desc': u'\u5b57\u6bb5 | if ( @entry_dt = temp.entry_dt AND @id_card_num = temp.id_card_num @rank : = @rank + 1 @rank : = 1 ) '}, 7: {'color': 'orange', 'name': '#meta1', 'desc': u"\u5b57\u6bb5 | if ( work_card_no = '' NULL work_card_no ) "}, 8: {'color': 'orange', 'name': u' ', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 9: {'color': 'orange', 'name': u'upper', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 10: {'color': 'orange', 'name': '#meta5', 'desc': u'\u5b57\u6bb5 | count ( 1 ) '}, 11: {'color': 'orange', 'name': u'work_card_no', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 12: {'color': 'orange', 'name': u'@rank', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 13: {'color': 'orange', 'name': '#meta3', 'desc': u'\u5b57\u6bb5 | ( id_card_num ) '}, 14: {'color': 'orange', 'name': u'@entry_dt', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 15: {'color': 'orange', 'name': u'entry_dt', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 16: {'color': 'orange', 'name': '#meta4', 'desc': u'\u5b57\u6bb5 | max ( leave_dt ) '}, 17: {'color': 'orange', 'name': u'ent_id', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 18: {'color': 'orange', 'name': u'@id_card_num', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 19: {'color': 'pink', 'name': u'rank', 'desc': '\xe5\xad\x97\xe6\xae\xb5\xe5\x88\xab\xe5\x90\x8d'}, 20: {'color': 'pink', 'name': u'work_card_num', 'desc': '\xe5\xad\x97\xe6\xae\xb5\xe5\x88\xab\xe5\x90\x8d'}, 21: {'color': 'pink', 'name': u'leave_date', 'desc': '\xe5\xad\x97\xe6\xae\xb5\xe5\x88\xab\xe5\x90\x8d'}, 22: {'color': 'pink', 'name': u'id_card_num', 'desc': '\xe5\xad\x97\xe6\xae\xb5\xe5\x88\xab\xe5\x90\x8d'}, 23: {'color': 'pink', 'name': u'entry_date', 'desc': '\xe5\xad\x97\xe6\xae\xb5\xe5\x88\xab\xe5\x90\x8d'}, 24: {'color': 'pink', 'name': u'leave_dt', 'desc': '\xe5\xad\x97\xe6\xae\xb5\xe5\x88\xab\xe5\x90\x8d'}, 25: {'color': 'pink', 'name': u'num', 'desc': '\xe5\xad\x97\xe6\xae\xb5\xe5\x88\xab\xe5\x90\x8d'}, 'ts': {'color': 'red', 'name': '\xe5\xbc\x80\xe5\xa7\x8b', 'desc': '\xe5\xbc\x80\xe5\xa7\x8b'}}, 'dag': 'ts >> 1 \n6 >> 19\n7 >> 20\n24 >> 21\n13 >> 22\n15 >> 23\n16 >> 24\n10 >> 25\n2 >> 6\n2 >> 7\n4 >> 8\n5 >> 9\n5 >> 10\n5 >> 11\n4 >> 12\n5 >> 13\n4 >> 14\n5 >> 15\n5 >> 16\n5 >> 17\n4 >> 18\n2 >> 4\n1 >> 2\n2 >> 3\n3 >> 5', 'title': '2.1 \xe6\xa0\xb9\xe6\x8d\xae\xe5\x91\xa8\xe8\x96\xaa\xe8\x96\xaa\xe7\x9a\x84\xe5\x91\xa8\xe6\x9c\x88\xe8\x96\xaa\xe5\x90\x8d\xe5\x8d\x95\xe8\xa1\xa5\xe5\x85\x85\xe5\x85\xa5\xe8\x81\x8c\xe7\x8a\xb6\xe6\x80\x81, \xe6\x9c\x88+\xe5\x91\xa8\xe8\xb4\xa6\xe5\x8d\x95\xe9\x87\x8c\xe5\xad\x98\xe5\x9c\xa8\xe7\x9a\x84\xef\xbc\x8c\xe5\xb0\xb1\xe6\x98\xaf\xe5\x9c\xa8\xe8\x81\x8c\xef\xbc\x8c\xe6\xb2\xa1\xe6\x9c\x89\xe7\x9a\x84\xe5\xb0\xb1\xe6\x98\xaf\xe7\xa6\xbb\xe8\x81\x8c'},
 {'nodes': {1: {'color': 'lightblue', 'name': 'SQL', 'desc': "\n        update jff_ent_tmp.zhouxinxin_league_name_list a\ninner join (\n\tselect ent_id, if(work_card_no='', null, work_card_no) as work_card_num, id_card_num, entry_dt as entry_date, leave_dt as leave_date,\n\tif(@entry_dt=temp.entry_dt and @id_card_num=temp.id_card_num, @rank := @rank+1, @rank := 1) as rank,\n\t@entry_dt := temp.entry_dt, @id_card_num := temp.id_card_num\n\tfrom (\n\t\tSELECT upper(id_card_num) as id_card_num, work_card_no, entry_dt, ent_id, max(leave_dt) as leave_dt, count(1) as num \n\t\tFROM `ods_zxx_2.0`.bill_weekly_batch_import_detail\n\t\twhere entry_dt > '1970-01-01'\n\t\tgroup by ent_id, upper(id_card_num), work_card_no, entry_dt\n\t\torder by ent_id, id_card_num, entry_dt, num desc\n\t)temp,\n\t(select @entry_dt := null, @id_card_num := null, @rank := 0) r\n) b on b.rank = 1 and a.id_card_num = b.id_card_num and coalesce(a.fixed_work_card_num, a.work_card_num) = b.work_card_num\nset a.hire_status = 1\nwhere a.interview_date >= '${MIN_INTV_DT}';\n\nupdate jff_ent_tmp.zhouxinxin_league_name_list\nset hire_status = 0\nwhere hire_status is null\nand interview_date >= '${MIN_INTV_D}' "}, 2: {'color': 'blue', 'name': '#meta9', 'desc': u'\u8868 | ( SELECT ent_id if #meta1 AS work_card_num id_card_num entry_dt AS entry_date leave_dt AS leave_date if #meta2 AS rank @entry_dt : = temp.entry_dt @id_card_num : = temp.id_card_num FROM #meta7 TEMP #meta8 r )'}, 3: {'color': 'blue', 'name': '#meta7', 'desc': u"\u8868 | ( SELECT upper #meta3 AS id_card_num work_card_no entry_dt ent_id max #meta4 AS leave_dt count #meta5 AS num FROM `ods_zxx_2.0`.bill_weekly_batch_import_detail WHERE entry_dt > '1970 - 01 - 01' GROUP BY ent_id upper #meta6 work_card_no entry_dt ORDER BY ent_id id_card_num entry_dt num DESC )"}, 4: {'color': 'blue', 'name': '#meta8', 'desc': u'\u8868 | ( SELECT @entry_dt : = NULL @id_card_num : = NULL @rank : = 0 )'}, 5: {'color': 'blue', 'name': u'`ods_zxx_2.0`.bill_weekly_batch_import_detail', 'desc': '\xe8\xa1\xa8'}, 6: {'color': 'orange', 'name': '#meta2', 'desc': u'\u5b57\u6bb5 | if ( @entry_dt = temp.entry_dt AND @id_card_num = temp.id_card_num @rank : = @rank + 1 @rank : = 1 ) '}, 7: {'color': 'orange', 'name': '#meta1', 'desc': u"\u5b57\u6bb5 | if ( work_card_no = '' NULL work_card_no ) "}, 8: {'color': 'orange', 'name': u' ', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 9: {'color': 'orange', 'name': u'upper', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 10: {'color': 'orange', 'name': '#meta5', 'desc': u'\u5b57\u6bb5 | count ( 1 ) '}, 11: {'color': 'orange', 'name': u'work_card_no', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 12: {'color': 'orange', 'name': u'@rank', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 13: {'color': 'orange', 'name': '#meta3', 'desc': u'\u5b57\u6bb5 | ( id_card_num ) '}, 14: {'color': 'orange', 'name': u'@entry_dt', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 15: {'color': 'orange', 'name': u'entry_dt', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 16: {'color': 'orange', 'name': '#meta4', 'desc': u'\u5b57\u6bb5 | max ( leave_dt ) '}, 17: {'color': 'orange', 'name': u'ent_id', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 18: {'color': 'orange', 'name': u'@id_card_num', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 19: {'color': 'pink', 'name': u'rank', 'desc': '\xe5\xad\x97\xe6\xae\xb5\xe5\x88\xab\xe5\x90\x8d'}, 20: {'color': 'pink', 'name': u'work_card_num', 'desc': '\xe5\xad\x97\xe6\xae\xb5\xe5\x88\xab\xe5\x90\x8d'}, 21: {'color': 'pink', 'name': u'leave_date', 'desc': '\xe5\xad\x97\xe6\xae\xb5\xe5\x88\xab\xe5\x90\x8d'}, 22: {'color': 'pink', 'name': u'id_card_num', 'desc': '\xe5\xad\x97\xe6\xae\xb5\xe5\x88\xab\xe5\x90\x8d'}, 23: {'color': 'pink', 'name': u'entry_date', 'desc': '\xe5\xad\x97\xe6\xae\xb5\xe5\x88\xab\xe5\x90\x8d'}, 24: {'color': 'pink', 'name': u'leave_dt', 'desc': '\xe5\xad\x97\xe6\xae\xb5\xe5\x88\xab\xe5\x90\x8d'}, 25: {'color': 'pink', 'name': u'num', 'desc': '\xe5\xad\x97\xe6\xae\xb5\xe5\x88\xab\xe5\x90\x8d'}, 'ts': {'color': 'red', 'name': '\xe5\xbc\x80\xe5\xa7\x8b', 'desc': '\xe5\xbc\x80\xe5\xa7\x8b'}}, 'dag': 'ts >> 1 \n6 >> 19\n7 >> 20\n24 >> 21\n13 >> 22\n15 >> 23\n16 >> 24\n10 >> 25\n2 >> 6\n2 >> 7\n4 >> 8\n5 >> 9\n5 >> 10\n5 >> 11\n4 >> 12\n5 >> 13\n4 >> 14\n5 >> 15\n5 >> 16\n5 >> 17\n4 >> 18\n2 >> 4\n1 >> 2\n2 >> 3\n3 >> 5', 'title': '2.2 \xe6\xa0\xb9\xe6\x8d\xae\xe5\x91\xa8\xe8\x96\xaa\xe8\x96\xaa\xe7\x9a\x84\xe5\x91\xa8\xe6\x9c\x88\xe8\x96\xaa\xe5\x90\x8d\xe5\x8d\x95\xe8\xa1\xa5\xe5\x85\x85\xe5\x85\xa5\xe8\x81\x8c\xe7\x8a\xb6\xe6\x80\x81, \xe6\x9c\x88+\xe5\x91\xa8\xe8\xb4\xa6\xe5\x8d\x95\xe9\x87\x8c\xe5\xad\x98\xe5\x9c\xa8\xe7\x9a\x84\xef\xbc\x8c\xe5\xb0\xb1\xe6\x98\xaf\xe5\x9c\xa8\xe8\x81\x8c\xef\xbc\x8c\xe6\xb2\xa1\xe6\x9c\x89\xe7\x9a\x84\xe5\xb0\xb1\xe6\x98\xaf\xe7\xa6\xbb\xe8\x81\x8c'},
 {'nodes': {1: {'color': 'lightblue', 'name': 'SQL', 'desc': '\n        insert data into jff_cb_name_list\xef\xbc\x9a\nSELECT distinct a.name_list_id, a.mbr_id_card_num,\nc.sp_id,  a.intv_dt, a.rcrt_typ,\na.intv_sts, b.hire_status\nFROM `ods_jff`.name_list a\nleft join ods_jff.league_store c on a.sp_id = c.sp_id\nleft join jff_ent_tmp.zhouxinxin_league_name_list b\non upper(a.mbr_id_card_num)=b.id_card_num and a.intv_dt=b.interview_date\nwhere a.intv_dt >= c.coop_start_dt\nand a.intv_dt <= CURRENT_DATE\nand c.sp_id != 1006 and a.is_deleted = 0; '}, 2: {'color': 'blue', 'name': u'`ods_jff`.name_list', 'desc': '\xe8\xa1\xa8'}, 3: {'color': 'blue', 'name': u'ods_jff.league_store', 'desc': '\xe8\xa1\xa8'}, 4: {'color': 'blue', 'name': u'jff_ent_tmp.zhouxinxin_league_name_list', 'desc': '\xe8\xa1\xa8'}, 5: {'color': 'orange', 'name': u'hire_status', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 6: {'color': 'orange', 'name': u'intv_dt', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 7: {'color': 'orange', 'name': u'sp_id', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 8: {'color': 'orange', 'name': u'intv_sts', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 9: {'color': 'orange', 'name': u'name_list_id', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 10: {'color': 'orange', 'name': u'rcrt_typ', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 11: {'color': 'orange', 'name': u'mbr_id_card_num', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 'ts': {'color': 'red', 'name': '\xe5\xbc\x80\xe5\xa7\x8b', 'desc': '\xe5\xbc\x80\xe5\xa7\x8b'}}, 'dag': 'ts >> 1 \n4 >> 5\n2 >> 6\n3 >> 7\n2 >> 8\n2 >> 9\n2 >> 10\n2 >> 11\n1 >> 2\n1 >> 3\n1 >> 4', 'title': '3 \xe6\x8a\xbd\xe5\x8f\x96\xe5\xad\x97\xe6\xae\xb5\xe7\x94\x9f\xe6\x88\x90\xe7\xae\x80\xe6\x98\x93\xe5\x8a\xa0\xe7\x9b\x9f\xe5\xba\x97\xe5\x90\x8d\xe5\x8d\x95jff_ent_tmp.jff_cb_name_list'},
 {'nodes': {1: {'color': 'lightblue', 'name': 'SQL', 'desc': "\n        select intv_dt, DATE_FORMAT(intv_dt, '%Y-%m') as interview_month, \ncount(*) as d_total,\ncount(rcrt_typ=2 or null) as d_zxx_total, \ncount(intv_sts=2 or null) as d_intv,\ncount(rcrt_typ=2 and intv_sts=2 or null) as d_zxx_intv, \ncount(rcrt_typ=2 and intv_sts=2 and hire_status=1 or null) as d_zxx_hire, \ncount(rcrt_typ=0 or null) as d_nzxx_total, \ncount(rcrt_typ=0 and intv_sts=2 or null) as d_nzxx_intv\nfrom jff_ent_tmp.jff_cb_name_list\ngroup by intv_dt "}, 2: {'color': 'orange', 'name': u'intv_dt', 'desc': '\xe5\xad\x97\xe6\xae\xb5'}, 3: {'color': 'orange', 'name': '#meta2', 'desc': u'\u5b57\u6bb5 | count ( * ) '}, 4: {'color': 'orange', 'name': '#meta3', 'desc': u'\u5b57\u6bb5 | count ( rcrt_typ = 2 OR NULL ) '}, 5: {'color': 'orange', 'name': '#meta1', 'desc': u"\u5b57\u6bb5 | DATE_FORMAT ( intv_dt '%Y - %m' ) "}, 6: {'color': 'orange', 'name': '#meta6', 'desc': u'\u5b57\u6bb5 | count ( rcrt_typ = 2 AND intv_sts = 2 AND hire_status = 1 OR NULL ) '}, 7: {'color': 'orange', 'name': '#meta7', 'desc': u'\u5b57\u6bb5 | count ( rcrt_typ = 0 OR NULL ) '}, 8: {'color': 'orange', 'name': '#meta4', 'desc': u'\u5b57\u6bb5 | count ( intv_sts = 2 OR NULL ) '}, 9: {'color': 'orange', 'name': '#meta5', 'desc': u'\u5b57\u6bb5 | count ( rcrt_typ = 2 AND intv_sts = 2 OR NULL ) '}, 10: {'color': 'orange', 'name': '#meta8', 'desc': u'\u5b57\u6bb5 | count ( rcrt_typ = 0 AND intv_sts = 2 OR NULL ) '}, 11: {'color': 'pink', 'name': u'd_total', 'desc': '\xe5\xad\x97\xe6\xae\xb5\xe5\x88\xab\xe5\x90\x8d'}, 12: {'color': 'pink', 'name': u'd_zxx_total', 'desc': '\xe5\xad\x97\xe6\xae\xb5\xe5\x88\xab\xe5\x90\x8d'}, 'ts': {'color': 'red', 'name': '\xe5\xbc\x80\xe5\xa7\x8b', 'desc': '\xe5\xbc\x80\xe5\xa7\x8b'}, 14: {'color': 'pink', 'name': u'd_zxx_hire', 'desc': '\xe5\xad\x97\xe6\xae\xb5\xe5\x88\xab\xe5\x90\x8d'}, 13: {'color': 'pink', 'name': u'interview_month', 'desc': '\xe5\xad\x97\xe6\xae\xb5\xe5\x88\xab\xe5\x90\x8d'}, 16: {'color': 'pink', 'name': u'd_intv', 'desc': '\xe5\xad\x97\xe6\xae\xb5\xe5\x88\xab\xe5\x90\x8d'}, 17: {'color': 'pink', 'name': u'd_zxx_intv', 'desc': '\xe5\xad\x97\xe6\xae\xb5\xe5\x88\xab\xe5\x90\x8d'}, 18: {'color': 'pink', 'name': u'd_nzxx_intv', 'desc': '\xe5\xad\x97\xe6\xae\xb5\xe5\x88\xab\xe5\x90\x8d'}, 15: {'color': 'pink', 'name': u'd_nzxx_total', 'desc': '\xe5\xad\x97\xe6\xae\xb5\xe5\x88\xab\xe5\x90\x8d'}}, 'dag': 'ts >> 1 \n3 >> 11\n4 >> 12\n5 >> 13\n6 >> 14\n7 >> 15\n8 >> 16\n9 >> 17\n10 >> 18\n1 >> 2\n1 >> 3\n1 >> 4\n1 >> 5\n1 >> 6\n1 >> 7\n1 >> 8\n1 >> 9\n1 >> 10', 'title': '4 \xe7\x94\x9f\xe6\x88\x90\xe7\xbb\x9f\xe8\xae\xa1\xe7\xbb\x93\xe6\x9e\x9c'},
 {'nodes': {1: {'color': 'lightblue', 'name': 'SQL', 'desc': "\n        select \ncount(distinct vIDCardNum) as cnt\nfrom \nods_woda.tbbrokeruserorderstatus ta\nwhere \neStatus=1\nand dtcheckintime>=%s\nand dtcheckintime<%s\nand  iCheckinRecruitTmpID in (\n        select iRecruitTmpID from ods_woda.tbRecruitTmp where vPositionName like '%%\xe5\x91\xa8\xe8\x96\xaa\xe8\x96\xaa%%' and eStatus=1\n)\nand eInterviewStatus in(1,2)\nand eJFFInterviewStatus=2\nand exists(\nselect \nmbr_id_card_num\nfrom \nods_jff.name_list \nwhere \nintv_dt>=%s\nand intv_dt<%s\nand rcrt_typ=1\nand is_deleted=0\nand left(ta.dtCheckinTime,10) = intv_dt\nand ta.vIDCardNum = mbr_id_card_num\n)"}, 2: {'color': 'blue', 'name': u'ods_woda.tbbrokeruserorderstatus', 'desc': '\xe8\xa1\xa8'}, 3: {'color': 'orange', 'name': '#meta1', 'desc': u'\u5b57\u6bb5 | count ( vIDCardNum ) '}, 4: {'color': 'pink', 'name': u'cnt', 'desc': '\xe5\xad\x97\xe6\xae\xb5\xe5\x88\xab\xe5\x90\x8d'}, 'ts': {'color': 'red', 'name': '\xe5\xbc\x80\xe5\xa7\x8b', 'desc': '\xe5\xbc\x80\xe5\xa7\x8b'}}, 'dag': 'ts >> 1 \n3 >> 4\n2 >> 3\n1 >> 2', 'title': '4 \xe7\x94\x9f\xe6\x88\x90\xe7\xbb\x9f\xe8\xae\xa1\xe7\xbb\x93\xe6\x9e\x9c'},]
