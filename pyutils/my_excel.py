# -*- coding: utf-8 -*-
import os
import pandas as pd


def xls_join(in_xls_dir, new_xls_path):
    list = os.listdir(in_xls_dir)
    writer = pd.ExcelWriter(new_xls_path)
    for file in list:
        if u'.xls' in file and u'jff_demand' not in file:
            df = pd.read_excel(in_xls_dir + file)
            df.to_excel(writer, sheet_name=file.replace('.xls', ''), index=False)
    writer.save()
