# -*- coding: utf-8 -*-
"""
Created on Mon Mar 14 18:32:07 2022

@author: SKIM
"""

import pickle

with open(f'D:/WOS_RAW_2022_PARSED/out_all_uid_py.p','rb') as f:
    out_all_uid_py = pickle.load(f)

with open(f'D:/WOS_RAW_2022_PARSED/out_all_doctype.p','rb') as f:
    out_all_doctype = pickle.load(f)
    
with open(f'D:/WOS_RAW_2022_PARSED/out_all_subcate.p','rb') as f:
    out_all_subcate = pickle.load(f)
    
with open(f'D:/WOS_RAW_2022_PARSED/out_all_pubinfo.p','rb') as f:
    out_all_pubinfo = pickle.load(f)

with open(f'D:/WOS_RAW_2022_PARSED/out_all_ttl.p','rb') as f:
    out_all_ttl = pickle.load(f)
    
with open(f'D:/WOS_RAW_2022_PARSED/out_all_edge.p','rb') as f:
    out_all_edge = pickle.load(f)

with open(f'D:/WOS_RAW_2022_PARSED/out_all_key_au.p','rb') as f:
    out_all_key_au = pickle.load(f)
    
with open(f'D:/WOS_RAW_2022_PARSED/out_all_abst.p','rb') as f:
    out_all_abst = pickle.load(f)