# -*- coding: utf-8 -*-
"""
Created on Tue Mar  8 11:59:32 2022

@author: SKIM
"""

import pandas as pd
import xml.etree.ElementTree as etree
import datetime
import os
import pickle

# file_name = 'D:/WOS_RAW_2022/tmp/WR_2020_20220227090101_CORE_0017.xml'

dir_path = 'D:/WOS_RAW_2022/unzip_2006_2021'
file_df = pd.DataFrame(columns=['fname','year','collection','part', 'fpath'])

for (root, directories, files) in os.walk(dir_path):
    for file in files:
        file_path = os.path.join(root,file)
        year = file.split('_')[1]
        collection = file.split('_')[3]
        part = file.split('_')[4].split('.')[0]
        file_df = file_df.append({'fname': file, 'year':year, 'collection':collection, 'part':part, 'fpath':file_path} , ignore_index=True)



#%% main
#####################################
## 파싱 메인. 스크립트 맨 아래 함수부터 실행할 것!
years = range(2016,2022)
for py in years:
    wos_parsed_output_year(py)



#%% after main, check results...
#####################################

## 2006-2021 차례로 불러서 uid만 모아서 unique 뽑아보고, 몇 개인지, 중복 있는지 확인하고 tc 파일과 비교

out_all = pd.DataFrame()
for py in range(2006,2022):
    with open(f'D:/WOS_RAW_2022_PARSED/output_{py}.p','rb') as f:
        out_year = pickle.load(f)
    out_all = out_all.append(out_year)
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f'\n !!! Now loaded output : {py} !!! ... Check datetime: {now} \n')

out_all.shape ## (43831184, 10)
len(out_all.uids.unique())  ## 43830620 (43831184 - 43830620 = 564 중복)

## 중복제거를 위한 체크: dups --> 중복 uid 일단 모두 모아두고
# dups = out_all.uids[out_all.uids.duplicated(keep = False)]
# dups = dups.sort_index()
# 중복은 core collection과 esci 컬렉션에서 각각 나오는 564개에서 발생....
## core collection을 남겨라.. 멀티인덱스 소팅하고 uid 기준으로 drop duplicate!
out_all = out_all.sort_index()
out_all = out_all.drop_duplicates(subset='uids',keep = 'first') # core를 남기고.. esci 컬렉션 날림.
out_all.shape ## (43830620, 10)


#%% 데이터 배포를 위한 pickle 제작
#####################################

## 전체 데이터 한방에 저장 -- 메모리 부족으로 실패 --> 컬럼별로 저장하자
# with open(f'D:/WOS_RAW_2022_PARSED/output_all.p','wb') as file:
#     pickle.dump(out_all, file)
# ['uids', 'py', 'pubinfo', 'subcate_list', 'ttl', 'doctype', 'doctype_norm', 'edge_list', 'keyword_au_list', 'abst']

out_all_uid_py = out_all[['uids','py']] # 저장해서 이후 다른 데이터 중복제거 기준으로 활용!
with open(f'D:/WOS_RAW_2022_PARSED/out_all_uid_py.p','wb') as file:
    pickle.dump(out_all_uid_py, file)


## 나머지 컬럼 2-3개씩 로드해서 정리,, pickle 저장
out_all_doctype = pd.DataFrame()
out_all_subcate = pd.DataFrame()
out_all_pubinfo = pd.DataFrame()
out_all_edge = pd.DataFrame()
out_all_ttl = pd.DataFrame()
out_all_key_au = pd.DataFrame()
out_all_abst = pd.DataFrame()

for py in range(2006,2022):
    with open(f'D:/WOS_RAW_2022_PARSED/output_{py}.p','rb') as f:
        out_year = pickle.load(f)
    # out_year_doctype = out_year[['doctype','doctype_norm']]
    # out_year_subcate = out_year[['subcate_list']]
    # out_year_pubinfo = out_year[['pubinfo']]
    # out_year_edge = out_year[['edge_list']]
    # out_year_ttl = out_year[['ttl']]
    out_year_key_au = out_year[['keyword_au_list']]
    out_year_abst = out_year[['abst']]

    # out_all_doctype = out_all_doctype.append(out_year_doctype)
    # out_all_subcate = out_all_subcate.append(out_year_subcate)
    # out_all_pubinfo = out_all_pubinfo.append(out_year_pubinfo)
    # out_all_edge = out_all_edge.append(out_year_edge)
    # out_all_ttl = out_all_ttl.append(out_year_ttl)
    out_all_key_au = out_all_key_au.append(out_year_key_au)
    out_all_abst = out_all_abst.append(out_year_abst)

    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f'\n !!! Now loaded output : {py} !!! ... Check datetime: {now} \n')

## 인덱스 소팅
# out_all_doctype = out_all_doctype.sort_index()
# out_all_subcate = out_all_subcate.sort_index()
# out_all_pubinfo = out_all_pubinfo.sort_index()
# out_all_edge = out_all_edge.sort_index()
# out_all_ttl = out_all_ttl.sort_index()
out_all_key_au = out_all_key_au.sort_index()
out_all_abst = out_all_abst.sort_index()

## 중복제거 후 저장해둔 out_all_uid_py 인덱스 사용해서 다른 테이블 중복제거
with open(f'D:/WOS_RAW_2022_PARSED/out_all_uid_py.p','rb') as f:
    out_all_uid_py = pickle.load(f)

## 중복제거(먼저 제거한 데이터에서 인덱스 사용)
# out_all_doctype = out_all_doctype[out_all_uid_py.index]
# out_all_subcate = out_all_subcate[out_all_uid_py.index]
# out_all_pubinfo = out_all_pubinfo[out_all_uid_py.index]
# out_all_edge = out_all_edge.loc[out_all_uid_py.index]
# out_all_ttl = out_all_ttl.loc[out_all_uid_py.index]
out_all_key_au = out_all_key_au.loc[out_all_uid_py.index]
out_all_abst = out_all_abst.loc[out_all_uid_py.index]

## 피클 저장
# with open(f'D:/WOS_RAW_2022_PARSED/out_all_doctype.p','wb') as file:
#     pickle.dump(out_all_doctype, file)
# with open(f'D:/WOS_RAW_2022_PARSED/out_all_subcate.p','wb') as file:
#     pickle.dump(out_all_subcate, file)
# with open(f'D:/WOS_RAW_2022_PARSED/out_all_pubinfo.p','wb') as file:
#     pickle.dump(out_all_pubinfo, file)
# with open(f'D:/WOS_RAW_2022_PARSED/out_all_edge.p','wb') as file:
#     pickle.dump(out_all_edge, file)
# with open(f'D:/WOS_RAW_2022_PARSED/out_all_ttl.p','wb') as file:
#     pickle.dump(out_all_ttl, file)
with open(f'D:/WOS_RAW_2022_PARSED/out_all_key_au.p','wb') as file:
    pickle.dump(out_all_key_au, file)
with open(f'D:/WOS_RAW_2022_PARSED/out_all_abst.p','wb') as file:
    pickle.dump(out_all_abst, file)

  
#%% 파싱 관련 함수들 !! 여기부터 실행 해두고....
################################################

def wos_parsed_output_year(py):

    py = str(py)
    trial_files = file_df[(file_df.year==py) & (file_df.collection == 'CORE')]
    trial_files = file_df[(file_df.year==py)]
    
    sample = trial_files['fname']
    
    out_year = pd.DataFrame()
    
    for fname in sample:
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f'\n !!! Now start with file : {fname} !!! ... Check datetime: {now} \n')
        
    # fname = sample.iloc[0]
    
        tmp = wos_xml_extract(dir_path, fname)
        out_year = out_year.append(tmp)
    
    with open(f'D:/WOS_RAW_2022_PARSED/output_{py}.p','wb') as file:
            pickle.dump(out_year, file)

#%%

def wos_xml_extract(dir_path, fname):

    #%%
    
    file_name = os.path.join(dir_path, fname)
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f'Now xml file {file_name} loads in PID : {os.getpid()}... Check now datetime: {now}')
    
    tree = etree.parse(file_name)
    root = tree.getroot()
    
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f'Now xml file {file_name} loaded in PID : {os.getpid()}... Check now datetime: {now}')
    
    # root.findall(".//{http://clarivate.com/schema/wok5.30/public/FullRecord}uid")
    
    #%%
    
    #     Now start parsing ... xml file D:/WOS_RAW_2022/tmp/WR_2020_20220227090101_CORE_0017.xml, Check now datetime: 2022-03-09 03:35:40
    # Now parsing done ... xml file D:/WOS_RAW_2022/tmp/WR_2020_20220227090101_CORE_0017.xml, Check now datetime: 2022-03-09 03:36:03
    ## 리스트 형태로 빠르게 저장해두는 건 파일 하나 당 1분도 안걸림..!!!
    ## parsing_hisroty 잘 저장해 두고 --> 멀티인덱스 형태로 붙여두면 더 확실할 듯. (일단 붙였음.)
    ## 연도별로 리스트 통합한 후 데이터프레임 혹은 csv로 저장.... 활용....
    ## 그 전에 분석 대상 연도 (2000~2022??)에 대한 parsing_history 얼른 뽑아서 2022_9주차 tc목록과 대조하고
    ## 최종 분석 대상 논문 목록 선정하는 작업을 먼저 해보는 것 --> 이걸 한번 해둬야 나중에 최종 분석 시점에 이 방법으로 기준 잡고 시작 가능..
    
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f'Now start parsing ... xml file {file_name}, Check now datetime: {now}')
    
    uids = [record[0].text for record in root]
    
    py = [record.find(".//{http://clarivate.com/schema/wok5.30/public/FullRecord}pub_info").get('pubyear') for record in root]
    
    ttl = [record.find(".//{http://clarivate.com/schema/wok5.30/public/FullRecord}title[@type = 'item']").text for record in root]

    # get pubinfo    
    pubinfo = [record.find(".//{http://clarivate.com/schema/wok5.30/public/FullRecord}pub_info").attrib for record in root]

    
    ## uid 하나당 1개인 정보들은 휙휙 되는데...
    ## 여러개인 정보들은 어떻게?? 혹은 없기도 한 정보들.... Nonetype 발생하는 애들..... -- 함수로.. 만들기엔 내용이 많이 다름....
    
    def target(record):
        try:
            target_uids = [ref_id.find("{http://clarivate.com/schema/wok5.30/public/FullRecord}uid").text for ref_id in record.findall(".//{http://clarivate.com/schema/wok5.30/public/FullRecord}reference")]
        except:
            target_uids = []
        return target_uids
        
       
    edge_list = [target(record) for record in root]


    ## absract 
    def abstract(record):
        try:
            abst = record.find(".//{http://clarivate.com/schema/wok5.30/public/FullRecord}abstract_text/{http://clarivate.com/schema/wok5.30/public/FullRecord}p").text
        except:
            abst = ""
        return abst
    
    abst = [abstract(record) for record in root]
    
    
    # doctype
    def doctype(record):
        try:
            docts = [dt.text for dt in record.findall(".//{http://clarivate.com/schema/wok5.30/public/FullRecord}doctypes/{http://clarivate.com/schema/wok5.30/public/FullRecord}doctype")] 
        except:
            docts = []
        return docts
    
    doctype = [doctype(record) for record in root]
    

    # key_au
    def key_au(record):
        try:
            keywords = [ak.text for ak in record.findall(".//{http://clarivate.com/schema/wok5.30/public/FullRecord}keywords/{http://clarivate.com/schema/wok5.30/public/FullRecord}keyword")]
        except:
            keywords = []
        return keywords
    
    keyword_au_list = [key_au(record) for record in root]
    
        # doctype_norm
    def doctype_norm(record):
        try:
            docts = [dt.text for dt in record.findall(".//{http://clarivate.com/schema/wok5.30/public/FullRecord}normalized_doctypes/{http://clarivate.com/schema/wok5.30/public/FullRecord}doctype")] 
        except:
            docts = []
        return docts
    
    doctype_norm = [doctype_norm(record) for record in root]

    # subject category
    def sub_cate(record):
        try:
            subcates = [sc.text for sc in record.findall(".//{http://clarivate.com/schema/wok5.30/public/FullRecord}subject[@ascatype = 'traditional']")]
        except:
            subcates = []
        return subcates
    
    subcate_list = [sub_cate(record) for record in root]

    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f'Now parsing done ... xml file {file_name}, Check now datetime: {now}')

    #%%
    
    # 각 리스트를 하나로 묶어서 이동.저장.식별 편하도록 패키징 (인덱스 --> WOS uid로 지정 + 파일명으로 다중 인덱싱?)
    
    # tmp = pd.DataFrame(zip(uids, pubinfo, subcate_list), index = pd.Index(uids), columns = ['uids', 'pubinfo','subcate_list'])
    
    fname = fname
    ex_prod=[uids,[fname]]
    ex_index = pd.MultiIndex.from_product(ex_prod, names=['uid','fname'])
    
    tmp = pd.DataFrame(zip(uids, py, pubinfo, subcate_list, ttl, doctype, doctype_norm, edge_list, keyword_au_list, abst), index = ex_index, columns = ['uids', 'py', 'pubinfo', 'subcate_list', 'ttl', 'doctype', 'doctype_norm', 'edge_list', 'keyword_au_list', 'abst'])
    
    return tmp
