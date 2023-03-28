# -*- coding: utf-8 -*-
"""
Created on Sun Mar 19 18:10:13 2023

@author: ASUS
"""

#网络资源包
import requests as rq
import time
import mysql.connector
from bs4 import BeautifulSoup as bs
import json
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

#自己的包
from bilibili import BilibiliHotSpyder
import bilidm_pb2

if __name__ == "__main__":
    #加载配置文件
    with open(r"./config.json", 'r', encoding='utf-8') as f:
        config=json.load(f)
    headers=config["headers"]
    database=config["database"]
    terms=config["terms"]
    log_path=config["logpath"]

    #走你
    b_spy=BilibiliHotSpyder(terms=terms, headers=headers, database=database, log_path=log_path)
    try:
        b_spy.Run()
    except Exception as e:
        b_spy.closeSql()
        b_spy.log(str(e))
        
        
#    调试用的
#    ids={'term': '100', 'bvid': 'BV1NM411c7kM', 'aid': 480217717, 'cid': 960866821}
#    try:
#        b_spy.getVideo(ids)
#        b_spy.getBullet('2023-01-11 12:00:00',ids)
#        b_spy.closeSql()
#    except:
#        b_spy.closeSql()

