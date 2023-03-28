# -*- coding: utf-8 -*-
"""
Created on Thu Mar 16 21:21:15 2023

@author: ASUS
"""

import requests as rq
import time
import mysql.connector
from bs4 import BeautifulSoup as bs
import json
import bilidm_pb2
from datetime import datetime

#更新coockie用
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait


class BilibiliHotSpyder:
    def __init__(self, terms, headers, database, log_path):
        self.headers=headers

        self.dic={}#记录视频数据

        self.start_term=terms["start"]
        self.end_term=terms["end"]
        self.log_path=log_path
        
        #一个全局休息参数
        self.all_sleep_arg=1

        #初始化数据库
        try:
            self.conn = mysql.connector.connect(user=database['dbuser'], password=database['dbpsw'], database=database['dbname'])
            self.cursor = self.conn.cursor()
            self.cursor.execute('SET NAMES utf8mb4;')
            #创建表
#            self.cursor.execute( \
#                 'CREATE TABLE IF NOT EXISTS videos (term INT, bvid VARCHAR(31) PRIMARY KEY, '+\
#                 'vtitle VARCHAR(255), vuser VARCHAR(255), vpudate DATETIME, vdesc VARCHAR(1023), '+\
#                 'vtag VARCHAR(255), vview INT, vlike INT, vcoin INT, vstar INT, vshare INT, vbullet INT, vcomment INT);')
#            self.cursor.execute( \
#                 'CREATE TABLE IF NOT EXISTS comments (bvid VARCHAR(31), rpid VARCHAR(255) PRIMARY KEY, root VARCHAR(1023), parent VARCHAR(255), '+\
#                 'cpudate DATETIME, cuser VARCHAR(255), culevel INT, caddress VARCHAR(255), ccontent VARCHAR(1023), cpicture INT, '+\
#                 'clike INT, crcount INT, cuplike BOOLEAN, cupreply BOOLEAN, FOREIGN KEY(bvid) REFERENCES videos(bvid));')
#            self.cursor.execute( \
#                 'CREATE TABLE IF NOT EXISTS bullets (bvid VARCHAR(31), dmid VARCHAR(255), bapdate DATE, bpudate DATETIME, '+\
#                 'bcolor VARCHAR(255), bcontent VARCHAR(255), PRIMARY KEY(dmid, bapdate), FOREIGN KEY(bvid) REFERENCES videos(bvid));')
            
        except Exception as e:
            with open(self.log_path, 'w') as f:
                f.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+':'+'数据库表连接失败\n')
                f.write(str(e)+'\n')
            self.cursor.close()
            self.conn.close()

    #log日志
    def log(self, message):
        with open(self.log_path, 'a+', encoding='utf-8') as f:
            f.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+':'+ message + '\n')
        
    #创建数据库表
    def createTable(self, term):
        try:
            #创建表
            self.cursor.execute( \
                 'CREATE TABLE IF NOT EXISTS videos_%s (term INT, bvid VARCHAR(31) PRIMARY KEY, '+\
                 'vtitle VARCHAR(255), vuser VARCHAR(255), vpudate DATETIME, vdesc VARCHAR(1023), '+\
                 'vtag VARCHAR(255), vview INT, vlike INT, vcoin INT, vstar INT, vshare INT, vbullet INT, vcomment INT);',[term])
            self.cursor.execute( \
                 'CREATE TABLE IF NOT EXISTS comments_%s (bvid VARCHAR(31), rpid VARCHAR(255) PRIMARY KEY, root VARCHAR(1023), parent VARCHAR(255), '+\
                 'cpudate DATETIME, cuser VARCHAR(255), culevel INT, caddress VARCHAR(255), ccontent VARCHAR(1023), cpicture INT, '+\
                 'clike INT, crcount INT, cuplike BOOLEAN, cupreply BOOLEAN, FOREIGN KEY(bvid) REFERENCES videos_%s(bvid));',[term,term])
            self.cursor.execute( \
                 'CREATE TABLE IF NOT EXISTS bullets_%s (bvid VARCHAR(31), dmid VARCHAR(255), bapdate DATE, bpudate DATETIME, '+\
                 'bcolor VARCHAR(255), bcontent VARCHAR(255), PRIMARY KEY(dmid, bapdate), FOREIGN KEY(bvid) REFERENCES videos_%s(bvid));',[term,term])
        
        except Exception as e:
            self.log("数据库表创建失败"+str(term))
            self.log(str(e))
            
    #视频数据提交到数据库
    def submitVideoData(self, dic):
        try:
            self.cursor.execute('INSERT INTO videos_%s VALUES'+\
                '(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);', \
                [dic['term'], dic['term'], dic['bvid'], dic['title'], dic['user'], \
                dic['pudate'], dic['desc'], dic['tag'], dic['view'], \
                dic['like'], dic['coin'], dic['star'], dic['share'], \
                dic['bullet'], dic['comment']])
            self.conn.commit()
            return 1
        except Exception as e:
            self.log('视频数据提交失败'+ dic['bvid'])
            self.log(str(e))
            return 0

    #评论数据提交到数据库
    def submitCommentData(self, dic):
        try:
            self.cursor.execute('INSERT INTO comments_%s VALUES'+\
                '(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);', \
                [dic['term'], dic['bvid'], dic['rpid'], dic['root'], dic['parent'], \
                dic['pudate'], dic['user'], dic['ulevel'], dic['address'], \
                dic['content'], dic['picture'], dic['like'], dic['rcount'], \
                dic['uplike'], dic['upreply']])
            self.conn.commit()
            return 1
        
        except Exception as e:
            self.log('评论数据提交失败'+ dic['bvid']+ ":"+ dic['rpid']+':'+dic['root'])
            self.log(str(e))
            return 0

    #弹幕数据提交到数据库
    def submitBulletData(self, dic):

        try:
            self.cursor.execute('INSERT INTO bullets_%s VALUES'+\
                '(%s, %s, %s, %s, %s, %s);', \
                [dic['term'], dic['bvid'], dic['dmid'], dic['apdate'], dic['pudate'], \
                dic['color'], dic['content']])
            self.conn.commit()
            return 1
        except Exception as e:
            self.log('弹幕数据提交失败'+ dic['bvid']+ ":"+ dic['dmid'])
            self.log(str(e))
            return 0

    #关闭数据库
    def closeSql(self):
        self.cursor.close()
        self.conn.close()
	
    #更新coockie
    def getNewCookies(self):
        browser=webdriver.Chrome()
        #隐式等待
        browser.implicitly_wait(10)
        #进一个视频
        browser.get('https://www.bilibili.com/video/BV1QL411R7QQ')
        #登录
        try:
            browser.find_element(By.CLASS_NAME,'header-login-entry').click()
            browser.find_elements(By.CLASS_NAME,'login-sns-item')[2].click()
            browser.switch_to.frame('ptlogin_iframe')
            browser.find_element(By.ID,'nick_3279511320').click()
#            WebDriverWait(browser,10,0.5).until(EC.presence_of_element_located((By.CLASS_NAME, "bili-avatar")))
            time.sleep(10)
        except Exception as e:
            browser.close()
            self.log(str(e))
            return
        #获取cookie
        cookiestr=""
        for item in browser.get_cookies():
            cookiestr+=item['name']+'='+item['value']+'; '
        cookiestr=cookiestr[:-2]
        browser.close()
        return cookiestr

    #获取视频各个id
    def getId(self, term):
        id_li=[]

        #构造每期必看的url
        hot_url="https://api.bilibili.com/x/web-interface/popular/series/one"
        hot_params={"number":term}
        try:
            r=rq.get(url=hot_url, headers=self.headers,params=hot_params)
            data=json.loads(r.text)["data"]["list"]

            
            for video in data:
                id_dic={}
                id_dic["term"]=term
                id_dic["bvid"]=video["bvid"]
                id_dic["aid"]=video["aid"]
                id_dic["cid"]=video["cid"]

                id_li.append(id_dic)
            
        except Exception as e:
            self.log("网站获取失败："+str(term))
            self.log(str(e))
        return id_li

    #获取视频基础数据
    def getBasicData(self, ids):
        vdata_url="https://api.bilibili.com/x/web-interface/view"
        v_params={"aid":ids['aid']}
        vdic={}
        try:
            r=rq.get(url=vdata_url,headers=self.headers,params=v_params)
            data=json.loads(r.text)["data"]

            #标题、简介、发布时间信息
            vdic["term"]=ids['term']
            vdic["bvid"]=ids['bvid']
            vdic["title"]=data["title"][:200]
            vdic["desc"]=data["desc"][:1000]
            vdic["pudate"]=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(data["pubdate"]))
            
            #发布者
            users=""
            if 'staff' in data:
                for user in data['staff']:
                    users+=user["name"]+"/"
            else:
                users=data["owner"]["name"]
            vdic["user"]=users
            #数据
            vdic["view"]=data["stat"]["view"]
            vdic["like"]=data["stat"]["like"]
            vdic["coin"]=data["stat"]["coin"]
            vdic["star"]=data["stat"]["favorite"]
            vdic["share"]=data["stat"]["share"]
            vdic["bullet"]=data["stat"]["danmaku"]
            vdic["comment"]=data["stat"]["reply"]

        except:
            self.log("信息获取失败："+ ids['bvid'])
        return vdic
    
    #获取视频标签
    def getTags(self, ids):
        tag_url="https://api.bilibili.com/x/web-interface/view/detail/tag"
        t_params={"aid":ids['aid'], "cid":ids['cid']}
        try:
            r=rq.get(url=tag_url, headers=self.headers, params=t_params)
            data=json.loads(r.text)
            #获取标签
            tags=""
            for tag in data["data"]:
                tags+=tag["tag_name"]+"/"

        except:
            self.log("获取标签失败："+ids['bvid'])
        return tags

    #爬取视频数据
    def getVideo(self, ids):
        vdic=self.getBasicData(ids)
        vdic["tag"]=self.getTags(ids)
        self.submitVideoData(vdic)
        self.log("第"+str(ids['term'])+"期:"+str(ids["bvid"])+"\t视频数据爬取成功")
        return vdic#这个主要后面爬弹幕要用


    #爬取评论
    def getComment(self, ids):
        #休眠参数
        comment_sleep_arg=1
        comment_url='https://api.bilibili.com/x/v2/reply'
        #页数
        pn=1
        while True:
            self.log("正在爬"+ids['bvid']+'的第'+str(pn)+'页')
            #重试参数
#            retry=0
            #访问参数
            params={'pn':pn, 'type':1, 'oid':ids['aid'],'sort':2}
            try:
                retry=0
                while True:
                    r=rq.get(url=comment_url, headers=self.headers, params=params)
                    if r.status_code==200 or retry > 5:
                        break
                    #换cookie
                    if retry > 2:
                        new_cookie=self.getNewCookies()
                        if new_cookie:
                            self.headers["cookie"]=new_cookie
                    retry+=1
                    time.sleep(3)
            except Exception as e:
                self.log("评论获取失败："+ ids['bvid']+'主')
                self.log(e)
                return
            #请求拦截 重试
#            if r.status_code!=200 and retry<5:
#                time.sleep(5)
#                retry+=1
#                continue
            try:
                data=json.loads(r.text)["data"]["replies"]
            except:
                self.log(r.status_code)
                self.log(params)
                
            #没数据了就结束
            if data is None:
                break
            
            if len(data) == 0:
                break
            
            for comment in data:
                #存储评论属性
                rdic={}

                #基本属性
                rdic["term"]=ids["term"]
                rdic["bvid"]=ids["bvid"]
                rdic["rpid"]=str(comment["rpid"])
                rdic["root"]=str(comment["root"])
                
                #防止后续嵌套爬取到重复数据(实际调试过程中这一段代码没用到过)
                if rdic['root'] != '0':
                    self.log('阿巴阿巴')
                    continue
                
                rdic["parent"]=str(comment["parent"])
                rdic['pudate']=time.strftime(r'%Y-%m-%d %H:%M:%S', time.localtime(comment["ctime"]))
                rdic['user']=comment['member']['uname']
                rdic['ulevel']=comment['member']['level_info']['current_level']
                if 'location' in comment['reply_control']:
                    rdic["address"]=comment['reply_control']["location"]
                else:
                    rdic["address"]="未知"
                #内容
                rdic['content']=comment['content']['message'][:1000]
                rdic['picture']=0
                if 'pictures' in  comment['content']:
                    rdic['pictures']= len(comment['content']['pictures'])

                #反馈信息
                rdic['like']=comment['like']
                rdic['rcount']=comment['rcount']
                rdic['uplike']=int(comment['up_action']['like'])
                rdic['upreply']=int(comment['up_action']['reply'])

                #提交数据
                if comment_sleep_arg%1000==0:
                    self.log("休息一小会儿"+str(comment_sleep_arg))
                    time.sleep(5)
                    self.log("继续工作！")
                if comment_sleep_arg>10000:
                    self.log('休息两分钟zzzz')
                    time.sleep(120)
                    comment_sleep_arg=0
                    self.log("继续工作！")
                self.submitCommentData(rdic)
                comment_sleep_arg+=1

                
                #嵌套获取相关回复
                if rdic['rcount'] > 0:
                    #调试 记录本轮爬到的rpid
                    rpid_li=[]
                    rcomment_url="https://api.bilibili.com/x/v2/reply/reply"
                    rpn=1
                    while True:
                        #重试参数
#                        rretry=0
                        rparams={'oid':ids['aid'],'pn':rpn, 'ps':10, 'root':rdic["rpid"], 'type':1}
                        #获取页面信息
                        try:
#                            r=rq.get(url=rcomment_url, headers=self.headers, params=rparams)
                            rretry=0
                            while True:
                                r=rq.get(url=rcomment_url, headers=self.headers, params=rparams)
                                if r.status_code==200 or retry > 5:
                                    break
                                #换cookie
                                if retry > 4:
                                    new_cookie=self.getNewCookies()
                                    if new_cookie:
                                        self.headers["cookie"]=new_cookie
                                rretry+=1
                                time.sleep(3)
                                self.log("休息3秒重试")
                        except Exception as e:
                            self.log("评论获取失败："+ ids['bvid'])
                            self.log(str(e))
                            return
                        #请求被拦截 休息下再发送
#                        if r.status_code!=200 and rretry<3:
#                            time.sleep(5)
#                            rretry+=1
#                            continue
                        try:
                            data=json.loads(r.text)["data"]["replies"]
                        except:
                            self.log(rparams)
                        #没数据了就结束
                        if data is None:
                            break
                        
                        if len(data) == 0:
                            break

                        for comment in data:

                            #封装评论属性
                            rrdic={}
                            #基本属性
                            rrdic["term"]=ids["term"]
                            rrdic["bvid"]=ids["bvid"]
                            rrdic["rpid"]=str(comment["rpid"])
                            
                            #调试用
                            if rrdic["rpid"] in rpid_li:
                                continue
                            
                            rrdic["root"]=str(comment["root"])
                            rrdic["parent"]=str(comment["parent"])
                            rrdic['pudate']=time.strftime(r'%Y-%m-%d %H:%M:%S', time.localtime(comment["ctime"]))
                            rrdic['user']=comment['member']['uname']
                            rrdic['ulevel']=comment['member']['level_info']['current_level']
                            if 'location' in comment['reply_control']:
                                rrdic["address"]=comment['reply_control']["location"]
                            else:
                                rrdic["address"]="未知"
                            #内容
                            #限定1000字
                            rrdic['content']=comment['content']['message'][:1000]
                            rrdic['picture']=0
                            if 'pictures' in  comment['content']:
                                rrdic['pictures']= len(comment['content']['pictures'])

                            #反馈信息
                            rrdic['like']=comment['like']
                            rrdic['rcount']=comment['rcount']
                            rrdic['uplike']=int(comment['up_action']['like'])
                            rrdic['upreply']=int(comment['up_action']['reply'])

                            #提交数据
                            if comment_sleep_arg%1000==0:
                                self.log("休息一小会儿"+str(comment_sleep_arg))
                                time.sleep(5)
                                self.log("继续工作！")
                            if comment_sleep_arg>10000:
                                comment_sleep_arg=0
                                self.log('休息两分钟zzzz')
                                time.sleep(120)
                                self.log("继续工作！")
                            if self.submitCommentData(rrdic):
                                rpid_li.append(rrdic['rpid'])
                                comment_sleep_arg+1
                        rpn+=1
                        time.sleep(0.5)
                        
            #每爬一页休息1秒
            pn+=1
            time.sleep(0.5)
        self.log(str(ids["bvid"])+"\t评论数据爬取成功\n")


    #获取历史弹幕日期
    def getBulletDate(self, st_date, ids):
        #从视频发布日期开始
        start_date=datetime.strptime(st_date, r"%Y-%m-%d %H:%M:%S")
        #2021.10之前的弹幕无法访问
        if start_date<datetime(2021,10,1):
            year=2021
            month=10
        else:
            year=start_date.year
            month=start_date.month

        #获取当前日期
        now_date=datetime.now()

        #构造弹幕日期列表
        date_li=[]
        while True:
            #自增，用于构造日期爬弹幕日期
            if month > 12:
                year+=1
                month=1

            #构造日期
            target_date=datetime(year, month, 1)
            #超出日期，滚犊子
            if target_date > now_date:
                break
            
            #构造url
            target_datestr=target_date.strftime(r"%Y-%m")
            date_url="https://api.bilibili.com/x/v2/dm/history/index"
            date_params={"type":1, "oid":ids["cid"], "month":target_datestr}
            
            date_data=None
            try:
                retry=0
                while True:
                    #超出次数
                    if retry>5:
                        self.log('访问日期超出次数')
                        break
                    #多次被请求拦截换cookie
                    if retry > 4 :
                        new_cookie=self.getNewCookies()
                        if new_cookie:
                            self.headers["cookie"]=new_cookie
                    #访问
                    date_r=rq.get(date_url, headers=self.headers, params=date_params)
                    #成功
                    if date_r.status_code==200:
                        #账号没登录换cookie
                        if 'data' not in json.loads(date_r.text):
                            new_cookie=self.getNewCookies()
                            if new_cookie:
                                self.headers["cookie"]=new_cookie
                            retry+=1
                            continue;
                        #登录成功
                        break
                    retry+=1
                    time.sleep(5)
            except Exception as e:
                self.log("当前日期弹幕获取失败："+target_datestr+ids['bvid'])
                self.log(str(e))
                
                
            date_data=json.loads(date_r.text)['data']

            #这个月没有弹幕就到下个月
            if date_data is None:
                month+=1
                continue

            #有弹幕就把弹幕添加到日期列表
            date_li+=date_data
            month+=1
        return date_li
    
    #爬取弹幕数据
    def getBullet(self, start_date, ids):
        date_li=self.getBulletDate(start_date, ids)

        #控制休眠的参数
        sleep_arg=0

        for  date in date_li:
            self.log('正在爬'+ids['bvid']+'弹幕:'+date)
            
            #每1000次休眠10秒
            sleep_arg+=1
            if sleep_arg%10==0:
                self.log('休息一会儿')
                time.sleep(5)
                self.log('继续工作！')
            if sleep_arg%50==0:
                sleep_arg=0
                self.log('再休息1分钟zzzzz')
                time.sleep(120)
                self.log('继续工作！')
            if self.all_sleep_arg%100==0:
                self.log('好好休息10分钟')
                time.sleep(600)
                self.log('继续干活')
            #构造url
            bu_url="https://api.bilibili.com/x/v2/dm/web/history/seg.so"
            bu_params={"type":1, "oid":ids["cid"], "date":date}
            
            #声明一个target
            target=None
                #请求拦截 重试
            retry=0
            while True:
                try:
                    #超出次数
                    if retry>5:
                        self.log('超出次数')
                        break
                    #多次被请求拦截换cookie
                    if retry > 2 :
                        new_cookie=self.getNewCookies()
                        if new_cookie:
                            self.headers["cookie"]=new_cookie
                    #访问
                    bu_r=rq.get(bu_url, headers=self.headers, params=bu_params)
                    self.all_sleep_arg+=1
                    #成功
                    if bu_r.status_code==200:
                        #赋值
                        target = bilidm_pb2.DmSegMobileReply()
                        target.ParseFromString(bu_r.content)
                        #登录成功
                        break
                    retry+=1
                    time.sleep(5)
                except Exception as e:
                    self.log("获取当前日期弹幕失败\n"+ids["bvid"]+date)
                    self.log(str(e))
                    #看看咋失败的
                    with open(r'./error/'+ids['bvid']+date+'.txt','wb') as f:
                        f.write(bu_r.content)
                    retry+=1
                    self.log("歇NNN分钟再来~")
                    time.sleep(retry*600)
                    #获取新cookie
                    new_cookie=self.getNewCookies()
                    if new_cookie:
                        self.headers["cookie"]=new_cookie
                    self.log("获取新cookie重试中~")
                    continue

            #获取当前日期的每条弹幕
            if target is not None:
                for bullet in target.elems:
                    #准备好数据表
                    bdic={}
                    bdic["term"]=ids["term"]
                    bdic["bvid"]=ids["bvid"]
                    bdic["dmid"]=str(bullet.id)
                    bdic["color"]=str(bullet.color)
                    bdic["apdate"]=date
                    bdic["pudate"]=time.strftime(r'%Y-%m-%d %H:%M:%S', time.localtime(bullet.ctime))
    
                    bdic["content"]=bullet.content
    
                    #上传数据
                    self.submitBulletData(bdic)

        self.log(str(ids["bvid"])+"\t弹幕数据爬取成功\n")


        #运行
    def Run(self):

        #休眠控制参数
        sleep_arg=0
        
        
        for term in range(self.start_term, self.end_term+1):
            #创建表
            self.createTable(term)
            #每爬完5组休眠1分钟
            sleep_arg+=1
            if sleep_arg>=5:
                sleep_arg=0
                self.log("已爬完5组视频,休眠1分钟--------\n")
                time.sleep(60)
            #获取ID
            id_li=self.getId(term)
            #跳过已爬取
            sql="SELECT bvid FROM videos_%s"%(term)
            self.cursor.execute(sql)
            result = self.cursor.fetchall()
            for ids in id_li:
                if (bytearray(bytes(ids['bvid'],encoding='utf-8')),) in result:
                    self.log(ids['bvid']+"跳过")
                    continue
                vdic=self.getVideo(ids)
                self.getComment(ids)
                self.getBullet(vdic['pudate'], ids)
                
        self.closeSql()







