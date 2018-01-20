#!/usr/bin/python
# -*- coding: UTF-8 -*-

from bs4 import BeautifulSoup
import urllib.request
import pymysql
import threading

#设置最大线程锁
thread_lock = threading.BoundedSemaphore(value=5)



def crawl(url):
    headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'}
    resquest = urllib.request.Request(url,headers=headers)#创建一个resquest对象
    page = urllib.request.urlopen(resquest,timeout=20)#发出请求
    contents = page.read()#获取响应
    soup = BeautifulSoup(contents,'lxml')#解析相应
    posts = soup.find_all('img')
    for post in posts:
        link = post.get('src') #图片链接
        movie = post.get('alt') #图片名
        #       操作数据库
        sql_insert(movie,link)
        # 下载图片
        down_pic(link, movie)
        # 操作文件
        write_text(movie)

def write_text(movie):
    # 操作文件
    f = open(r'C:\Users\six\Desktop\movie.txt', 'a', encoding='utf-8')  # 打开文件
    f.write(movie + '\n')
    f.close()


def down_pic(link,movie):
    # 下载图片
    urllib.request.urlretrieve(link, r'C:\Users\six\Desktop\movie_post\%s.jpg' % movie)


def sql_insert(movie,link):
    #       创建数据库链接
    database = pymysql.connect(host='localhost', user='root', passwd='', db='yiibaidb', port=3307, charset='utf8')
    #        创建一个游标
    cur = database.cursor()
    sql_insert = 'insert into `doubanmovie` (name,link) VALUES ("'"%s"'","'"%s"'")' % (movie, link)
    print(sql_insert)
    try:
        cur.execute(sql_insert)  # 执行sql
        database.commit()  # 提交事物
    except:
        database.rollback()  # 回滚
    cur.close()  # 关闭游标
    database.close()  # 释放数据库资源

def sql_distict():
    #       操作数据库
    #       创建数据库链接
    database = pymysql.connect(host='localhost', user='root', passwd='', db='yiibaidb', port=3307, charset='utf8')
    #        创建一个游标
    cur = database.cursor()
    sql_distict = 'delete from doubanmovie where id not in (select minid from (select min(id) as minid from doubanmovie group by name) b);'
    print(sql_distict)
    try:
        cur.execute(sql_distict)  # 执行sql
        database.commit()  # 提交事物
    except:
        database.rollback()  # 回滚
    cur.close()  # 关闭游标
    database.close()  # 释放数据库资源




if __name__ == '__main__':
    n=0
    try:
        while n<500:
            url = 'https://movie.douban.com/top250?start=%d&filter=' %n
            n = n+25
            crawl(url)
    finally:
         sql_distict()

