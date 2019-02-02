# -*- coding: utf-8 -*-
# 此程序用来抓取 的数据
import hashlib
import os

import requests
import time
import random
import re
from multiprocessing.dummy import Pool
import csv
import json
import sys
from fake_useragent import UserAgent, FakeUserAgentError
from save_data import database

class Spider(object):
	def __init__(self):
		try:
			self.ua = UserAgent(use_cache_server=False).random
		except FakeUserAgentError:
			pass
		# self.date = '2000-10-01'
		# self.limit_count = 50000 # 限制条数
		self.db = database()
	
	def get_headers(self):
		user_agents = ['Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20130406 Firefox/23.0',
		               'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:18.0) Gecko/20100101 Firefox/18.0',
		               'IBM WebExplorer /v0.94', 'Galaxy/1.0 [en] (Mac OS X 10.5.6; U; en)',
		               'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0)',
		               'Opera/9.80 (Windows NT 6.0) Presto/2.12.388 Version/12.14',
		               'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0; TheWorld)',
		               'Opera/9.52 (Windows NT 5.0; U; en)',
		               'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.2pre) Gecko/2008071405 GranParadiso/3.0.2pre',
		               'Mozilla/5.0 (Windows; U; Windows NT 5.2; en-US) AppleWebKit/534.3 (KHTML, like Gecko) Chrome/6.0.458.0 Safari/534.3',
		               'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/532.0 (KHTML, like Gecko) Chrome/4.0.211.4 Safari/532.0',
		               'Opera/9.80 (Windows NT 5.1; U; ru) Presto/2.7.39 Version/11.00']
		user_agent = random.choice(user_agents)
		headers = {'Host': 'v.youku.com', 'Connection': 'keep-alive',
		           'User-Agent': user_agent,
		           'Referer': 'https://v.youku.com/v_show/id_XMzU3MzUyNjYyMA==.html?spm=a2h0k.11417342.soresults.dtitle&s=50efbfbdefbfbd5fefbf',
		           'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
		           'Accept-Encoding': 'gzip, deflate, br',
		           'Accept-Language': 'zh-CN,zh;q=0.8'
		           }
		return headers

	def p_time(self, stmp):  # 将时间戳转化为时间
		stmp = float(str(stmp)[:10])
		timeArray = time.localtime(stmp)
		otherStyleTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
		return otherStyleTime
	
	def replace(self, x):
		# 将其余标签剔除
		removeExtraTag = re.compile('<.*?>', re.S)
		x = re.sub(removeExtraTag, "", x)
		x = re.sub('/', ';', x)
		return x.strip()
	
	def GetProxies(self):
		# 代理服务器
		proxyHost = "http-dyn.abuyun.com"
		proxyPort = "9020"
		# 代理隧道验证信息
		proxyUser = "HK847SP62Z59N54D"
		proxyPass = "C0604DD40C0DD358"
		proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
			"host": proxyHost,
			"port": proxyPort,
			"user": proxyUser,
			"pass": proxyPass,
		}
		proxies = {
			"http": proxyMeta,
			"https": proxyMeta,
		}
		return proxies

	def get_comments_page(self, s):
		film_url, videoId, product_number, plat_number, page = s
		print page
		url = "https://p.comments.youku.com/ycp/comment/pc/commentList"
		querystring = {"app": "100-DDwODVkv",
		               "objectId": videoId,
		               "objectType": "1",
		               "listType": "0",
		               "currentPage": str(page),
		               "pageSize": "30",
		               "sign": "bd7879082c5df4cc91e30ea817b75d79",
		               "time": "1533124166"}
		retry = 5
		while 1:
			try:
				headers = self.get_headers()
				headers['host'] = 'p.comments.youku.com'
				text = \
					requests.get(url, headers=headers, timeout=10,
					             params=querystring).json()[
						'data']['comment']
				last_modify_date = self.p_time(time.time())
				results = []
				for item in text:
					nick_name = item['user']['userName']
					cmt_time = self.p_time(item['createTime'])
					cmt_date = cmt_time.split()[0]
					# if cmt_date < self.date:
					# 	continue
					comments = item['content']
					like_cnt = str(item['upCount'])
					cmt_reply_cnt = str(item['replyCount'])
					long_comment = '0'
					src_url = film_url
					tmp = [product_number, plat_number, nick_name, cmt_date, cmt_time, comments, like_cnt,
					       cmt_reply_cnt, long_comment, last_modify_date, src_url]
					print '|'.join(tmp)
					results.append([x.encode('gbk', 'ignore') for x in tmp])
				return results
			except Exception as e:
				retry -= 1
				if retry == 0:
					print e
					return None
				else:
					continue

	def get_total_page(self, videoId):  # 获取评论总页数
		url = "https://p.comments.youku.com/ycp/comment/pc/commentList"
		querystring = {"app": "100-DDwODVkv",
		               "objectId": videoId,
		               "objectType": "1",
		               "listType": "0",
		               "currentPage": "1",
		               "pageSize": "30",
		               "sign": "6df70e8b76612db60a3077ea61f615d5",
		               "time": "1536257606"}
		retry = 5
		while 1:
			try:
				headers = self.get_headers()
				headers['host'] = 'p.comments.youku.com'
				text = requests.get(url, headers=headers, timeout=10,
				                    params=querystring).json()[
					'data']['totalPage']
				totalpage = int(text)
				# limit_page = self.limit_count / 30
				# if totalpage > limit_page:
				# 	totalpage = limit_page
				return totalpage
			except:
				retry -= 1
				if retry == 0:
					return None
				else:
					continue
	
	def get_show_id(self, product_url):
		retry = 5
		while 1:
			try:
				headers = self.get_headers()
				headers['host'] = 'list.youku.com'
				text = requests.get(product_url, headers=headers, timeout=10).text
				p0 = re.compile(u'showid:"(\d+?)"')
				showId = re.findall(p0, text)[0]
				return showId
			except Exception as e:
				retry -= 1
				if retry == 0:
					print e
					return None
				else:
					continue
	
	def get_all_urls(self, product_url):  # 获取电视剧每一集的链接
		showid = self.get_show_id(product_url)
		if showid is None:
			return None
		url = "https://list.youku.com/show/module"
		querystring = {"id": showid, "tab": "showInfo", "cname": "电视剧",
		               "callback": "jQuery1112012550660370838052_1540621623677", "_": "1540621623678"}
		retry = 1
		while 1:
			try:
				headers = {'host': "list.youku.com",
				           'connection': "keep-alive",
				           'accept': "text/javascript, application/javascript, application/ecmascript, application/x-ecmascript, */*; q=0.01",
				           'x-requested-with': "XMLHttpRequest",
				           'user-agent': "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36",
				           'referer': "https://list.youku.com/show/id_zc4fe3ca0606b11e29498.html?spm=a2h0j.11185381.bpmodule-playpage-righttitle.5~H2~A",
				           'accept-encoding': "gzip, deflate, br",
				           'accept-language': "zh-CN,zh;q=0.9"}
				text = requests.get(url, headers=headers, params=querystring, timeout=10).text
				p0 = re.compile('(\{.*?\})', re.S)
				t1 = re.findall(p0, text)[0].replace('\\', '')
				p1 = re.compile('<li><dl><dt><a class="c555" href="(.*?)"', re.S)
				items = re.findall(p1, t1)
				if len(items) == 0:
					p1 = re.compile('<a class="c555" href="(.*?)"', re.S)
					items = re.findall(p1, t1)
				results = []
				for item in items:
					item_url = 'https:' + item
					results.append(item_url)
				return results
			except Exception as e:
				retry -= 1
				if retry == 0:
					print e
					return None
				else:
					continue
	
	def get_movie_id(self, film_url):  # 获取电影id
		retry = 5
		while 1:
			try:
				headers = self.get_headers()
				headers['host'] = 'v.youku.com'
				text = requests.get(film_url, headers=headers, timeout=10).text
				p0 = re.compile(u'videoId: \'(.*?)\'')
				videoId = re.findall(p0, text)[0]
				return videoId
			except Exception as e:
				retry -= 1
				if retry == 0:
					print e
					return None
				else:
					continue
	
	def get_all_videoIds(self, urls):  # 获取每一集的videoId
		pool = Pool(5)
		items = pool.map(self.get_movie_id, urls)
		pool.close()
		pool.join()
		videoIds = filter(lambda x: x is not None, items)
		return videoIds
	
	def save_sql(self, table_name,items):  # 保存到sql
		all = len(items)
		print all
		results = []
		for i in items:
			try:
				t = [x.decode('gbk', 'ignore') for x in i]
				dict_item = {'product_number': t[0],
				             'plat_number': t[1],
				             'nick_name': t[2],
				             'cmt_date': t[3],
				             'cmt_time': t[4],
				             'comments': t[5],
				             'like_cnt': t[6],
				             'cmt_reply_cnt': t[7],
				             'long_comment': t[8],
				             'last_modify_date': t[9],
				             'src_url': t[10]}
				results.append(dict_item)
			except:
				continue
		for item in results:
			try:
				self.db.add(table_name, item)
			except:
				continue
	
	def get_comments_all(self, film_url, product_number, plat_number):  # 获取所有评论
		film_urls = self.get_all_urls(film_url)
		if film_urls is None:
			print u'%s 评论抓取出错' % product_number
			return None
		print u'共有 %d 集' % len(film_urls)
		print u'开始获取videoId'
		videoIds = self.get_all_videoIds(film_urls)
		print u'videoId获取完毕'
		for videoId in videoIds:
			print videoId
			if videoId is None:
				print u'%s 评论抓取出错' % product_number
				return None
			else:
				pagenums = self.get_total_page(videoId)
				if pagenums is None:
					print u'%s 评论抓取出错' % product_number
					return None
				else:
					print u'%s 共有 %d 页' % (videoId, pagenums)
					s = []
					for page in range(1, pagenums + 1):
						s.append([film_url, videoId, product_number, plat_number, page])
					pool = Pool(5)
					items = pool.map(self.get_comments_page, s)
					pool.close()
					pool.join()
					mm = []
					for item in items:
						if item is not None:
							mm.extend(item)

					with open('data_youku.csv', 'a') as f:
						writer = csv.writer(f, lineterminator='\n')
						writer.writerows(mm)

					# print u'%s 开始录入数据库' % product_number
					# self.save_sql('T_COMMENTS_PUB_MOVIE', mm)  # 手动修改需要录入的库的名称
					# print u'%s 录入数据库完毕' % product_number

	def normal_url(self, film_url):
		if 'list' in film_url:
			return film_url
		retry = 5
		while 1:
			try:
				text = requests.get(film_url, timeout=10).text
				p0 = re.compile(u'<a href="(.*?)" target="_blank" class="" title=".*?">')
				url0 = re.findall(p0, text)[0]
				film_url = 'https:' + url0
				return film_url
			except:
				retry -= 1
				if retry == 0:
					return None
				else:
					continue


if __name__ == "__main__":
	spider = Spider()
	s = []
	with open('new_data.csv') as f:
		tmp = csv.reader(f)
		for i in tmp:
			if 'http' in i[2]:
				s.append([i[2], i[0], 'P03'])
	for j in s:
		print j[1],j[0]
		spider.get_comments_all(j[0], j[1], j[2])
	spider.db.db.close()

