import sys
import random

import re
import time
import requests
from openpyxl import workbook
from jsonpath import jsonpath
from dateutil.parser import parse
from config import ck_pool
from config import logger

'''
功能：传入微博详情链接，获取该微博的所有评论数据
1、微博评论需要登录后才能获取，这里采用cookie实现登录
2、从起始页（start_url）开始获取评论数据，从该接口中可以获取到下一页URL所需参数 max_id
3、每个api接口只能请求两次（有时效性），请求两次过后需重新从起始页（start_url）获取该评论接口链接

解释：
一级评论：最上层直接就能看到的评论
二级评论：评论的评论（跟评）
'''


class WeiBo_Spider:
    def __init__(self):
        # 一级评论api接口
        self.start_url = 'https://m.weibo.cn/comments/hotflow?&max_id_type=0'
        self.next_url = 'https://m.weibo.cn/comments/hotflow?&max_id={}&max_id_type={}'

        # 二级评论api接口
        self.second_level_url = 'https://m.weibo.cn/comments/hotFlowChild'

        self.headers = {
            "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 13_2_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.3 Mobile/15E148 Safari/604.1",
        }

        self.GENDER = {'f': '女', 'm': '男'}

        self.wb = workbook.Workbook()  # 创建excel对象
        self.ws = self.wb.active  # 激活
        self.ws.append(['用户名', '性别', '内容', '时间', '评论级别'])

    def get_response(self, detail_url):
        '''
        传入微博详情链接，获取该条微博的id（截取URL最后一段数字）
        :param detail_url: 微博详情链接
        :return: 微博id
        '''
        try:
            self.headers['cookie'] = random.choice(ck_pool)
            response = requests.get(detail_url, headers=self.headers, timeout=(15, 15))
            if response.status_code == 200:
                response.encoding = response.apparent_encoding
                return response
            raise Exception(f'请求异常，状态码{response.status_code}')
        except Exception as e:
            logger.error(e)
            # 延时8秒重发请求
            time.sleep(8)
            return self.get_response(detail_url)

    def get_weibo_id(self, response):
        '''
        获取微博文章id，用于请求评论接口
        :param response:
        :return:
        '''
        expression = '"id":\s"\d+"'
        try:
            weibo_id = re.findall(expression, response.text)[0].split('"')[-2]
            logger.info(f'weibo_id acquired success >>>{weibo_id}')
            return weibo_id
        except:
            logger.error('微博id未找到，请检查该链接是否正确')
            sys.exit(1)

    def get_api_data(self, url, weibo_id):
        '''
        获取一级评论
        :param url:评论接口链接
        :param weibo_id:微博id
        :return:
        '''
        params = {
            'id': weibo_id,
            'mid': weibo_id,
        }
        self.headers['Cookie'] = random.choice(ck_pool)
        logger.debug(f'Cookie:{self.headers["Cookie"][:50]}')
        try:
            response = requests.get(url, params=params, headers=self.headers, allow_redirects=False)
            logger.info(f'一级接口请求信息：{response.status_code} {response.url}')  # 查看状态码&对应的url
            if response.status_code == 200:
                try:
                    data = response.json()
                    usernames = jsonpath(data, '$..user.screen_name')  # 用户名
                    genders = jsonpath(data, '$..user.gender')  # 性别 f female 女性  m male 男性
                    comments = jsonpath(data, '$..text')  # 评论内容
                    created_ats = jsonpath(data, '$..created_at')  # 评论时间 ->"Sun Aug 15 01:27:28 +0800 2021"
                    total_numbers = jsonpath(data, '$..total_number')  # 评论时间 ->"Sun Aug 15 01:27:28 +0800 2021"
                    rootids = jsonpath(data, '$..rootid')  # 评论的评论（二级评论） api接口id
                except:
                    logger.info('json解析异常，评论抓取完毕~')
                    return

                for user, gender, comment, created_at, total_number, cid in zip(usernames, genders, comments,
                                                                                created_ats, total_numbers, rootids):
                    comment = re.sub('<.*?>', '', comment)  # 评论 --> 正则处理html 字符
                    comment_time = str(parse(created_at)).split('+')[0]  # 评论时间

                    logger.info(f'>>>{user} {self.GENDER[gender]} {comment} {comment_time}')
                    self.ws.append([user, self.GENDER[gender], comment, comment_time, '1'])

                    # 获取二级评论
                    self.get_second_level_comments(self.second_level_url, cid)

                    # 推荐启用 过滤 无二级评论信息的不请求二级接口，可提升采集速度（存在问题，极少部分数据可能采集不到，比如有@某人，被@的人有回复的）
                    # if total_number != 0:
                    #     self.get_second_level_comments(self.second_level_url, cid)
                    # else:
                    #     logger.info('无二级评论内容')

                # todo 翻页>>> 获取下一页URL所需参数 max_id
                try:
                    max_id = jsonpath(data, '$..max_id')[0]
                    max_id_type = jsonpath(data, '$..max_id_type')[0]
                    if str(max_id) == '0':
                        raise Exception('data capture is complete')
                except TypeError as e:
                    logger.info('no page，data capture is complete', e)
                    return
                except Exception as e:
                    logger.error(e)
                    return

                time.sleep(random.randint(5, 10))
                self.get_api_data(self.next_url.format(max_id, max_id_type), weibo_id)  # 递归调用 实现翻页获取下一页评论数据
            else:
                time.sleep(random.randint(5, 10))
                return self.get_api_data(url, weibo_id)
        except:
            time.sleep(random.randint(5, 15))
            self.get_api_data(url, weibo_id)

    def get_second_level_comments(self, second_level_url, cid, max_id=0, max_id_type=0):
        '''
        获取某条评论下的跟评（二级评论）
        :param second_level_url: 二级评论api接口
        :param cid: 二级评论 cid（从一级评论的api接口中可获取）
        :param max_id: 翻页id（下一页）
        :return:
        '''
        params = {
            'cid': cid,
            'max_id': max_id,
            'max_id_type': max_id_type,
        }
        self.headers['Cookie'] = random.choice(ck_pool)
        logger.debug(self.headers['cookie'][:20])
        response = requests.get(second_level_url, params=params, headers=self.headers, allow_redirects=False)  # ,
        logger.info(f"二级接口请求信息：{response.status_code} {response.url}")
        if response.status_code == 200:
            try:
                # 微博会预留评论接口，但该接口可能会没有数据，会导致解析出错，这里处理这个异常
                data = response.json()
                second_level_users = jsonpath(data, '$..data..screen_name')  # 用户
                genders = jsonpath(data, '$..user.gender')  # 性别
                second_level_comments = jsonpath(data, '$..data..text')  # 内容
                created_ats = jsonpath(data, '$..created_at')  # 评论时间
            except:
                logger.info('secondary comment crawling completed~')
                return

            # TODO 遍历打印所有二级评论数据
            for user, gender, comment, created_at in zip(second_level_users, genders, second_level_comments,
                                                         created_ats):
                comment = re.sub('<.*?>', '', comment)
                comment_time = str(parse(created_at)).split('+')[0]  # 评论时间
                print('\t', user, self.GENDER[gender], comment, comment_time)
                self.ws.append([user, self.GENDER[gender], comment, comment_time, '2'])

            # todo 二级评论翻页>>> 获取下一页的二级评论数据（如果有）
            try:
                next_max_id = jsonpath(data, '$..max_id')[0]
                max_id_type = jsonpath(data, '$..max_id_type')[0]
                if str(next_max_id) == '0':
                    print('secondary comment crawling completed~')
                    return
            except TypeError as e:
                print('该用户的二级评论爬取完毕~', e)
                return
            time.sleep(random.randint(7, 9))  # todo 二级评论抓取延迟  cookie 数量足够多时 这个时间可以减少  7-9 是单个cookie的请求延迟时间
            self.get_second_level_comments(second_level_url, cid, next_max_id, max_id_type)  # 递归调用 实现翻页获取下一页二级评论数据
        else:
            time.sleep(10)
            self.get_second_level_comments(second_level_url, cid, max_id,max_id_type)

    def start_spider(self):
        # TODO 这里更换要请求的微博链接
        detail_url = 'https://m.weibo.cn/status/Ktsc870Sn?type=comment&jumpfrom=weibocom#_rnd1629171600634'
        response = self.get_response(detail_url)
        weibo_id = self.get_weibo_id(response)
        try:
            self.get_api_data(self.start_url, weibo_id)
        finally:
            self.wb.save(f'{weibo_id}.xlsx')  # 保存到excel


if __name__ == '__main__':
    while True:
        weibo = WeiBo_Spider()
        weibo.start_spider()
