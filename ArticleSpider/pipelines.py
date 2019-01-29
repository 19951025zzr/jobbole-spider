# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import codecs
import json
from scrapy.pipelines.images import ImagesPipeline
from scrapy.exporters import JsonItemExporter
import MySQLdb
import MySQLdb.cursors
from twisted.enterprise import adbapi


# 用提取到的字段初始化item
class ArticlespiderPipeline(object):
    def process_item(self, item, spider):
        return item


class JsonWithEncodingPipeline(object):
    #  自定义json文件导出
    def __init__(self):
        #  以utf8打开文件，初始化file
        self.file = codecs.open('article.json', 'w', encoding='utf-8')

    def process_item(self, item, spider):
        # 将字典转化成字符串，关闭unicode,防止乱码
        lines = json.dumps(dict(item), ensure_ascii=False) + '\n'
        self.file.write(lines)
        return item

    def spider_closed(self, spider):
        self.file.close()


class MysqlPipeline(object):
    #  采用同步的方式写入数据库，数据量大时可能拥塞
    def __init__(self):
        self.conn = MySQLdb.connect('127.0.0.1', 'test', '1234', 'article_spider', charset='utf8', use_unicode='True')
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):
        insert_sql = """
            insert jobbole_article(title, url, create_date, fav_nums)
            VALUES (%s, %s, %s, %s)
        """
        self.cursor.execute(insert_sql, (item['title'], item['url'], item['create_date'], item['fav_nums']))
        self.conn.commit()


class MysqlTwistedPipeline(object):
    # 接收类方法返回的实例化对象的参数
    def __init__(self, dbpool):
        self.dbpool = dbpool

    @classmethod
    def from_settings(cls, settings):
        dbparms = dict(
                    host=settings['MYSQL_HOST'],
                    db=settings['MYSQL_DBNAME'],
                    user=settings['MYSQL_USER'],
                    passwd=settings['MYSQL_PASSWORD'],
                    charset='utf8',
                    cursorclass=MySQLdb.cursors.DictCursor,
                    use_unicode=True
        )
        dbpool = adbapi.ConnectionPool('MySQLdb', **dbparms)
        #  返回一个实例化对象
        return cls(dbpool)

    def process_item(self, item, spider):
        #  使用twisted将数据库插入变成异步执行
        query = self.dbpool.runInteraction(self.do_insert, item)
        query.addErrback(self.handle_error)  # 处理异常

    def handle_error(self, failure):
        #  处理异步插入的异常
        print(failure)

    def do_insert(self, cursor, item):
        insert_sql = """
                    insert jobbole_article(title, url, create_date, fav_nums, front_image_url)
                    VALUES (%s, %s, %s, %s, %s)
                """
        cursor.execute(insert_sql, (item['title'], item['url'], item['create_date'], item['fav_nums'], item['front_image_url'][0]))


class JsonExporterPipeline(object):
    #  调用scrapy 提供的json exporter 导出json 文件
    def __init__(self):
        self.file = open('articleexport.json', 'wb')
        self.exporter = JsonItemExporter(self.file, encoding='utf-8', ensure_ascii=False)
        self.exporter.start_exporting()

    def close_spider(self, spider):
        self.exporter.finish_exporting()
        self.file.close()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item


# image pipeline 要求传递front_image_url 使用列表的形式
# 爬虫管道不能将下载到本地的图片关联为工程相对路径，通过自定义图片管道利用重载函数得到
class ArticleImagePipeline(ImagesPipeline):
    def item_completed(self, results, item, info):
        if 'front_image_url' in item:
            # 列表中元素是元组，元组中有bool和dict，OK 接收元组中的bool , value 接收元组中的字典
            for ok, value in results:
                image_file_path = value['path']
            item['front_image_path'] = image_file_path
        return item

