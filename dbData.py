from transformers import BertTokenizer
from tqdm import tqdm
from datetime import datetime
import psycopg2
import psycopg2.pool
from concurrent.futures import ThreadPoolExecutor, as_completed
import re

import threading

simple_conn_pool = psycopg2.pool.SimpleConnectionPool(minconn=1, maxconn=6, database='bc', user='postgres',
                                                      password='90-=op[]', host='172.20.42.67', port='5432')

class CLoadDbDataset():
    def __init__(self):
        # self.labels = ['体育', '娱乐', '家居', '房产', '教育', '时尚', '时政', '游戏', '科技', '财经']
        self.matchList1 = ["bitcoin", "eth", "fil", "比特币", "link", "iost", "ada", "algo", "atom", "avax",
                           "bal", "bch", "bnb", "comp", "dash", "doge", "eos", "etc", "iota", "knc", "ltc",
                           "neo", "omg", "ont", "qtum", "sushi", "theta", "trx", "vet", "xlm", "xmr", "xrp", "xtz",
                           "zec", "zil", "zrx"]
        self.labels = [-2, -1, 0, 1, 2]
        self.tokenizer = BertTokenizer.from_pretrained('bert-base-chinese')
        self.input_ids = []
        self.token_type_ids = []
        self.attention_mask = []
        self.label_id = []
        self.load_data()

    def toTradePair(self, keyword):
        return {
            "bitcoin": "BTC/USDT",
            "比特币": "BTC/USDT",
            "eth": "ETH/USDT",
            "fil": "FIL/USDT",
            "doge": "DOGE/USDT",
            "link": "LINK/USDT",
            "iost": "IOST/USDT",
            "ada": "ADA/USDT",
            "algo": "ALGO/USDT",
            "atom": "ATOM/USDT",
            "avax": "AVAX/USDT",
            "bal": "BAL/USDT",
            "bch": "BCH/USDT",
            "bnb": "BNB/USDT",
            "comp": "COMP/USDT",
            "dash": "DASH/USDT",
            "eos": "EOS/USDT",
            "etc": "ETC/USDT",
            "iota": "IOTA/USDT",
            "knc": "KNC/USDT",
            "ltc": "LTC/USDT",
            "neo": "NEO/USDT",
            "omg": "OMG/USDT",
            "ont": "ONT/USDT",
            "qtum": "QTUM/USDT",
            "sushi": "SUSHI/USDT",
            "theta": "THETA/USDT",
            "trx": "TRX/USDT",
            "vet": "VET/USDT",
            "trx": "TRX/USDT",
            "xlm": "XLM/USDT",
            "xmr": "XMR/USDT",
            "xrp": "XRP/USDT",
            "xtz": "XTZ/USDT",
            "zec": "ZEC/USDT",
            "zil": "ZIL/USDT",
            "zrx": "ZRX/USDT"
        }.get(keyword.lower(), None)

    def getTradePair(self, text):
        words_flat1 = "|".join(r'\b{}\b'.format(word) for word in self.matchList1)
        rx = re.compile(words_flat1, re.IGNORECASE)
        tp = ''
        if pattern := rx.search(text):
            tp = self.toTradePair(pattern.group(0))
        return tp

    def mark(self, chunk):
        try:
            conn = simple_conn_pool.getconn()
            cur = conn.cursor()
            conn.autocommit = False
            insert_sql = "INSERT INTO bitcointrain1 (id, inputtime, title, description, up_count, down_count, hits, url, tp, rate, cat) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO nothing;"
            # insert_sql = 'INSERT INTO bitcointrain1 (id ) VALUES (%s)'
            # insert_sql = 'INSERT INTO bitcointrain1 (id, inputtime, title, description, up_count, down_count, hits, url, tp, rate, cat ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
            # insert_sql = '''INSERT INTO bitcointrain1 (id, inputtime, title, description, up_count, down_count, hits, url, tp, rate, cat ) VALUES (1, '2020-11-12 11:11:11', '3', 'd', 3, 5, 6, 'url', 'tp', 1, 1 )'''
            for row in chunk:
                # print(row)
                tp = self.getTradePair(row[2])
                if tp is None:
                    tp = self.getTradePair(row[3])
                    if tp is None:
                        tp = ""
                rate = 0
                upanddown = int(row[4]) + int(row[5])
                if upanddown > 0:
                    upRate = float(row[4]) / upanddown
                    if upRate > 0.9:
                        rate = 2
                    elif upRate > 0.6:
                        rate = 1
                    elif upRate > 0.45:
                        rate = 0
                    elif upRate > 0.15:
                        rate = -1
                    else:
                        rate = -2
                # print('rate={}'.format(rate))
                cur.execute(insert_sql, (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], tp, rate, 1))
                # cur.execute(insert_sql, (1, '2020-11-12 11:11:11', '3', 'd', 3, 5, 6, row[7], 'tp', 1, 1 ))
                # cur.execute(insert_sql)
                # cur.execute(insert_sql, (row,))
                # result = cur.fetchone()
                # print(result)
            conn.commit()
            count = cur.rowcount
            print(count, "Record inserted successfully into mobile table")

            simple_conn_pool.putconn(conn)
        except (Exception, psycopg2.Error) as error:
            print("Failed to insert record into mobile table", error)
        return

    def load_data(self):
        print('loading data from:')
        batchSize = 1000

        # 从数据库连接池获取连接
        conn = simple_conn_pool.getconn()
        # 自动提交事务设为false
        conn.autocommit = False
        # 创建游标,这里传入name参数，会返回一个服务端游标否则返回的是客户端游标
        cursor = conn.cursor('cursorname')

        cursor.execute("select id, inputtime, title, description, up_count, down_count, hits, url from bitcoinscrap1")
        count = 0
        all_task = []
        startTime = datetime.now()
        with ThreadPoolExecutor(max_workers=5) as t:
            while True:
                count = count + 1
                data = cursor.fetchmany(batchSize)
                if not data:
                    break
                all_task.append(t.submit(self.mark, data))
        print(as_completed(all_task))
        simple_conn_pool.putconn(conn)
                # for row in data:
                #     print("Id = ", row[0], " description = ", row[1], "\n")
        print('total read time:', (datetime.now() - startTime).seconds)

