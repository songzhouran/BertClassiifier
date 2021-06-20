import datetime as dt
import time
from urllib import parse

import crawlertool as tool
# from Selenium4R import Chrome
import re
import csv
import requests
import time
import pandas as pd
import datetime
from tqdm import tqdm

csvPath = '/Users/a/project/BertClassifier/csv/bitcoin86.csv'
# csvPath = '/home/songzhouran/py/bert/BertClassifier/csv/bitcoin86.csv'
csvMarkPath = '/Users/a/project/BertClassifier/csv/bitcoin86mark1.csv'
# csvMarkPath = '/home/songzhouran/py/bert/BertClassifier/csv/bitcoin86mark.csv'
matchList = ['coin', 'bitcoin', 'cryptocurrency', 'virtual', 'currency', 'crypto', 'doge', 'eth', 'usdt', 'busd', 'binance', '', '', '', '', '', '', '', '', '', '', '', '', '']
twitterId = 'whale_alert'
class SpiderTwitterAccountPost(tool.abc.SingleSpider):
    """
    Twitter账号推文爬虫
    """

    def __init__(self, driver):
        self.driver = driver

        # 爬虫实例的变量
        self.user_name = None

    @staticmethod
    def get_twitter_user_name(page_url: str) -> str:
        """提取Twitter的账号用户名称
        主要用于从Twitter任意账号页的Url中提取账号用户名称
        :param page_url: Twitter任意账号页的Url
        :return: Twitter账号用户名称
        """
        if pattern := re.search(r"(?<=twitter.com/)[^/]+", page_url):
            return pattern.group()

    def running(self, user_name: str, since_date, until_date):
        """执行Twitter账号推文爬虫
        :param user_name: Facebook账号主页名称（可以通过get_facebook_user_name获取）
        :param since_date: 抓取时间范围的右侧边界（最早日期）
        :param until_date: 抓取时间范围的左侧边界（最晚日期）
        :return: 推文信息列表
        """
        words_flat = "|".join(r'\b{}\b'.format(word) for word in matchList)
        rx = re.compile(words_flat, re.IGNORECASE)
        # isMatch1 = rx.search('Tesla & Bitcoin')
        self.user_name = user_name

        item_list = []

        # 生成请求的Url
        query_sentence = []
        query_sentence.append("from:%s" % user_name)  # 搜索目标用户发布的推文
        query_sentence.append("-filter:retweets")  # 过滤到所有转推的推文
        if since_date is not None:
            query_sentence.append("since:%s" % str(since_date))  # 设置开始时间
            query_sentence.append("until:%s" % str(until_date))  # 设置结束时间
        query = " ".join(query_sentence)  # 计算q(query)参数的值
        params = {
            "q": query,
            "f": "live"
        }
        actual_url = "https://twitter.com/search?" + parse.urlencode(params)
        self.console("实际请求Url:" + actual_url)

        # 打开目标Url
        self.driver.get(actual_url)
        time.sleep(3)

        # 判断是否该账号在指定时间范围内没有发文
        label_test = self.driver.find_element_by_css_selector("main > div > div > div > div:nth-child(1) > div > div:nth-child(2) > div > div")
        if "你输入的词没有找到任何结果" in label_test.text:
            return item_list

        # 定位标题外层标签
        label_outer = self.driver.find_element_by_css_selector(
            "main > div > div > div > div:nth-child(1) > div > div:nth-child(2) > div > div > section > div > div")
        self.driver.execute_script("arguments[0].id = 'outer';", label_outer)  # 设置标题外层标签的ID


        # 循环遍历外层标签
        tweet_id_set = set()
        for _ in range(1000):
            last_label_tweet = None
            for label_tweet in label_outer.find_elements_by_xpath('//*[@id="outer"]/div'):  # 定位到推文标签

                item = {}

                # 读取推文ID
                if label := label_tweet.find_element_by_css_selector(
                            "article > div > div > div > div:nth-child(2) > div:nth-child(2) > div:nth-child(1) > div > div > div:nth-child(1) > a"):
                    if pattern := re.search("[0-9]+$", label.get_attribute("href")):
                         item["tweet_id"] = pattern.group()
                if "tweet_id" not in item:
                    self.log("账号名称:" + user_name + "|未找到推文ID标签(第" + str(len(item_list)) + "条推文)")
                    continue

                # 判断推文是否已被抓取(若未被抓取则解析推文)
                if item["tweet_id"] in tweet_id_set:
                    continue

                tweet_id_set.add(item["tweet_id"])
                last_label_tweet = label_tweet

                # 解析推文内容
                if label := label_tweet.find_element_by_css_selector(
                        "article > div > div > div > div:nth-child(2) > div:nth-child(2) > div:nth-child(2) > div:nth-child(1)"):
                    if re.match('^回复 \\n@', label.text):
                        label = label_tweet.find_element_by_css_selector(
                        "article > div > div > div > div:nth-child(2) > div:nth-child(2) > div:nth-child(2) > div:nth-child(2)")
                    isMatch = rx.search(label.text)
                    if isMatch:
                        item["text"] = label.text
                    else:
                        continue

                # 解析推文发布时间
                if label := label_tweet.find_element_by_css_selector(
                        "article > div > div > div > div:nth-child(2) > div:nth-child(2) > div:nth-child(1) > div > div > div:nth-child(1) > a > time"):
                    item["time"] = label.get_attribute("datetime").replace("T", " ").replace(".000Z", "")
                item_list.append(item)

            # 向下滚动到最下面的一条推文
            if last_label_tweet is not None:
                self.driver.execute_script("arguments[0].scrollIntoView();", last_label_tweet)  # 滑动到推文标签
                self.console("执行一次向下翻页...")
                time.sleep(3)
            else:
                break
        f = open('/Users/a/project/twitterscraper/csv/' + twitterId + '.csv', 'a', encoding='utf8', newline='')
        with f:
            fnames = ['tweet_id', 'text', 'time']
            writer = csv.DictWriter(f, fieldnames=fnames)
            writer.writerows(item_list)
        return item_list

class SpiderBitcoin86():
    """
    Twitter账号推文爬虫
    """

    def __init__(self):
        # 爬虫实例的变量
        self.user_name = None
        self.matchList1 = ["bitcoin", "eth", "fil", "比特币", "link", "iost", "ada", "algo", "atom", "avax",
                      "bal", "bch", "bnb", "comp", "dash", "doge", "eos", "etc", "iota", "knc", "ltc",
                      "neo", "omg", "ont", "qtum", "sushi", "theta", "trx", "vet", "xlm", "xmr", "xrp", "xtz",
                      "zec", "zil", "zrx"]

    def running(self, fromDate=None):
        fnames = ['id', 'inputtime', 'title', 'description', 'up_count', 'down_count', 'hits', 'url']

        start = list(range(0, 20000, 1))

        # 设置爬虫请求头，不然会被反爬报错418，常用爬虫请求头可参见：http://www.guokuidata.com/crawler-requests-headers/
        headers = {
            'User-Agent': 'MQQBrowser/26 Mozilla/5.0 (Linux; U; Android 2.3.7; zh-cn; MB200 Build/GRJ22; CyanogenMod-7) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1'
        }

        lst = []

        try:
            with open(csvPath, 'a', encoding='utf8', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fnames, extrasaction='ignore')
                isDone = False
                for i in start:
                    if isDone:
                        break
                    url = 'https://www.bitcoin86.com/api_v2/index.php/api/news/lives_list?p={}'.format(
                        str(i))
                    # requests解析网页
                    res = requests.get(url, headers=headers)
                    # 判断相应状态，200为正常继续往下运行，否则返回响应值
                    if res.status_code == 200:
                        print('正在爬取第{}条数据，共计15条数据'.format(str(i)))
                        # 将json解析为python数据类型
                        js = res.json()
                        # 将每个网页中获取的‘data’对应的包含20个电影详细信息的列表列表合并到一个列表中
                        if not js['data']:
                            print("page {} is empty, break!".format(str(i)))
                            break
                        for i, item in enumerate(js['data']):
                            item['inputtime'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(item['inputtime'])))
                            if fromDate is not None and item['inputtime'] <= fromDate:
                                js['data'] = js['data'][0:i]
                                isDone = True
                                break
                            item['description'] = item['description'].replace(',', ' ').replace('\"', '')
                            item['title'] = item['title'].replace(',', ' ').replace('\"', '')
                        lst += js['data']
                        if len(lst) > 6000:
                            writer.writerows(lst)
                            lst=[]


                        time.sleep(0.5)
                    else:
                        print(res.status_code)

        finally:
            writer.writerows(lst)
            # 将该列表转化为dataframe数组
            # df = pd.DataFrame(lst)
            # # 将数组信息写入excel文件
            # df.to_excel('近十年豆瓣评分前500大陆电影.xlsx')
            print('爬取完成！')
        return

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

    def getRate(self, tp, startts, endts):
        level2 = 0.075
        level1 = 0.01
        headers = {
            'User-Agent': 'MQQBrowser/26 Mozilla/5.0 (Linux; U; Android 2.3.7; zh-cn; MB200 Build/GRJ22; CyanogenMod-7) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1'
        }
        url = r'https://api.binance.com/api/v3/klines?symbol={}&interval=1m&startTime={}&endTime={}'.format(tp, startts, endts)
        res = requests.get(url, headers=headers)
        if res.status_code == 200:
            klines = res.json()
            if klines:
                currentPrice = float(klines[0][1])
                hPrice = -1.0
                lPrice = float('inf')
                for item in klines:
                    if float(item[2]) > hPrice:
                        hPrice = float(item[2])
                    if float(item[3]) < lPrice:
                        lPrice = float(item[3])
                rise = (hPrice - currentPrice) / currentPrice
                drop = (currentPrice - lPrice) / currentPrice
                print('rise={},drop={}'.format(rise, drop))
                if drop > level2 and rise > level2:
                    if drop < rise:
                        return 2
                    else:
                        return -2
                elif rise > level2:
                    return 2
                elif drop > level2:
                    return -2
                elif drop > level1 and rise > level1:
                    if drop < rise:
                        return 1
                    else:
                        return -1
                elif rise > level1:
                    return 1
                elif drop > level1:
                    return -1
                else:
                    return 0
            else:
                print('empty klines={}, tp={}, startts={}, endts={}'.format(klines, tp, startts, endts))
        else:
            return -100

    def getTradePair(self, text):
        words_flat1 = "|".join(r'\b{}\b'.format(word) for word in self.matchList1)
        rx = re.compile(words_flat1, re.IGNORECASE)
        tp = ''
        if pattern := rx.search(text):
            tp = self.toTradePair(pattern.group(0))
        return tp

    def process(self):
        filename = "/Users/a/fsdownload/bitcoin86mark1.csv"
        outfile="/Users/a/fsdownload/bitcoin86markproc1.csv"
        i = 0
        with open(outfile, 'a', encoding='utf-8') as predW:
            with open(filename, 'r', encoding='utf-8') as wf:
                lines = wf.readlines()
            for line in tqdm(lines):
                lineList = line.split(',')
                lineSize = len(lineList)
                newLine = ""
                if lineSize > 12:
                    textList = lineList[3:-8]
                    text = ";".join(textList)
                    newLine = f'{",".join(lineList[0:3])},{text},{",".join(lineList[-9:])}'
                    print(str(i)+" newline:" + newLine)
                else:
                    _, _, _, text, _, _, _, _, _, _, _, _ = lineList
                    newLine = line
                predW.write(newLine)
                i = i + 1
        return

    def mark(self):
        cSize = 5000

        reader = pd.read_csv(csvPath, chunksize=cSize, header=None)
        row = -1
        iChunk = -1
        colNames = ['id', 'time', 'title', 'description', 'up_count', 'down_count', 'hits', 'url', 'risk', 'tp', 'rate', 'cat']
        pdNew = pd.DataFrame()
        for chunk in reader:
            iChunk = iChunk + 1
            # print(chunk)
            chunk[8] = 0 #是否风险
            chunk[9] = ''
            chunk[10] = -101
            chunk[11] = 1 #1 政策 2 大v
            # for i in range(cSize):
            for idx, item in chunk.iterrows():
                row = row + 1
                tp = self.getTradePair(chunk.at[row, 3])
                if tp is not None:
                    startTs = int(datetime.datetime.strptime(item[1], "%Y-%m-%d %H:%M:%S").timestamp())
                    endTs = 1200 + startTs
                    startTs = startTs * 1000
                    endTs = endTs * 1000
                    rate = self.getRate(tp.replace("/", ""), startTs, endTs)
                    print('rate={}'.format(rate))
                    item[9] = tp
                    item[10] = rate
                    pdrow = pd.DataFrame(item).T
                    pdNew = pdNew.append(pdrow, ignore_index=True)
                    if(pdNew.shape[0] >= 500):
                        print("=======================witecsv")
                        pdNew.to_csv(csvMarkPath, mode='a', index=None, header=False)
                        pdNew = pdNew.drop(index=pdNew.index)
        pdNew.to_csv(csvMarkPath, mode='a', index=None, header=False)

# ------------------- 单元测试 -------------------
if __name__ == "__main__":
    print("start")
    # SpiderBitcoin86().mark()
    # SpiderBitcoin86().running("2021-06-08 16:17:04")
    SpiderBitcoin86().process()
    # driver = Chrome(cache_path=r"/Users/a/project/twitterscraper/chrome")
    # SpiderBitcoin86().running()
    # print(SpiderTwitterAccountPost(driver).running(
    #     user_name=SpiderTwitterAccountPost.get_twitter_user_name("https://twitter.com/" + twitterId),
    #     # since_date=dt.date(2020, 11, 27),
    #     # until_date=dt.date(2021, 5, 27)
    #     # since_date=dt.date(2020, 8, 27),
    #     # until_date=dt.date(2020, 11, 26)
    #     since_date=dt.date(2020, 5, 27),
    #     until_date=dt.date(2020, 8, 26)
    # ))