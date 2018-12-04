import time
import random
import pymysql
import requests
from lxml import etree


def page_request(p_url):
    proxies = [
        '113.205.44.77:3128', '61.135.155.82:443', '211.152.33.24:48749',
        '36.110.14.66:80', '112.16.4.99:81', '114.223.165.185:8118'
    ]
    proxy = {'http': random.choice(proxies)}
    user_agents = [
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.60 Safari/537.17',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 '
        'Safari/537.36'
        'Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/40.0.2214.93 Safari/537.36',
    ]
    header = {'User-Agent': random.choice(user_agents)}
    try:
        res = requests.get(p_url, headers=header, proxies=proxy)
        res.encoding = 'utf-8'
        html = etree.HTML(res.text)
        return html
    except Exception as e:
        print('error is:', e)


def get_page_num():
    p = p_html.xpath('//div[@class="list-head clear"]/h2/span/text()')
    total = int(p[0])
    if total <= 30:
        page = 1
        return page
    else:
        page = total / 30
        if page > 100:
            page = 100
            return page
        else:
            return page + 1


def parse_page():
    page = get_page_num()
    conn = pymysql.connect(host='localhost', user='root', passwd='2558', port=3306, db='room', charset='utf8mb4')
    print('connection successful!')
    values = ', '.join(['%s'] * 9)
    cur = conn.cursor()
    for i in range(1, page + 1):
        print('current page is:', i)
        addresses = []
        types = []
        sizes = []
        heights = []
        data_time = []
        averages = []

        real_url = url + str(i)
        region = real_url.split('/')[4]
        html = page_request(real_url)
        adds = html.xpath('//span[@class="region"]/text()')
        zones = html.xpath('//span[@class="zone"]/span/text()')
        meters = html.xpath('//span[@class="meters"]/text()')
        sources = html.xpath('//div[@class="con"]/a/text()')
        height = html.xpath('//div[@class="con"]//text()')
        prices = html.xpath('//div[@class="price"]/span/text()')
        for m in meters:
            sizes.append(m.split('平')[0])
        for add, zone, meter, price in zip(adds, zones, sizes, prices):
            addresses.append(add.strip())
            types.append(zone.strip())
            average = round(float(price)/float(meter), 2)
            averages.append(average)
        for x in range(int(len(height) / 5)):
            a = 2 + x * 5
            b = 4 + x * 5
            h = height[a]
            d = height[b]
            heights.append(h)
            data_time.append(d)
        for n in range(len(sizes)):
            sql = 'insert into shanghai(region, address, height, time, source, type, size, price, average) ' \
                  'values (%s)' % values
            try:
                cur.execute(sql, (region, addresses[n], heights[n], data_time[n], sources[n], types[n], sizes[n],
                                  prices[n], averages[n]))
                conn.commit()
            except Exception as e:
                print('error is:', e)
                conn.rollback()
        t = random.uniform(2, 4)
        time.sleep(t)
    cur.close()
    conn.close()


if __name__ == '__main__':
    regs = ['pudong', 'minhang', 'baoshan', 'xuhui', 'putuo', 'yangpu', 'changning', 'songjiang', 'jiading',
                 'huangpu', 'jingan', 'zhabei', 'hongkou', 'qingpu', 'fengxian', 'jinshan', 'chongming']
    urls = []
    for reg in regs:
        url = 'https://sh.lianjia.com/zufang/{}/pg'.format(reg)
        urls.append(url)
    for url in urls:
        p_html = page_request(url)
        parse_page()
