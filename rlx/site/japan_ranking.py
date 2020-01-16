import datetime
import sys
import requests
import re
import json
import time
from catscore.http.request import CatsRequest
from catscore.lib.logger import CatsLogging as logging
from catscore.lib.time import get_today_date, get_current_time
from bs4 import BeautifulSoup
import itertools
import pandas as pd
from dataclasses import asdict
from rlx.model.ranking import RankedItem
from rlx.site.detail import DetailSite

class JapanRankingSite:
    base_url = "https://rlx.jp"

    def __init__(self, request: CatsRequest):
        self.request = request

    @property
    def top_page(self):
        """[https://rlx.jp/r/ranking/allにアクセス]

        Returns:
            [type] -- [description]
        """
        url = f"{self.base_url}/r/ranking/all"
        logging.info(f"request to {url}")
        response = self.request.get(url=url, response_content_type="html")
        return response

    def category_ranking(self, keyword, url):
        """[指定されたurlのランク付きアイテムを取得]

        Arguments:
            folder {[type]} -- []

        Returns:
            [type] -- [description]
        """
        def _parse(div):
            thum = div.find("div", {"class": "thum"})
            hotel_img = thum.find("img").get("src")
            info = div.find("div", {"class": "info"})
            rank_num = info.find("img").get("alt").replace("best","")
            hotel_name = info.find("h2", {"class": "name"}).text.replace("\n","")
            hotel_detail_link = self.base_url + info.find("h2", {"class": "name"}).find("a").get("href")
            hotel_area = info.find("p", {"class": "area"}).text.replace("\n","").replace(" ","")
            hotel_copy = info.find("p", {"class": "copy"}).text.replace("\n","").replace(" ","")
            detail = DetailSite(self.request, hotel_detail_link)
            hotel_introduction = detail.introduction
            hotel_review_score = detail.review_score
            return RankedItem(
                rank_keyword=keyword,
                hotel_name=hotel_name,
                rank_num=rank_num,
                hotel_img=hotel_img,
                hotel_detail_link=hotel_detail_link,
                hotel_area=hotel_area,
                hotel_copy=hotel_copy,
                hotel_introduction=hotel_introduction,
                hotel_review_score=hotel_review_score)

        logging.info(f"category_ranking: request to {url}")
        soup = self.request.get(url=url, response_content_type="html").content
        rank_list = soup.find("div", {"class": "entry-list"}).findAll("div", {"class": "entry"})
        result = list(map(lambda r: _parse(r), rank_list))
        return result

    @property
    def category_ranking_menu(self):
        """[ランキング一覧(カテゴリ別)を取得]

        Returns:
            [type] -- [description]
        """
        li = self.top_page.content.find("ul", {"class", "area_link"}).findAll("li")
        result = list(map(lambda l: (l.text.replace("\n",""), f'{self.base_url}{l.find("a").get("href")}'), li))[1:]
        logging.info(result)
        return result

    def all_category_ranking(self, pandas=False):
        """[summary]
        """
        menu = self.category_ranking_menu
        r = list(map(lambda m: self.category_ranking(m[0], m[1]), menu))
        result = list(itertools.chain.from_iterable(r))
        if pandas:
            return pd.DataFrame([asdict(x) for x in result])
        else:
            return result