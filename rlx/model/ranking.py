from dataclasses import dataclass, field
from catscore.lib.time import get_today_date
from pyspark.sql import SparkSession
from pyspark import SparkContext, SQLContext
from pyspark.sql import SparkSession, DataFrame
from catscore.db.mysql import MySQLConf, MySQLSpark
from catslab.word.mecab import CatsMeCab
from catscore.spark.io import SparkIO

@dataclass(frozen=True)
class RankedItem:
    rank_keyword: str
    hotel_name: str
    rank_num: str
    hotel_img: str
    hotel_detail_link: str
    hotel_area: str
    hotel_copy: str
    hotel_introduction: str
    hotel_review_score: str
    update_date: str = get_today_date()
    
class RankedItemTable:
    _table_name = "relux_ranked_item"
    
    @classmethod
    def from_file_as_df(cls, spark:SparkSession, input_path:str):
        """
        """
        input_path = f"{input_path}/*.csv"
        print(f"RankedItemTable: input path is {input_path}")
        df = spark.read.csv(path=input_path,header=True,multiLine=True,ignoreLeadingWhiteSpace=True,escape="\"").drop("_c0")
        return df

    @classmethod
    def to_db(cls, spark:SparkSession, df:DataFrame, mysql_conf:MySQLConf):
        df.write.jdbc(mysql_conf.connection_uri("jdbc"), table=cls._table_name, mode='overwrite')
        
    @classmethod
    def cooking(cls, spark:SparkSession, df:DataFrame, mecab_dict: str):
        def _cooking(d):
            s = f'{d["hotel_introduction"]}'
            mecab = CatsMeCab(mecab_dict)
            parsed_s = mecab.parse(str(s))
            noun_s = list(filter(lambda r: (r.word_type == "名詞") or (r.word_type == "動詞") or (r.word_type == "形容詞"), parsed_s))
            noun_one_str = list(map(lambda r: f"{r.word}", noun_s))
            nouns = ",".join(noun_one_str)
            result = CookedRankedItem(
                rank_keyword = d["rank_keyword"],
                hotel_name = d["hotel_name"],
                rank_num = d["rank_num"],
                hotel_img = d["hotel_img"],
                hotel_detail_link = d["hotel_detail_link"],
                hotel_area = d["hotel_area"],
                hotel_copy = d["hotel_copy"],
                hotel_introduction = d["hotel_introduction"],
                hotel_review_score = d["hotel_review_score"],
                nouns = nouns)
            return result
        return df.rdd.map(lambda d: _cooking(d)).toDF()

@dataclass(frozen=True)
class CookedRankedItem:
    rank_keyword: str
    hotel_name: str
    rank_num: str
    hotel_img: str
    hotel_detail_link: str
    hotel_area: str
    hotel_copy: str
    hotel_introduction: str
    hotel_review_score: str
    nouns: str
    
class CookedRankedItemTable:
    _table_name = "cooked_relux_ranked_item"

    @classmethod
    def to_db(cls, spark:SparkSession, df:DataFrame, mysql_conf:MySQLConf):
        df.write.jdbc(mysql_conf.connection_uri("jdbc"), table=cls._table_name, mode='overwrite')
    
    @classmethod
    def from_db_as_sdf(cls, spark:SparkSession, mysql_conf:MySQLConf):
        return MySQLSpark.read(spark, mysql_conf, cls._table_name)
    
    @classmethod
    def to_json(cls, spark:SparkSession, mysql_conf:MySQLConf, output_path: str):
        df = cls.from_db_as_sdf(spark=spark, mysql_conf=mysql_conf).drop("nouns").drop("hotel_introduction")
        SparkIO.write_to_json(df, f"{output_path}/{cls._table_name}.json")