import argparse
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark import SparkContext
from catscore.db.mysql import MySQLConf
from catslab.word.mecab import CatsMeCab
from rlx.model.ranking import RankedItemTable, CookedRankedItemTable

def main():
    # spark
    conf = SparkConf()
    conf.setAppName('relux')
    sc: SparkContext = SparkContext(conf=conf)
    spark:SparkSession = SparkSession(sc)

    # init
    parser = argparse.ArgumentParser(description='pyspark app args')
    parser.add_argument('-op', '--output_path', type=str, required=True, help='input folder path')
    parser.add_argument('-db', '--db_conf', type=str, required=True, help='input db conf path')
    parser.add_argument('-mc', '--mecab_conf', type=str, required=True, help='input db conf path')
    args = parser.parse_args()
    print(f"args: {args}")
    ## db
    mysql_conf = MySQLConf.from_json(args.db_conf)
    print(f"mysql_conf {mysql_conf}")
    ## mecab
    mecab = CatsMeCab.from_json(args.mecab_conf)

    # convert
    CookedRankedItemTable.to_json(spark=spark, mysql_conf=mysql_conf, output_path=args.output_path)

    
if __name__ == '__main__':
    main()