import argparse
import logging
from rlx.site.japan_ranking import JapanRankingSite
from rlx.site.detail import DetailSite
from catscore.http.request import CatsRequest
from catscore.lib.time import get_today_date

def main():
    parser = argparse.ArgumentParser(description="relux robot")

    # args params
    parser.add_argument('-f', '--function', nargs='*', choices=['japan_ranking', 'detail'], help="functions")
    parser.add_argument('-d', '--dump_path', help="result dump path", required=True)
    args = parser.parse_args()
    print(args)
    
    request: CatsRequest = CatsRequest()
    for f in args.function:
        if f == "japan_ranking":
            output_path = f"{args.dump_path}/relux/japan_ranking_{get_today_date()}.csv"
            result = JapanRankingSite(request).all_category_ranking(pandas=True)
            result.to_csv(output_path)
    request.close()
if __name__ == "__main__":
    main()