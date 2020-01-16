from dataclasses import dataclass, field

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
