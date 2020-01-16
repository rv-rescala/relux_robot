from dataclasses import dataclass, field

@dataclass(frozen=True)
class Item:
    id: str
    name: str
    url: str
    img_src: str
    category: str