import json

from typing import cast, Optional
from app.flex import Flexmeta, Flextable, pl


class Profile(Flextable):
    def __init__(self):
        super().__init__(Flexmeta("profiles", 1000))
        self.gender: str = ""
        self.name: str = ""
        self.city: str = ""
        self.state: str = ""
        self.email: str = ""
        self.coordinates: dict = {}
        self.pictures: list[str] = []
        self.date: pl.Datetime = pl.Datetime()

    @staticmethod
    def load(id: int) -> Optional["Profile"]:
        return cast(Profile, Flextable._load(Profile(), id))


with open("/app/src/temp_data/users.json", "rb") as handle:
    data = json.load(handle)

    for item in data["results"]:
        profile = Profile()
        profile.gender = item["gender"]
        profile.name = f"{item['name']['first']} {item['name']['last']}"
        profile.city = item["location"]["city"]
        profile.state = item["location"]["state"]
        profile.email = item["email"]
        profile.pictures = item["picture"]
        profile.coordinates = item["location"]["coordinates"]

        print(profile.id, profile.commit())

profile = Profile()

for item in profile.select(
    profile.table.filter(profile.c.id > 1040).sort("name", descending=True)
).fetch_all(1, 10):
    print(item.to_json())
