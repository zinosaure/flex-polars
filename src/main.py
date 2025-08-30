import json

from typing import cast, Optional, Any
from app.flex import Flexmeta, Flextable
from datetime import datetime


class Profile(Flextable):
    def __init__(self):
        super().__init__(Flexmeta("profiles", 1000))
        self.gender: str = ""
        self.name: str = ""
        self.city: str = ""
        # self.state: str = ""
        self.email: str = ""
        self.coordinates: dict = {}
        self.pictures: list[str] = []
        self.date: datetime = datetime.now()

    @staticmethod
    def load(id: int) -> Optional["Profile"]:
        return cast(Profile, Flextable._load(Profile(), id))

    def on_compose(self, name: str, value: Any) -> Any:
        if value and name == "date" and isinstance(value, str):
            return datetime.fromisoformat(value)

        return value

    def on_decompose(self, name: str, value: Any) -> Any:
        if value and name == "date" and isinstance(value, datetime):
            return value.isoformat()

        return value


with open("/app/src/temp_data/users.json", "rb") as handle:
    data = json.load(handle)

    for item in data["results"]:
        profile = Profile()
        profile.gender = item["gender"]
        profile.name = f"{item['name']['first']} {item['name']['last']}"
        profile.city = item["location"]["city"]
        # profile.state = item["location"]["state"]
        profile.email = item["email"]
        profile.pictures = item["picture"]
        profile.coordinates = item["location"]["coordinates"]

        print(profile.id, profile.commit())

profile = Profile()

for item in profile.select(
    profile.table.filter(profile.c.id > 1040).sort("name", descending=True)
).fetch_all(1, 10):
    print(item)
