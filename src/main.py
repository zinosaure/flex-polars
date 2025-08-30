import json

from typing import cast, Optional
from app.flex import Flexmeta, Flextable
from datetime import datetime


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
        self.dob: int = int(datetime.now().timestamp())

    @staticmethod
    def load(id: int) -> Optional["Profile"]:
        return cast(Profile, Flextable._load(Profile(), id))


# with open("/app/src/temp_data/users.json", "rb") as handle:
#     data = json.load(handle)

#     for item in data["results"]:
#         profile = Profile()
#         profile.gender = item["gender"]
#         profile.name = f"{item['name']['first']} {item['name']['last']}"
#         profile.city = item["location"]["city"]
#         profile.state = item["location"]["state"]
#         profile.email = item["email"]
#         # profile.pictures = item["picture"]
#         profile.coordinates = item["location"]["coordinates"]
#         profile.dob = int(datetime.fromisoformat(item["dob"]["date"]).timestamp())

#         print(profile.id, profile.commit())

profile = Profile()
select = profile.select()

select().filter(select.id.is_between(1010, 1020))
print(select.fetch_all(callback=lambda x: x.to_dict()))