import sys
import json

sys.path.append("../src/")

from typing import cast, Optional, Any
from flex import Flex
from datetime import datetime

Flex.FLEXSTORE_PATH = "/app/tests/flexstore"


class Contact(Flex.Flexobject):
    def __init__(self) -> None:
        self.cell: str = "+33 06 25 35 55 33"
        self.smartphone: bool = False


class Profile(Flex.Flextable):
    def __init__(self):
        super().__init__(Flex.Flexmeta("profiles", 1000))
        self.gender: str = ""
        self.name: str = ""
        self.city: str = ""
        self.email: str = ""
        self.coordinates: dict = {}
        self.pictures: list[str] = []
        self.date: datetime = datetime.now()
        self.contact: Contact = Contact()

    @staticmethod
    def load(id: int) -> Optional["Profile"]:
        return cast(Profile, Flex.Flextable._load(Profile(), id))

    def on_compose(self, name: str, value: Any) -> Any:
        if value and name == "date" and isinstance(value, str):
            return datetime.fromisoformat(value)

        return value

    def on_decompose(self, name: str, value: Any) -> Any:
        if value and name == "date" and isinstance(value, datetime):
            return value.isoformat()

        return super().on_decompose(name, value)


with open("/app/tests/temp_data/users.json", "rb") as handle:
    data = json.load(handle)
    commits = []

    for item in data["results"]:
        profile = Profile()
        profile.gender = item["gender"]
        profile.name = f"{item['name']['first']} {item['name']['last']}"
        profile.city = item["location"]["city"]
        profile.email = item["email"]
        profile.pictures = item["picture"]
        profile.coordinates = item["location"]["coordinates"]
        commits.append(profile)

    Profile.batch_commit(commits)

profile = Profile()


def x(table: Flex.Flextable.DataFrame) -> Flex.Flextable.DataFrame:
    # table = table.filter(profile.c.name.str.starts_with("Jo"))
    table = table.sort("id", descending=False)

    return table


for v in profile.select(x).tail(5):
    print(v.to_json())
