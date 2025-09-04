import os
import json
import polars as pl

from pathlib import Path
from datetime import datetime
from typing import Any, Generator, Optional, Callable, TypeAlias, Union


class Flexstore:
    PATH: Path = Path("/app/src/flexstore")

    def __init__(self, name: str, schema: Optional[dict[str, Any]] = None):
        self.name: str = name.replace("/", "_").lower()
        self.schema: Optional[dict[str, Any]] = schema
        self.fn_lines: Path = Flexstore.PATH / Path(f"{name}.lines.ndjson")
        self.fn_metadata: Path = Flexstore.PATH / Path(f"{name}.metadata.json")

        if not os.path.isdir(Flexstore.PATH):
            os.umask(0)
            os.makedirs(Flexstore.PATH, mode=0o777, exist_ok=True)

    def is_exists(self) -> bool:
        return os.path.exists(self.fn_lines) or os.path.exists(self.fn_metadata)

    def DataFrame(
        self,
        data: list[dict[str, Any]],
        infer_length: int = 100,
    ) -> pl.DataFrame:
        return pl.DataFrame(
            data,
            orient="row",
            strict=True,
            nan_to_null=True,
            schema_overrides=self.schema,
            infer_schema_length=infer_length,
        )

    def load(self, chunk_size: int = 1000) -> tuple[pl.DataFrame, dict[str, Any]]:
        lines: pl.DataFrame = self.DataFrame([])
        n_lines: list[dict[str, Any]] = []

        for i, line in enumerate(open(self.fn_lines, "r").readlines()):
            n_lines.append(json.loads(line))

            if i % chunk_size == 0:
                lines = pl.concat([lines, self.DataFrame(n_lines)], how="diagonal")
                n_lines = []

        try:
            lines = pl.concat([lines, self.DataFrame(n_lines)], how="diagonal")
        except Exception:
            print(
                "Flexstore.load: Data type are incompatible! Maybe you have to define some columns as 'pl.Object' data type in your schema."
            )

        return lines, self.load_metadata()

    def load_metadata(self) -> dict[str, Any]:
        with open(self.fn_metadata, "rb") as fp:
            return json.load(fp)

    def save(self, lines: pl.DataFrame, metadata: dict[str, Any]) -> bool:
        try:
            with open(self.fn_lines, "w+") as fp:
                fp.write("\n".join(map(json.dumps, lines.to_dicts())))

            with open(self.fn_metadata, "w+") as fp:
                return fp.write(json.dumps(metadata)) > 0
        except Exception as e:
            print("Flexstore.save:", str(e))

        return False


class Flexmeta:
    def __init__(self, name: str, schema: Optional[dict[str, Any]] = None, id: int = 0):
        self.flexstore: Flexstore = Flexstore(name, schema)
        self.lines: pl.DataFrame = self.flexstore.DataFrame([])
        self.metadata: dict[str, Any] = {
            "id": id,
            "count": 0,
            "datetime": datetime.now().isoformat(),
        }

        if not self.flexstore.is_exists():
            self.flexstore.save(self.lines, self.metadata)

        if self.lines.is_empty():
            self.lines, self.metadata = self.flexstore.load()

    def next_id(self) -> int:
        self.metadata["id"] += 1

        return self.metadata["id"]

    def count(self) -> int:
        return self.lines.shape[0]

    def load(self, id: int) -> Optional[dict[str, Any]]:
        if not self.lines.is_empty():
            for line in self.lines.filter(pl.col.id == id).to_dicts():
                return line

    def commit(self, line: Union[dict[str, Any], list[dict[str, Any]]]) -> bool:
        if isinstance(line, dict):
            line = [line]

        n_lines: pl.DataFrame = self.flexstore.DataFrame(line)

        try:
            self.lines = pl.concat([self.lines, n_lines], how="diagonal")
            self.lines = self.lines.unique(
                subset=["id"], keep="last", maintain_order=True
            )
            self.metadata["count"] = self.lines.shape[0]
            self.metadata["datetime"] = datetime.now().isoformat()

            return self.flexstore.save(self.lines, self.metadata)
        except Exception:
            print(
                "Flexstore.commit: Data type are incompatible! Maybe you have to define some columns as 'pl.Object' data type in your schema."
            )
            return False

    def delete(self, ident: Union[int, list[int]]) -> bool:
        if self.lines.is_empty():
            return False

        n = self.lines.shape[0]

        if not isinstance(ident, list):
            self.lines = self.lines.remove(pl.col.id == ident)
        else:
            self.lines = self.lines.remove(pl.col.id.is_in(ident))

        if n > self.lines.shape[0]:
            return self.flexstore.save(self.lines, self.metadata)

        return False

    class Exception(Exception):
        def __init__(self, classname: str):
            super().__init__(
                f"This object is not defined as a Flexmeta's Object! First implement: `flexmeta: Flexmeta = Flexmeta(...)` as a static variable in '{classname}' class."
            )


class Flexobject:
    flexmeta: Flexmeta
    is_unstruct: list[str] = []

    @classmethod
    def clone(cls, line: dict[str, Any] = {}) -> "Flexobject":
        return cls().update(line, struct=True)

    @classmethod
    def load(cls, id: int) -> Optional["Flexobject"]:
        if not hasattr(cls, "flexmeta"):
            raise Flexmeta.Exception(cls.__name__)

        if line := cls.flexmeta.load(id):
            return cls.clone(line)

    @classmethod
    def batch_commit(cls, objects: list["Flexobject"]) -> bool:
        if not hasattr(cls, "flexmeta"):
            raise Flexmeta.Exception(cls.__name__)

        if not len(objects):
            return False

        return cls.flexmeta.commit(
            [object.takeout() for object in objects if object.flexmeta == cls.flexmeta]
        )

    @classmethod
    def batch_delete(cls, objects: list["Flexobject"]) -> bool:
        if not hasattr(cls, "flexmeta"):
            raise Flexmeta.Exception(cls.__name__)

        if not len(objects):
            return False

        return cls.flexmeta.delete(
            [object.id for object in objects if object.flexmeta == cls.flexmeta]
        )

    def __init__(self):
        if self.is_flexobject():
            self.id: int = self.flexmeta.next_id()

    def __str__(self) -> str:
        return self.json()

    def __getitem__(self, name: str) -> Any:
        if name not in self.__dict__:
            raise AttributeError()

        if isinstance(line := self.__dict__[name], dict):
            return line
        elif isinstance(line, Flexobject):
            return line.takeout()

        return line

    def __setitem__(self, name: str, value: Any):
        if name not in self.__dict__:
            raise AttributeError()

        if type(line := self.__dict__[name]) is type(value):
            self.__dict__[name] = value
        elif isinstance(line, Flexobject) and isinstance(value, dict):
            self.__dict__[name] = line.update(value)

    def is_flexobject(self) -> bool:
        return hasattr(self, "flexmeta")

    def fetch(self) -> "Flexobject":
        if line := self.flexmeta.load(self.id):
            self.update(line)

        return self

    def update(self, line: dict[str, Any], struct: bool = False) -> "Flexobject":
        for name, value in self.__dict__.items():
            if name in line and value != self:
                self.__setitem__(name, line[name])

            if struct and name in self.is_unstruct and isinstance(value, Flexobject):
                self.__dict__[name] = value.fetch()

        return self

    def takeout(self, unstruct: bool = False) -> dict[str, Any]:
        line: dict[str, Any] = {}

        for name, value in self.__dict__.items():
            line[name] = self.__getitem__(name)

            if unstruct and name in self.is_unstruct and isinstance(value, Flexobject):
                line[name] = {"id": value.id}

        return line

    def json(self, indent: Optional[int] = 4) -> str:
        return json.dumps(self.takeout(), indent=indent)

    def commit(self) -> bool:
        if not self.is_flexobject():
            raise Flexmeta.Exception(self.__class__.__name__)

        return self.flexmeta.commit(self.takeout(unstruct=True))

    def delete(self) -> bool:
        if not self.is_flexobject():
            raise Flexmeta.Exception(self.__class__.__name__)

        return self.flexmeta.delete(self.id)

    def select(self, callback: Callable[[pl.DataFrame], pl.DataFrame]) -> "Flexselect":
        if not self.is_flexobject():
            raise Flexmeta.Exception(self.__class__.__name__)

        return Flexselect(self, callback(self.flexmeta.lines))


class Flexselect:
    Callback: TypeAlias = Optional[Callable[[Flexobject], Flexobject]]

    def __init__(self, object: Flexobject, lines: pl.DataFrame) -> None:
        self.object: Flexobject = object
        self.lines: list[dict[str, Any]] = lines.to_dicts()

    def __len__(self) -> int:
        return len(self.lines)

    def __iter__(self) -> Generator[Flexobject, Any, None]:
        for item in self.lines:
            yield self.object.clone(item)

    def count(self) -> int:
        return len(self.lines)

    def map(self, callback: Callable[[dict[str, Any]], dict[str, Any]]):
        self.lines = list(map(callback, self.lines))

    def head(self, limit: int = 10, callback: Callback = None) -> list[Flexobject]:
        lines = list(map(self.object.clone, self.lines[0:limit]))

        if callable(callback):
            return list(map(callback, lines))

        return lines

    def tail(self, limit: int = 10, callback: Callback = None) -> list[Flexobject]:
        lines = list(map(self.object.clone, self.lines[-limit:]))

        if callable(callback):
            return list(map(callback, lines))

        return lines

    def fetch_one(self, callback: Callback = None) -> Optional[Flexobject]:
        for item in self.lines:
            if callable(callback):
                return callback(self.object.clone(item))

            return self.object.clone(item)

    def fetch_all(
        self, page: int = -1, limit: int = 10, callback: Callback = None
    ) -> list[Flexobject]:
        if page < 1 or limit < 1:
            lines = self.lines
        else:
            paging = abs((page * limit) - limit)
            lines = self.lines[paging : paging + limit]

        lines = list(map(self.object.clone, lines))

        if callable(callback):
            return list(map(callback, lines))

        return lines


###


Flexstore.PATH = Path("/app/tests/flexstore")


class Contact(Flexobject):
    flexmeta: Flexmeta = Flexmeta("contacts")

    def __init__(self):
        super().__init__()
        self.telegram: str = "06-00-00-00-00"
        self.whatsapp: str = "07-00-00-00-00"
        self.in_contact: bool = False


class Location(Flexobject):
    flexmeta: Flexmeta = Flexmeta("locations")

    def __init__(self):
        super().__init__()
        self.street: str = "?"
        self.state: str = "?"
        self.city: str = "?"


class Level(Flexobject):
    def __init__(self):
        super().__init__()
        self.value: int = 0
        self.label: str = "level 0"


class Profile(Flexobject):
    flexmeta: Flexmeta = Flexmeta(
        "profiles",
        schema={
            "contact": pl.Object,
        },
    )
    is_unstruct: list[str] = [
        # "contact",
    ]

    def __init__(self):
        super().__init__()
        self.name: str = "?"
        self.level: Level = Level()
        self.contact: Contact = Contact()
        self.location: Location = Location()


# for i in range(1000):
#     profile = Profile()
#     profile.name = f"name {i}"
#     profile.level.value = i
#     profile.level.label = f"level {i}"
#     profile.contact.telegram = f"06-{i}{i}-{i}{i}-{i}{i}-{i}{i}"
#     profile.contact.whatsapp = f"07-{i}{i}-{i}{i}-{i}{i}-{i}{i}"
#     profile.contact.in_contact = i % 2 == 0
#     profile.contact.commit()
#     profile.location.street = f"street {i}"
#     profile.location.state = f"state {i}"
#     profile.location.city = f"city {i}"
#     profile.location.commit()
#     profile.commit()

if profile := Profile.load(2913):
    print(profile)
