import os
import json
import polars as pl

from pathlib import Path
from datetime import datetime
from typing import Any, Generator, Optional, Callable, TypeAlias, Union


class Flexmeta:
    FLEXSTORE_PATH: Path = Path("/app/src/flexstore")

    def __init__(self, name: str, id: int = 0, schema: Optional[dict[str, Any]] = None):
        name = os.path.basename(name.replace("/", "_").lower())
        self.schema: Optional[dict[str, Any]] = schema
        self.content: pl.DataFrame = pl.DataFrame(
            [],
            schema_overrides=self.schema,
            orient="row",
            strict=False,
            nan_to_null=True,
        )
        self.metadata: dict[str, Any] = {
            "id": id,
            "datatime": datetime.now().isoformat(),
        }
        self.fn_items: Path = Flexmeta.FLEXSTORE_PATH / Path(f"{name}.items.ndjson")
        self.fn_metadata: Path = Flexmeta.FLEXSTORE_PATH / Path(f"{name}.metadata.json")

        if os.path.exists(self.fn_metadata):
            with open(self.fn_metadata, "rb") as fp:
                self.metadata = json.load(fp)

        if not (is_init := os.path.exists(self.fn_items)):
            if not os.path.isdir(Flexmeta.FLEXSTORE_PATH):
                os.umask(0)
                os.makedirs(Flexmeta.FLEXSTORE_PATH, mode=0o777, exist_ok=True)

            self.save_database()

        if is_init and self.content.is_empty():
            self.content = pl.read_ndjson(
                self.fn_items,
                schema_overrides=self.schema,
                low_memory=True,
                ignore_errors=False,
            )

    def next_id(self) -> int:
        return self.metadata["id"] + 1

    def count(self) -> int:
        return self.content.shape[0]

    def load(self, id: int) -> Optional[dict[str, Any]]:
        if not self.content.is_empty():
            for items in self.content.filter(pl.col.id == id).to_dicts():
                return items

    def commit(self, items: Union[dict[str, Any], list[dict[str, Any]]]) -> bool:
        if isinstance(items, dict):
            items = [items]

        n_content: pl.DataFrame = pl.DataFrame(
            items,
            schema_overrides=self.schema,
            strict=False,
            nan_to_null=True,
        )

        try:
            self.content = pl.concat([self.content, n_content], how="diagonal")
            self.content = self.content.unique(
                subset=["id"], keep="last", maintain_order=True
            )

            return self.save_database()
        except Exception as e:
            print("Flexmeta.commit:", str(e))
            return False

    def delete(self, ident: Union[int, list[int]]) -> bool:
        if self.content.is_empty():
            return False

        n = self.content.shape[0]

        if not isinstance(ident, list):
            self.content = self.content.remove(pl.col.id == ident)
        else:
            self.content = self.content.remove(pl.col.id.is_in(ident))

        if n > self.content.shape[0]:
            return self.save_database()

        return False

    def save_database(self) -> bool:
        try:
            metadata: dict[str, Any] = {
                "id": self.next_id(),
                "datatime": datetime.now().isoformat(),
            }
            self.content.write_ndjson(self.fn_items)

            with open(self.fn_metadata, "w+") as fp:
                return fp.write(json.dumps(metadata)) > 0
        except Exception as e:
            print("Flexmeta.save_database:", str(e))
            return False


class Property:
    @classmethod
    def clone(cls, items: dict[str, Any] = {}) -> "Property":
        return cls().update(items)

    def __init__(self, items: dict[str, Any] = {}):
        for name, value in items.items():
            self.__setitem__(name, value)

    def __getitem__(self, name: str) -> Any:
        if name not in self.__dict__:
            raise AttributeError()

        if isinstance(item := self.__dict__[name], dict):
            return item
        elif isinstance(item, Property):
            return item.takeout()

        return item

    def __setitem__(self, name: str, value: Any):
        if name not in self.__dict__:
            raise AttributeError()

        if type(item := self.__dict__[name]) is type(value):
            self.__dict__[name] = value
        elif isinstance(item, Property) and isinstance(value, dict):
            self.__dict__[name] = item.update(value)

    def __str__(self) -> str:
        return self.json()

    def update(self, items: dict[str, Any]) -> "Property":
        for name, value in self.__dict__.items():
            if name in items and value != self:
                self.__setitem__(name, value)

        return self

    def takeout(self) -> dict[str, Any]:
        items: dict[str, Any] = {}

        for name, _ in self.__dict__.items():
            items[name] = self.__getitem__(name)

        return items

    def json(self, indent: Optional[int] = 4) -> str:
        return json.dumps(self.takeout(), indent=indent)


class ObjectMetaclass(type):
    def __call__(cls, *args, **kwargs):
        model: Object = type.__call__(cls, *args, **kwargs)

        return model.__post_init__()


class Object(Property, metaclass=ObjectMetaclass):
    flexmeta: Flexmeta

    @classmethod
    def load(cls, id: int) -> Optional[Union[Property, "Object"]]:
        if items := cls.flexmeta.load(id):
            return cls.clone(items)

    @classmethod
    def batch_commit(cls, data: list["Object"]) -> bool:
        if not len(data):
            return False

        return cls.flexmeta.commit(
            [model.takeout() for model in data if model.flexmeta == cls.flexmeta]
        )

    @classmethod
    def batch_delete(cls, data: list["Object"]) -> bool:
        if not len(data):
            return False

        return cls.flexmeta.delete(
            [model.id for model in data if model.flexmeta == cls.flexmeta]
        )

    def __init__(self):
        self.id: int = self.flexmeta.next_id()

    def __post_init__(self) -> "Object":
        if not hasattr(self, "flexmeta"):
            raise Exception('Please add "super().__init__()" in "__init__()"!')

        return self

    def commit(self) -> bool:
        return self.flexmeta.commit(self.takeout())

    def delete(self) -> bool:
        return self.flexmeta.delete(self.id)

    def update(self, items: dict[str, Any]) -> Union[Property, "Object"]:
        return super().update(items)

    def takeout(self) -> dict[str, Any]:
        return super().takeout()

    def select(self, callback: Callable[[pl.DataFrame], pl.DataFrame]) -> "Select":
        return Select(self, callback(self.flexmeta.content))


class Select:
    Callback: TypeAlias = Optional[Callable[[Property | Object], Property | Object]]

    def __init__(self, model: Object, content: pl.DataFrame) -> None:
        self.model: Object = model
        self.items: list[dict[str, Any]] = content.to_dicts()

    def __len__(self) -> int:
        return len(self.items)

    def __iter__(self) -> Generator[Property | Object, Any, None]:
        for item in self.items:
            yield self.model.clone(item)

    def count(self) -> int:
        return len(self.items)

    def map(self, callback: Callable[[dict[str, Any]], dict[str, Any]]):
        self.items = list(map(callback, self.items))

    def head(
        self, limit: int = 10, callback: Callback = None
    ) -> list[Property | Object]:
        items = self.items[0:limit]
        items = list(map(self.model.clone, items))

        if callable(callback):
            return list(map(callback, items))

        return items

    def tail(
        self, limit: int = 10, callback: Callback = None
    ) -> list[Property | Object]:
        items = self.items[-limit:]
        items = list(map(self.model.clone, items))

        if callable(callback):
            return list(map(callback, items))

        return items

    def fetch_one(self, callback: Callback = None) -> Optional[Property | Object]:
        for item in self.items:
            if callable(callback):
                return callback(self.model.clone(item))

            return self.model.clone(item)

    def fetch_all(
        self, page: int = -1, limit: int = 10, callback: Callback = None
    ) -> list[Property | Object]:
        if page < 1 or limit < 1:
            items = self.items
        else:
            paging = abs((page * limit) - limit)
            items = self.items[paging : paging + limit]

        items = list(map(self.model.clone, items))

        if callable(callback):
            return list(map(callback, items))

        return items


###


Flexmeta.FLEXSTORE_PATH = Path("/app/tests/flexstore")

from datetime import datetime


class P(Property):
    def __init__(self, items: dict[str, Any] = {}):
        super().__init__(items)
        self.uniqid: str = "252 6545"
        self.date: datetime = datetime.now()

    def update(self, items: dict[str, Any]) -> Property:
        items["date"] = datetime.fromisoformat(items["date"])
        return super().update(items)

    def takeout(self) -> dict[str, Any]:
        items = super().takeout()
        items["date"] = self.date.isoformat()
        return items


class M(Object):
    flexmeta: Flexmeta = Flexmeta("users", schema={"mymodel": pl.Object})

    def __init__(self):
        super().__init__()
        self.name = "John Doe"
        self.phone = "0601363265"
        self.mymodel: P = P()


m = M()
p = P()

print(m.commit())