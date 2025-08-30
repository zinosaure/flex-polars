import os
import json
import uuid
import polars as pl

from pathlib import Path
from datetime import datetime
from typing import Any, Generator, Optional, Callable, TypeAlias


def protect(*protected):
    """Returns a metaclass that protects all attributes given as strings"""

    class Protect(type):
        has_base = False

        def __new__(cls, name, bases, attrs):
            if cls.has_base:
                for attribute in attrs:
                    if attribute in protected:
                        raise AttributeError(
                            'Overriding of attribute "%s" not allowed.' % attribute
                        )
            cls.has_base = True
            klass = super().__new__(cls, name, bases, attrs)
            return klass

    return Protect


class Flex(object):
    StorePath: Path = Path("/app/src/flexstore")
    ConnectionPool: dict[str, "Flexmeta"] = {}

    def __init__(self, attributes: dict = {}):
        for k, v in attributes.items():
            if isinstance(v, (list, tuple)):
                setattr(self, k, [Flex(x) if isinstance(x, dict) else x for x in v])
            else:
                setattr(self, k, Flex(v) if isinstance(v, dict) else v)

    @staticmethod
    def load(uniqid: str) -> "Flexmeta":
        return Flex.ConnectionPool[uniqid]


class Flexmeta:
    def __init__(
        self,
        name: str,
        minimum_id: int = 0,
        schema: Optional[dict[str, Any]] = None,
    ):
        name = os.path.basename(name.replace("/", "_").lower())
        self.__id: int = minimum_id
        self.__uniqid: str = str(uuid.uuid5(uuid.NAMESPACE_OID, name))
        self.__schema: Optional[dict[str, pl.DataType]] = schema
        self.__table: pl.DataFrame = self.DataFrame([])
        self.__datetime: datetime = datetime.now()
        self.__filename: Path = Path(Flex.StorePath / Path(f"{name}.json"))

        if not os.path.isdir(Flex.StorePath):
            os.umask(0)
            os.makedirs(Flex.StorePath, mode=0o777, exist_ok=True)

        if os.path.exists(self.__filename):
            self.__load_state()
        else:
            self.__save_state()

        Flex.ConnectionPool[self.__uniqid] = self

    @property
    def uniqid(self) -> str:
        return self.__uniqid

    @property
    def table(self) -> pl.DataFrame:
        return self.__table

    @property
    def schema(self) -> Optional[dict[str, pl.DataType]]:
        return self.__schema

    def DataFrame(self, data: list[dict[str, Any]]) -> pl.DataFrame:
        return pl.DataFrame(data, schema_overrides=self.__schema, nan_to_null=False)

    def next_id(self) -> int:
        return self.__id + 1

    def count(self) -> int:
        return self.__table.shape[0]

    def load(self, id: int) -> Optional[dict[str, Any]]:
        if not self.__table.is_empty():
            for items in self.__table.filter(pl.col.id == id).to_dicts():
                return items

    def append(self, data: dict[str, Any]) -> bool:
        table = self.DataFrame([data])

        try:
            if self.__table.is_empty():
                self.__table = table
            else:
                self.__table = pl.concat([self.__table, table], how="diagonal")

            return self.__save_state()
        except Exception as e:
            print("Flexmeta.append:", str(e))
            return False

    def update(self, data: dict[str, Any]) -> bool:
        n = self.__table.shape[0]
        self.__table = self.__table.remove(pl.col.id == data["id"])

        if n > self.__table.shape[0]:
            return self.append(data)

        return False

    def delete(self, id: int) -> bool:
        n = self.__table.shape[0]
        self.__table = self.__table.remove(pl.col.id == id)

        if n > self.__table.shape[0]:
            return self.__save_state()

        return False

    def __load_state(self) -> bool:
        try:
            with open(self.__filename, "rb") as handler:
                data = json.load(handler)
                self.__id = data["id"]
                self.__datetime = data["datetime"]
                self.__table = self.DataFrame(data["items"])
            return True
        except Exception as e:
            print("Flexmeta.__load_state:", str(e))

        return False

    def __save_state(self) -> bool:
        try:
            self.__datetime = datetime.now()
            data = json.dumps(
                {
                    "id": self.next_id(),
                    "datetime": self.__datetime,
                    "items": self.__table.to_dicts(),
                }
            )

        except Exception as e:
            print("Flexmeta.__save_state:", str(e))
            return False

        with open(self.__filename, "w+") as handler:
            return handler.write(data) >= 0


class Flextable(
    metaclass=protect(
        "c",
        "table",
        "schema",
        "flexmeta",
        "_load",
        "clone",
        "commit",
        "delete",
        "update",
        "to_dict",
        "to_json",
        "select",
    ),
):
    def __init__(self, flexmeta: Flexmeta):
        self.__flexmeta_uniqid__: str = flexmeta.uniqid
        self.id: int = flexmeta.next_id()

    @property
    def c(self) -> pl.expr.expr.Expr:
        return pl.col

    @property
    def flexmeta(self) -> Flexmeta:
        return Flex.ConnectionPool[self.__flexmeta_uniqid__]

    @property
    def table(self) -> pl.DataFrame:
        return self.flexmeta.table

    @property
    def schema(self) -> Optional[dict[str, pl.DataType]]:
        return self.flexmeta.schema

    @staticmethod
    def _load(flextable: "Flextable", id: int) -> Optional["Flextable"]:
        if items := flextable.flexmeta.load(id):
            return flextable.update(items)

    @staticmethod
    def load(id: int) -> Optional["Flextable"]:
        raise NotImplementedError(
            'Static method "load(id: int) -> Optional["Flextable"]" is not yet implemented!'
        )

    def clone(self, items: dict[str, Any] = {}) -> "Flextable":
        flextable = type(self)()  # type: ignore

        if items and isinstance(items, dict):
            return flextable.update(items)

        return flextable

    def commit(self) -> bool:
        if self.flexmeta.load(self.id):
            return self.flexmeta.update(self.to_dict())

        return self.flexmeta.append(self.to_dict())

    def delete(self) -> bool:
        return self.flexmeta.delete(self.id)

    def update(self, items: dict[str, Any]) -> "Flextable":
        for k, v in [(k, v) for k, v in self.__dict__.items() if k in items]:
            if isinstance(v, Flextable) and v != self:
                setattr(self, k, v.update(items[k]))
            else:
                setattr(self, k, items[k])

        return self

    def to_dict(self) -> dict[str, Any]:
        items: dict[str, Any] = {}

        for k, v in self.__dict__.items():
            if isinstance(v, Flextable) and v != self:
                items[k] = v.to_dict()
            else:
                items[k] = v

        return items

    def to_json(self, indent: Optional[int] = 4) -> str:
        def default(o: Any) -> Any:
            try:
                return o.__dict__
            except Exception:
                return str(o)

        return json.dumps(self.to_dict(), indent=indent, default=default)

    def select(self, table: pl.DataFrame) -> "Flextable.Flexselect":
        return Flextable.Flexselect(table, self)

    class Flexselect:
        Callback: TypeAlias = Optional[Callable[["Flextable"], "Flextable"]]

        def __init__(self, table: pl.DataFrame, flextable: "Flextable"):
            self.__items: list[dict[str, Any]] = table.to_dicts()
            self.__flextable: Flextable = flextable

        def __len__(self) -> int:
            return len(self.__items)

        def __iter__(self) -> Generator["Flextable", Any, None]:
            for item in self.__items:
                yield self.__flextable.clone(item)

        def count(self) -> int:
            return len(self.__items)

        def fetch_one(self, callback: Callback = None) -> Optional["Flextable"]:
            for item in self.__items:
                if callable(callback):
                    return callback(self.__flextable.clone(item))

                return self.__flextable.clone(item)

        def fetch_all(
            self, page: int = -1, limit: int = 10, callback: Callback = None
        ) -> list["Flextable"]:
            if page < 1 or limit < 1:
                items = self.__items
            else:
                paging = abs((page * limit) - limit)
                items = self.__items[paging : paging + limit]

            items = list(map(self.__flextable.clone, items))

            if callable(callback):
                return list(map(callback, items))

            return items
