import os
import json
import glob
import math
import uuid
import random
import polars as pl

from pathlib import Path
from collections import namedtuple
from typing import Any, Iterable, Optional, Callable


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
        schema: Optional[dict[str, pl.DataType]] = None,
    ):
        name = os.path.basename(name.replace("/", "_").lower())
        self.__id: int = minimum_id
        self.__uniqid: str = str(uuid.uuid5(uuid.NAMESPACE_OID, name))
        self.__schema: Optional[dict[str, pl.DataType]] = schema
        self.__lines: pl.DataFrame = self.DataFrame([])
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
    def lines(self) -> pl.DataFrame:
        return self.__lines

    @property
    def schema(self) -> Optional[dict[str, pl.DataType]]:
        return self.__schema

    def DataFrame(self, data: list[dict[str, Any]]) -> pl.DataFrame:
        return pl.DataFrame(data, schema=self.__schema, nan_to_null=False)

    def next_id(self) -> int:
        return self.__id + 1

    def count(self) -> int:
        return self.__lines.shape[0]

    def load(self, id: int) -> Optional[dict[str, Any]]:
        if not self.__lines.is_empty():
            for items in self.__lines.filter(pl.col.id == id).to_dicts():
                return items

    def append(self, data: dict[str, Any]) -> bool:
        lines = self.DataFrame([data])

        try:
            if self.__lines.is_empty():
                self.__lines = lines
            else:
                self.__lines = pl.concat([self.__lines, lines], how="diagonal")

            return self.__save_state()
        except Exception as e:
            print("Flexmeta.append:", str(e))
            return False

    def update(self, data: dict[str, Any]) -> bool:
        n = self.__lines.shape[0]
        self.__lines = self.__lines.remove(pl.col.id == data["id"])

        if n > self.__lines.shape[0]:
            return self.append(data)

        return False

    def delete(self, id: int) -> bool:
        n = self.__lines.shape[0]
        self.__lines = self.__lines.remove(pl.col.id == id)

        if n > self.__lines.shape[0]:
            return self.__save_state()

        return False

    def __load_state(self) -> bool:
        try:
            with open(self.__filename, "rb") as handler:
                data = json.load(handler)
                self.id = data["id"]
                self.__lines = self.DataFrame(data["lines"])
            return True
        except Exception as e:
            print("Flexmeta.__load_state:", str(e))

        return False

    def __save_state(self) -> bool:
        try:
            data = json.dumps({"id": self.next_id(), "lines": self.__lines.to_dicts()})

        except Exception as e:
            print("Flexmeta.__save_state:", str(e))
            return False

        with open(self.__filename, "w+") as handler:
            return handler.write(data) >= 0


class Flextable(
    metaclass=protect(
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
    def flexmeta(self) -> Flexmeta:
        return Flex.ConnectionPool[self.__flexmeta_uniqid__]

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

    def select(self) -> "Flextable.Flexselect":
        return Flextable.Flexselect(self, self.flexmeta.lines)

    class Flexselect:
        def __init__(self, flextable: "Flextable", lines: pl.DataFrame):
            self.__lines: pl.DataFrame = lines
            self.__flextable: Flextable = flextable

        def __getattr__(self, name: str) -> pl.Expr:
            return pl.col(name)

        def __getitem__(self, name: str) -> pl.Expr:
            return pl.col(name)

        def __call__(self) -> pl.DataFrame:
            return self.__lines

        def __len__(self) -> int:
            return self.count()

        def __iter__(self):
            for item in self.__lines.iter_rows(named=True):
                yield item

        def empty(self):
            self.__lines.clear()

        def count(self) -> int:
            return self.__lines.shape[0]

        def distinct(self, *args, **kwords):
            self.__lines = self.__lines.unique(args or kwords)

        def map(self, *args, **kwords):
            self.__lines = self.__lines.map_rows(args or kwords)  # type: ignore

        def where(self, *args, **kwords):
            self.__lines = self.__lines.filter(args or kwords)

        def sort(self, *args, **kwords):
            self.__lines = self.__lines.sort(args or kwords)

        def fetch_one(self) -> Optional["Flextable"]:
            for item in self.__lines.to_dicts():
                return self.__flextable.clone(item)

        def fetch_all(
            self, page: int = -1, limit: int = 10, callback: Optional[Callable] = None
        ) -> list["Flextable"]:
            lines = self.__lines.to_dicts()

            if page > 1 and limit > 1:
                paging = abs((page * limit) - limit)
                lines = lines[paging : paging + limit]

            lines = list(map(self.__flextable.clone, lines))

            if callable(callback):
                return list(map(callback, lines))

            return lines
