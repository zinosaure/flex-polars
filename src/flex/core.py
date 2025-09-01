import os
import json
import polars as pl

from pathlib import Path
from datetime import datetime
from typing import Any, Generator, Optional, Callable, TypeAlias, Union


def protect(*protected):
    """Returns a metaclass that protects all objects given as strings"""

    class Protect(type):
        has_base = False

        def __new__(cls, name, bases, attrs):
            if cls.has_base:
                for object in attrs:
                    if object in protected:
                        raise objectError(
                            'Overriding of object "%s" not allowed.' % object
                        )
            cls.has_base = True
            klass = super().__new__(cls, name, bases, attrs)
            return klass

    return Protect


class Flexmeta:
    @property
    def metadata(self) -> dict[str, Any]:
        return self.__metadata__

    @property
    def table(self) -> pl.DataFrame:
        return self.__table__.clone()

    @property
    def schema(self) -> Optional[dict[str, Any]]:
        return self.__schema__

    @property
    def content(self) -> dict[str, Any]:
        return {
            "metadata": {
                "id": self.next_id(),
                "count": self.count(),
                "datetime": datetime.now().isoformat(),
            },
            "items": self.__table__.to_dicts(),
        }

    def __init__(
        self,
        name: str,
        minimum_id: int = 0,
        schema: Optional[dict[str, Any]] = None,
    ):
        name = os.path.basename(name.replace("/", "_").lower())
        self.__metadata__: dict[str, Any] = {
            "id": minimum_id,
            "count": 0,
            "datetime": datetime.now().isoformat(),
        }
        self.__schema__: Optional[dict[str, Any]] = schema
        self.__table__: pl.DataFrame = self.DataFrame([])
        self.__filename__: Path = Path(Flex.FLEXSTORE_PATH / Path(f"{name}.json"))

        if os.path.exists(self.__filename__):
            with open(self.__filename__, "rb") as handler:
                data = json.load(handler)
                self.__metadata__ = data["metadata"]
                # self.__table = pl.read_ndjson(
                # StringIO(data["items"]),
                # schema_overrides=self.__schema,
                # low_memory=True,
                # ignore_errors=True
                # )
                self.__table__ = self.DataFrame(data["items"])
        else:
            if not os.path.isdir(Flex.FLEXSTORE_PATH):
                os.umask(0)
                os.makedirs(Flex.FLEXSTORE_PATH, mode=0o777, exist_ok=True)

            self.write_to_json()

    def DataFrame(self, data: list[dict[str, Any]]) -> pl.DataFrame:
        return pl.DataFrame(
            data,
            schema_overrides=self.__schema__,
            orient="row",
            strict=False,
            nan_to_null=True,
        )

    def next_id(self) -> int:
        return self.__metadata__["id"] + 1

    def count(self) -> int:
        return self.__table__.shape[0]

    def load(self, id: int) -> Optional[dict[str, Any]]:
        if not self.__table__.is_empty():
            for items in self.__table__.filter(pl.col.id == id).to_dicts():
                return items

    def commit(self, items: dict[str, Any] | list[dict[str, Any]]) -> bool:
        if isinstance(items, list):
            table = self.DataFrame(items)
        else:
            table = self.DataFrame([items])

        try:
            self.__table__ = pl.concat([self.__table__, table], how="diagonal")
            self.__table__ = self.__table__.unique(
                subset=["id"], keep="last", maintain_order=True
            )

            return self.write_to_json()
        except Exception as e:
            print("Flexmeta.commit:", str(e))
            return False

    def delete(self, ident: int | list[int]) -> bool:
        if self.__table__.is_empty():
            return False

        n = self.__table__.shape[0]

        if not isinstance(ident, list):
            self.__table__ = self.__table__.remove(pl.col.id == ident)
        else:
            self.__table__ = self.__table__.remove(pl.col.id.is_in(ident))

        if n > self.__table__.shape[0]:
            return self.write_to_json()

        return False

    def batch_commit(self, data: list["Flexmodel"]) -> bool:
        if not len(data):
            return False

        return self.commit([model.object.takeout(safe_mode=True) for model in data])

    def batch_delete(self, data: list["Flexmodel"]) -> bool:
        if not len(data):
            return False

        return self.delete([model.id for model in data])

    def write_to_json(self) -> bool:
        try:
            data = json.dumps(self.content)
        except Exception as e:
            print("Flexmeta.write_to_json:", str(e))
            return False

        with open(self.__filename__, "w+") as handler:
            return handler.write(data) > 0


class Flexheritage:
    def on_update(self, items: dict[str, Any]) -> dict[str, Any]:
        return items

    def on_takeout(self, items: dict[str, Any]) -> dict[str, Any]:
        return items


class Flexmodel(
    Flexheritage,
    metaclass=protect(
        *[
            "_load",
            "__object__",
            "meta",
            "meta_table",
            "meta_schema",
            "column",
            "object",
            "select",
        ]
    ),
):
    __meta__: Flexmeta

    @property
    def meta(self) -> Flexmeta:
        return self.__meta__

    @property
    def meta_table(self) -> pl.DataFrame:
        return self.__meta__.table

    @property
    def meta_schema(self) -> Optional[dict[str, Any]]:
        return self.__meta__.schema

    @property
    def column(self) -> pl.expr.Expr:
        return pl.col

    @property
    def object(self) -> "Flexobject":
        return self.__object__

    def __init__(self):
        self.id: int = self.__meta__.next_id()
        self.__object__: Flexobject = Flexobject(self)

    @staticmethod
    def load(id: int) -> Optional[Flexheritage]:
        """
        Example how to implement this static method.
        --------

        @staticmethod
        def load(id: int) -> Optional["MyObject"]:
            return cast(MyObject, MyObject()._load(id))
        """

        raise NotImplementedError(
            'Static method "load(id: int) -> Optional[Flexheritage]" is not yet implemented!'
        )

    def _load(self, id: int) -> Optional[Flexheritage]:
        if items := self.__meta__.load(id):
            return self.object.update(items)

    def clone(
        self, items: dict[str, Any] = {}, args_init: dict[str, Any] = {}
    ) -> Flexheritage:
        """
        Example how to re-implement this method.
        --------

        def clone(self, items: dict[str, Any] = {}, args_init: dict[str, Any] = {}) -> "MyObject":
            return cast(MyObject, super().clone(items, args_init))
        """

        if (model := type(self)(**args_init)) and items:
            return model.object.update(items)

        return model

    def commit(self) -> bool:
        return self.__meta__.commit(self.__object__.takeout(safe_mode=True))

    def delete(self) -> bool:
        return self.__meta__.delete(self.id)

    def select(self, callback: Callable[[pl.DataFrame], pl.DataFrame]) -> "Flexselect":
        return Flexselect(callback(self.__meta__.table), self)


class Flexobject(Flexheritage):
    RESERVED_KEYWORDS: list[str] = ["__meta__", "__model__", "__object__"]

    def __init__(self, model: Flexheritage):
        self.__model__: Flexheritage = model

    def __str__(self) -> str:
        return self.json()

    def __getitem__(self, name: str) -> Any:
        return self.__model__.__dict__.get(name)

    def __setitem__(self, name: str, value: Any):
        if name not in self.__model__.__dict__:
            return

        item = self.__model__.__dict__[name]

        if type(item) is type(value):
            item = value
        elif isinstance(value, dict):
            if isinstance(item, Flexobject):
                item.update(value)
            elif isinstance(item, Flexmodel):
                item.object.update(value)
            elif isinstance(item, object):
                item.__dict__.update(value)
        else:
            return

        self.__model__.__dict__[name] = item

    def update(self, items: dict[str, Any]) -> Flexheritage:
        items = self.__model__.on_update(items)

        for name, _ in self.__model__.__dict__.items():
            if name not in self.RESERVED_KEYWORDS and name in items:
                self.__setitem__(name, items[name])

        return self.__model__

    def takeout(self, safe_mode: bool = False) -> dict[str, Any]:
        items: dict[str, Any] = {}

        for name, item in self.__model__.__dict__.items():
            if name not in self.RESERVED_KEYWORDS:
                if isinstance(item, Flexobject):
                    items[name] = item.takeout(safe_mode)
                elif isinstance(item, Flexmodel):
                    items[name] = item.object.takeout(safe_mode)
                elif isinstance(item, object) and hasattr(item, "__dict__"):
                    items[name] = item.__dict__
                else:
                    items[name] = item

        if safe_mode:
            items = self.__model__.on_takeout(items)

        return items

    def json(self, indent: Optional[int] = 4) -> str:
        return json.dumps(self.takeout(safe_mode=True), indent=indent)


class Flexselect:
    Callback: TypeAlias = Optional[Callable[[Flexmodel], Flexmodel]]

    def __init__(self, table: pl.DataFrame, model: Flexmodel):
        self.__items__: list[dict[str, Any]] = table.to_dicts()
        self.__table__: Flexmodel = model

    def __len__(self) -> int:
        return len(self.__items__)

    def __iter__(self) -> Generator[Flexmodel, Any, None]:
        for item in self.__items__:
            yield self.__table__.clone(item)  # type: ignore

    def count(self) -> int:
        return len(self.__items__)

    def map(self, callback: Callable[[dict[str, Any]], dict[str, Any]]):
        self.__items__ = list(map(callback, self.__items__))

    def head(self, limit: int = 10, callback: Callback = None) -> list[Flexmodel]:
        items = self.__items__[0:limit]
        items = list(map(self.__table__.clone, items))

        if callable(callback):
            return list(map(callback, items))

        return items

    def tail(self, limit: int = 10, callback: Callback = None) -> list[Flexmodel]:
        items = self.__items__[-limit:]
        items = list(map(self.__table__.clone, items))

        if callable(callback):
            return list(map(callback, items))

        return items

    def fetch_one(self, callback: Callback = None) -> Optional[Flexmodel]:
        for item in self.__items__:
            if callable(callback):
                return callback(self.__table__.clone(item))

            return self.__table__.clone(item)

    def fetch_all(
        self, page: int = -1, limit: int = 10, callback: Callback = None
    ) -> list[Flexmodel]:
        if page < 1 or limit < 1:
            items = self.__items__
        else:
            paging = abs((page * limit) - limit)
            items = self.__items__[paging : paging + limit]

        items = list(map(self.__table__.clone, items))

        if callable(callback):
            return list(map(callback, items))

        return items


class Flex(object):
    Pl: TypeAlias = pl
    DataFrame: TypeAlias = pl.DataFrame
    FLEXSTORE_PATH: Path = Path("/app/src/flexstore")

    @staticmethod
    def setPath(store_path: Path):
        Flex.FLEXSTORE_PATH = store_path

    class Flexmeta(Flexmeta):
        pass

    class Flexmodel(Flexmodel):
        pass


class Ro:
    def __init__(self, items: dict[str, Any] = {}):
        for k, v in items.items():
            if isinstance(v, (list, tuple)):
                setattr(self, k, [Ro(x) if isinstance(x, dict) else x for x in v])
            else:
                setattr(self, k, Ro(v) if isinstance(v, dict) else v)
