import os
import glob
import json
import polars as pl

from pathlib import Path
from datetime import datetime
from typing import Any, Generator, Optional, Callable, Union


def protect(*protected):
    """Returns a metaclass that protects all objects given as strings"""

    class Protect(type):
        has_base = False

        def __new__(cls, name, bases, attrs):
            if cls.has_base:
                for object in attrs:
                    if object in protected:
                        raise AttributeError(
                            'Overriding of object "%s" not allowed.' % object
                        )
            cls.has_base = True
            klass = super().__new__(cls, name, bases, attrs)
            return klass

    return Protect


class Flexmeta:
    PATH: Path = Path("/app/src/flexstore")

    @classmethod
    def setup(cls, path: str | Path):
        cls.PATH = Path(path)

    def __init__(
        self, name: str, schema: Optional[dict[str, Any]] = None, minimum_id: int = 0
    ):
        self.name: str = name.replace("/", "_").lower()
        self.name_d: Path = self.PATH / Path(self.name)
        self.schema: Optional[dict[str, Any]] = schema
        self.table: pl.DataFrame = self.create_table([])
        self.metadata: dict[str, Any] = {
            "id": minimum_id,
            "count": 0,
            "datetime": datetime.now().isoformat(),
        }

        if not os.path.isdir(self.name_d):
            os.umask(0)
            os.makedirs(self.name_d, mode=0o777, exist_ok=True)

        if os.path.exists(self.metadata_path()):
            with open(self.metadata_path(), "rb") as fp:
                self.metadata = json.load(fp)

        if self.table.is_empty():
            self.table = self.load_all()

    def create_table(self, data: list[dict[str, Any]]) -> pl.DataFrame:
        return pl.DataFrame(
            data,
            orient="row",
            strict=False,
            nan_to_null=True,
            schema_overrides=self.schema,
        )

    def next_id(self) -> int:
        self.metadata["id"] += 1
        return self.metadata["id"]

    def count(self) -> int:
        return self.table.shape[0]

    def object_path(self, id: int) -> Path:
        return self.name_d / Path(f"{id}.item.json")

    def metadata_path(self) -> Path:
        return self.name_d / Path("metadata.json")

    def load(self, id: int) -> Optional[dict[str, Any]]:
        if not self.table.is_empty():
            for item in self.table.filter(pl.col.id == id).to_dicts():
                return item

    def commit(self, items: Union[dict[str, Any], list[dict[str, Any]]]) -> bool:
        if isinstance(items, dict):
            items = [items]

        try:
            self.table = pl.concat(
                [self.table, self.create_table(items)], how="diagonal"
            )
            self.table = self.table.unique(
                subset="id", keep="last", maintain_order=True
            )
            self.metadata["count"] = self.table.shape[0]
            self.metadata["datetime"] = datetime.now().isoformat()

            for item in items:
                with open(self.object_path(item["id"]), "w+") as fp:
                    fp.write(json.dumps(item, separators=(",", ":")))

            with open(self.metadata_path(), "w+") as fp:
                return fp.write(json.dumps(self.metadata, separators=(",", ":"))) > 0
        except Exception:
            print(
                "Flex.update: Data type are incompatible! Maybe you have to define some columns as 'pl.Object' data type in your schema."
            )

        return False

    def delete(self, ident: Union[int, list[int]]) -> bool:
        if self.table.is_empty():
            return False

        if isinstance(ident, int):
            ident = [ident]

        n = self.count()
        self.table = self.table.remove(pl.col.id.is_in(ident))

        if not n > self.count():
            return False

        return len([os.remove(self.object_path(id)) for id in ident]) > 0

    def load_all(self, chunk_size: int = 100) -> pl.DataFrame:
        table: pl.DataFrame = self.create_table([])
        items: list[dict[str, Any]] = []

        for i, filename in enumerate(
            sorted(glob.glob(os.path.join(self.name_d, "*.item.json")))
        ):
            with open(filename, "rb") as fp:
                items.append(json.load(fp))

            if i % chunk_size == 0:
                table = pl.concat([table, self.create_table(items)], how="diagonal")
                items = []

        return pl.concat([table, self.create_table(items)], how="diagonal")

    class Exception(Exception):
        def __init__(self, classname: str):
            super().__init__(
                f"This object is not defined as a Flexmeta's Object! First implement: `flexmeta: Flexmeta = Flexmeta(...)` as a static variable in '{classname}' class."
            )


class Flexobject:
    flexmeta: Flexmeta

    @property
    def c(self) -> pl.expr.Expr:
        return pl.col

    @property
    def table(self) -> pl.DataFrame:
        return self.flexmeta.table

    @classmethod
    def clone(cls, item: dict[str, Any] = {}) -> "Flexobject":
        return cls().update(item)

    @classmethod
    def load(cls, id: int) -> Optional["Flexobject"]:
        if not hasattr(cls, "flexmeta"):
            raise Flexmeta.Exception(cls.__name__)

        if item := cls.flexmeta.load(id):
            return cls.clone(item)

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
        if self.is_flexmeta():
            self.id: int = self.flexmeta.next_id()

    def __str__(self) -> str:
        return self.json()

    def __getitem__(self, name: str):
        return self.takeout().get(name)

    def __setitem__(self, name: str, value: Any):
        self.update({name: value})

    def is_flexmeta(self) -> bool:
        return hasattr(self, "flexmeta")

    def fetch(self) -> "Flexobject":
        if self.is_flexmeta() and (item := self.flexmeta.load(self.id)):
            self.update(item)

        return self

    def update(self, items: dict[str, Any]) -> "Flexobject":
        def callback(item: Any, n_item: Any):
            if item is None:
                return n_item
            elif isinstance(item, Flexobject) and isinstance(n_item, dict):
                return item.clone(n_item)
            elif isinstance(item, (list, tuple)) and isinstance(n_item, (list, tuple)):
                if not len(item):
                    # item is not "type-controlled"
                    return n_item

                return [callback(item[0], v) for v in n_item]
            elif isinstance(item, dict) and isinstance(n_item, dict):
                if not len(item):
                    # item is not "type-controlled"
                    return n_item

                for k, v in [(k, v) for k, v in item.items() if k in n_item]:
                    item[k] = callback(v, n_item[k])
            elif isinstance(item, object) and isinstance(n_item, dict):
                for k, v in [(k, v) for k, v in item.__dict__.items() if k in n_item]:
                    item.__dict__[k] = callback(v, n_item[k])
            elif type(item) is type(n_item):
                return n_item

            return item

        for name, item in self.__dict__.items():
            if name in items and item != self:
                self.__dict__[name] = callback(item, items[name])

        return self

    def commit(self) -> bool:
        if not self.is_flexmeta():
            raise Flexmeta.Exception(self.__class__.__name__)

        return self.flexmeta.commit(self.takeout())

    def delete(self) -> bool:
        if not self.is_flexmeta():
            raise Flexmeta.Exception(self.__class__.__name__)

        return self.flexmeta.delete(self.id)

    def takeout(self) -> dict[str, Any]:
        def callback(name: str, item: Any):
            if isinstance(item, Flexobject):
                return item.takeout()
            elif isinstance(item, (list, tuple)):
                return [callback(name, v) for v in item]
            elif isinstance(item, dict):
                return {k: callback(name, v) for k, v in item.items()}
            elif isinstance(item, object) and hasattr(item, "__dict__"):
                return {k: callback(name, v) for k, v in item.__dict__.items()}

            return item

        return {k: callback(k, v) for k, v in self.__dict__.items()}

    def json(self, indent: Optional[int] = 4) -> str:
        return json.dumps(self.takeout(), indent=indent)

    def select(self, callback: Callable[[pl.DataFrame], pl.DataFrame]) -> "Flexselect":
        if not self.is_flexmeta():
            raise Flexmeta.Exception(self.__class__.__name__)

        return Flexselect(self, callback(self.flexmeta.table))


class Flexselect:
    def __init__(self, object: Flexobject, table: pl.DataFrame) -> None:
        self.object: Flexobject = object
        self.items: list[dict[str, Any]] = table.to_dicts()

    def __len__(self) -> int:
        return len(self.items)

    def __iter__(self) -> Generator[Flexobject, Any, None]:
        for data in self.items:
            yield self.object.clone(data)

    def count(self) -> int:
        return len(self.items)

    def map(self, callback: Callable[[dict[str, Any]], dict[str, Any]]):
        self.items = list(map(callback, self.items))

    def head(
        self,
        limit: int = 10,
        callback: Optional[Callable[[Flexobject], Flexobject]] = None,
    ) -> list[Flexobject]:
        items = list(map(self.object.clone, self.items[0:limit]))

        if callable(callback):
            return list(map(callback, items))

        return items

    def tail(
        self,
        limit: int = 10,
        callback: Optional[Callable[[Flexobject], Flexobject]] = None,
    ) -> list[Flexobject]:
        items = list(map(self.object.clone, self.items[-limit:]))

        if callable(callback):
            return list(map(callback, items))

        return items

    def fetch_one(
        self, callback: Optional[Callable[[Flexobject], Flexobject]] = None
    ) -> Optional[Flexobject]:
        for data in self.items:
            if callable(callback):
                return callback(self.object.clone(data))

            return self.object.clone(data)

    def fetch_all(
        self,
        page: int = -1,
        limit: int = 10,
        callback: Optional[Callable[[Flexobject], Flexobject]] = None,
    ) -> list[Flexobject]:
        if page < 1 or limit < 1:
            items = self.items
        else:
            paging = abs((page * limit) - limit)
            items = self.items[paging : paging + limit]

        items = list(map(self.object.clone, items))

        if callable(callback):
            return list(map(callback, items))

        return items
