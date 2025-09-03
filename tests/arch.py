import os
import json
import polars as pl

from pathlib import Path
from datetime import datetime
from typing import Any, Generator, Optional, Callable, TypeAlias, Union


def protect(*protected):
    """
    Returns a metaclass that protects all objects given as strings
    """

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


class Flex:
    FLEXSTORE_PATH: Path = Path("/app/src/flexstore")

    @property
    def table(self) -> pl.DataFrame:
        return self.__table

    @property
    def schema(self) -> Optional[dict[str, Any]]:
        return self.__schema

    @property
    def metadata(self) -> dict[str, Any]:
        return self.__metadata

    def __init__(self, name: str, id: int = 0, schema: Optional[dict[str, Any]] = None):
        name = os.path.basename(name.replace("/", "_").lower())
        self.__table: pl.DataFrame = pl.DataFrame(
            [],
            schema_overrides=schema,
            orient="row",
            strict=True,
            nan_to_null=True,
        )
        self.__schema: Optional[dict[str, Any]] = schema
        self.__metadata: dict[str, Any] = {
            "id": id,
            "datatime": datetime.now().isoformat(),
        }
        self.__items_fn: Path = Flex.FLEXSTORE_PATH / Path(f"{name}.items.ndjson")
        self.__metadata_fn: Path = Flex.FLEXSTORE_PATH / Path(f"{name}.metadata.json")

        if os.path.exists(self.__items_fn):
            try:
                self.__table = pl.read_ndjson(
                    self.__items_fn,
                    schema_overrides=self.__schema,
                    low_memory=True,
                    ignore_errors=False,
                )
            except Exception as e:
                print("Flex.__init__:", str(e))

            if os.path.exists(self.__metadata_fn):
                with open(self.__metadata_fn, "rb") as fp:
                    self.__metadata = json.load(fp)
        else:
            if not os.path.isdir(Flex.FLEXSTORE_PATH):
                os.umask(0)
                os.makedirs(Flex.FLEXSTORE_PATH, mode=0o777, exist_ok=True)

            self.save_database()

    def next_id(self) -> int:
        return self.__metadata["id"] + 1

    def count(self) -> int:
        return self.__table.shape[0]

    def load(self, id: int) -> Optional[dict[str, Any]]:
        if not self.__table.is_empty():
            for items in self.__table.filter(pl.col.id == id).to_dicts():
                return items

    def commit(self, items: Union[dict[str, Any], list[dict[str, Any]]]) -> bool:
        if isinstance(items, dict):
            items = [items]

        table: pl.DataFrame = pl.DataFrame(
            items,
            schema_overrides=self.__schema,
            orient="row",
            strict=True,
            nan_to_null=True,
        )

        try:
            self.__table = pl.concat([self.__table, table], how="diagonal")
            self.__table = self.__table.unique(
                subset=["id"], keep="last", maintain_order=True
            )

            return self.save_database()
        except Exception as e:
            print("Flex.commit:", str(e))
            return False

    def delete(self, ident: Union[int, list[int]]) -> bool:
        if self.__table.is_empty():
            return False

        n = self.__table.shape[0]

        if not isinstance(ident, list):
            self.__table = self.__table.remove(pl.col.id == ident)
        else:
            self.__table = self.__table.remove(pl.col.id.is_in(ident))

        if n > self.__table.shape[0]:
            return self.save_database()

        return False

    def batch_commit(self, data: list["Model"]) -> bool:
        if not len(data):
            return False

        return self.commit(
            [model.prop.takeout(custom=True) for model in data if model.flex == self]
        )

    def batch_delete(self, data: list["Model"]) -> bool:
        if not len(data):
            return False

        return self.delete([model.id for model in data if model.flex == self])

    def save_database(self) -> bool:
        try:
            self.__table.write_ndjson(self.__items_fn)
            metadata = {
                "id": self.next_id(),
                "datatime": datetime.now().isoformat(),
            }
            with open(self.__metadata_fn, "w+") as fp:
                return fp.write(json.dumps(metadata)) > 0
        except Exception as e:
            print("Flex.save_database:", str(e))
            return False


class T:
    @classmethod
    def clone(cls, props: dict[str, Any] = {}) -> "T":
        return cls()

    def custom_update(self, items: dict[str, Any]) -> dict[str, Any]:
        return items

    def custom_takeout(self, items: dict[str, Any]) -> dict[str, Any]:
        return items


class Property(T):
    RESERVED_KEYWORDS: list[str] = ["flex", "prop", "__prop", "__object"]

    @property
    def prop(self) -> "Property":
        return self

    def __init__(self, model: T) -> None:
        self.__object: T = model

    def __str__(self) -> str:
        return self.jsonify()

    def __setitem__(self, name: str, value: str):
        if name not in self.__object.__dict__:
            return

        item = self.__object.__dict__[name]

        if type(item) is type(value):
            item = value
        elif isinstance(value, dict):
            if isinstance(item, (Property, Model)):
                item.prop.update(value)
            elif isinstance(item, object):
                item.__dict__.update(**value)
        else:
            return

        self.__object.__dict__[name] = item

    def update(self, items: dict[str, Any]) -> T:
        items = self.__object.custom_update(items)

        for name, _ in self.__object.__dict__.items():
            if name not in self.RESERVED_KEYWORDS and name in items:
                self.__setitem__(name, items[name])

        return self.__object

    def takeout(self, custom: bool = True) -> dict[str, Any]:
        items: dict[str, Any] = {}

        if custom:
            items = self.__object.custom_takeout(items)

        for name, item in self.__object.__dict__.items():
            if name not in self.RESERVED_KEYWORDS and item != self:
                if isinstance(item, dict):
                    items[name] = item
                elif isinstance(item, (Property, Model)):
                    items[name] = item.prop.takeout(custom)
                elif isinstance(item, object) and hasattr(item, "__dict__"):
                    items[name] = item.__dict__
                else:
                    items[name] = item

        return items

    def jsonify(self, indent: Optional[int] = 4) -> str:
        return json.dumps(self.takeout(custom=True), indent=indent)


class Select:
    def __init__(self, model: "Model", table: pl.DataFrame) -> None:
        self.__model: Model = model
        self.__items: list[dict[str, Any]] = table.to_dicts()

    def fetch_one(self) -> Optional["Model"]:
        return


class MMC(type):
    def __call__(cls, *args, **kwargs):
        obj = type.__call__(cls, *args, **kwargs)
        obj.__postinit__()

        return obj


class Model(T, metaclass=MMC):
    flex: Flex

    @property
    def prop(self) -> Property:
        return self.__prop

    @classmethod
    def load(cls, id: int) -> Optional["T"]:
        if props := cls.flex.load(id):
            return cls.clone(props)

    def __init__(self):
        self.__prop: Property = Property(self)
        self.id: int = self.flex.next_id()

    def __postinit__(self):
        if not hasattr(self, "flex"):
            raise Exception('Please add "super().__init__()" in "__init__()"!')

    def __str__(self) -> str:
        return self.__prop.jsonify()

    def commit(self) -> bool:
        return self.flex.commit(self.prop.takeout(custom=True))

    def delete(self) -> bool:
        return self.flex.delete(self.id)

    def select(self, callback: Callable[[pl.DataFrame], pl.DataFrame]) -> Select:
        return Select(self, callback(self.flex.table))


Flex.FLEXSTORE_PATH = Path("/app/tests/flexstore")


class P(Property):
    def __init__(self):
        super().__init__(self)
        self.uniqid: str = "252 6545"


class M(Model):
    flex: Flex = Flex("users")

    def __init__(self):
        super().__init__()
        self.name = "John Doe"
        self.phone = "0601363265"
        self.mymodel: P = P()


def work_with_df(table: pl.DataFrame) -> pl.DataFrame:
    return table


m = M()
p = P()
m.commit()

# p.clone({})
# m.clone({})
# m.load(1)
# m.select(work_with_df).fetch_one()
# m.flex.load(1)
# m.flex.commit([{}, {}])
# m.flex.delete(1)
# m.flex.delete([1, 2])
# m.prop.takeout()
# p.prop.takeout()
# m.prop.update({})
# p.prop.update({})
