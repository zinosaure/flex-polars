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


class Flexmeta:
    def __init__(
        self,
        name: str,
        minimum_id: int = 0,
        schema: Optional[dict[str, Any]] = None,
    ):
        name = os.path.basename(name.replace("/", "_").lower())
        self.__uniqid: str = str(uuid.uuid5(uuid.NAMESPACE_OID, name))
        self.__metadata: dict[str, Any] = {
            "id": minimum_id,
            "count": 0,
            "datetime": datetime.now().isoformat(),
        }
        self.__schema: Optional[dict[str, Any]] = schema
        self.__table: pl.DataFrame = self.DataFrame([])
        self.__filename: Path = Path(Flex.FLEXSTORE_PATH / Path(f"{name}.json"))

        if not os.path.isdir(Flex.FLEXSTORE_PATH):
            os.umask(0)
            os.makedirs(Flex.FLEXSTORE_PATH, mode=0o777, exist_ok=True)

        if self.__uniqid in Flex.REGISTERED_FLEXMETA:
            flexmeta = Flex.REGISTERED_FLEXMETA[self.__uniqid]
            self.__metadata = flexmeta.content["metadata"]
            self.__table = self.DataFrame(flexmeta.content["items"])
        else:
            if os.path.exists(self.__filename):
                self.__load_state()
            else:
                self.__save_state()

        Flex.REGISTERED_FLEXMETA[self.__uniqid] = self

    @property
    def uniqid(self) -> str:
        return self.__uniqid

    @property
    def metadata(self) -> dict[str, Any]:
        return self.__metadata

    @property
    def table(self) -> pl.DataFrame:
        return self.__table.clone()

    @property
    def schema(self) -> Optional[dict[str, Any]]:
        return self.__schema

    @property
    def content(self) -> dict[str, Any]:
        return {
            "metadata": {
                "id": self.next_id(),
                "count": self.count(),
                "datetime": datetime.now().isoformat(),
            },
            "items": self.__table.to_dicts(),
        }

    def DataFrame(self, data: list[dict[str, Any]]) -> pl.DataFrame:
        return pl.DataFrame(
            data,
            schema_overrides=self.__schema,
            nan_to_null=False,
        )

    def next_id(self) -> int:
        return self.__metadata["id"] + 1

    def count(self) -> int:
        return self.__table.shape[0]

    def load(self, id: int) -> Optional[dict[str, Any]]:
        if not self.__table.is_empty():
            for props in self.__table.filter(pl.col.id == id).to_dicts():
                return props

    def update(self, values: dict[str, Any] | list[dict[str, Any]]) -> bool:
        if isinstance(values, list):
            table = self.DataFrame(values)
        else:
            table = self.DataFrame([values])

        try:
            self.__table = pl.concat([self.__table, table], how="diagonal")
            self.__table = self.__table.unique(
                subset=["id"], keep="last", maintain_order=True
            )

            return self.__save_state()
        except Exception as e:
            print("Flexmeta.append:", str(e))
            return False

    def delete(self, ident: int | list[int]) -> bool:
        if self.__table.is_empty():
            return False

        n = self.__table.shape[0]

        if not isinstance(ident, list):
            self.__table = self.__table.remove(pl.col.id == ident)
        else:
            self.__table = self.__table.remove(pl.col.id.is_in(ident))

        if n > self.__table.shape[0]:
            return self.__save_state()

        return False

    def __load_state(self) -> bool:
        try:
            with open(self.__filename, "rb") as handler:
                data = json.load(handler)
                self.__metadata = data["metadata"]
                self.__table = self.DataFrame(data["items"])
            return True
        except Exception as e:
            print("Flexmeta.__load_state:", str(e))

        return False

    def __save_state(self) -> bool:
        try:
            data = json.dumps(self.content)
        except Exception as e:
            print("Flexmeta.__save_state:", str(e))
            return False

        with open(self.__filename, "w+") as handler:
            return handler.write(data) > 0


protected_methods = [
    "uniqid",
    "c",
    "column",
    "table",
    "schema",
    "flexmeta",
    "_load",
    "batch_commit",
    "batch_delete",
    "clone",
    "select",
    "commit",
    "delete",
    "to_dict",
    "to_json",
]


class Flextable(metaclass=protect(*protected_methods)):
    DataFrame: TypeAlias = pl.DataFrame

    def __init__(self, flexmeta: Flexmeta):
        self.__flexmeta_uniqid__: str = flexmeta.uniqid
        self.id: int = flexmeta.next_id()

    @property
    def uniqid(self) -> str:
        return self.__flexmeta_uniqid__

    @property
    def c(self) -> pl.expr.Expr:
        return pl.col

    @property
    def column(self) -> pl.expr.Expr:
        return pl.col

    @property
    def flexmeta(self) -> Flexmeta:
        return Flex.REGISTERED_FLEXMETA[self.__flexmeta_uniqid__]

    @property
    def table(self) -> pl.DataFrame:
        return self.flexmeta.table

    @property
    def schema(self) -> Optional[dict[str, Any]]:
        return self.flexmeta.schema

    @staticmethod
    def _load(flextable: "Flextable", id: int) -> Optional["Flextable"]:
        if props := flextable.flexmeta.load(id):
            return flextable.clone(props, flextable)

    @staticmethod
    def load(id: int) -> Optional["Flextable"]:
        raise NotImplementedError(
            'Static method "load(id: int) -> Optional["Flextable"]" is not yet implemented!'
        )

    @staticmethod
    def batch_commit(data: list["Flextable"]) -> bool:
        if not len(data):
            return False

        return data[0].flexmeta.update(
            [flextable.to_dict(decompose=True) for flextable in data]
        )

    @staticmethod
    def batch_delete(data: list["Flextable"]) -> bool:
        if not len(data):
            return False

        return data[0].flexmeta.delete([flextable.id for flextable in data])

    def clone(
        self, props: dict[str, Any] = {}, flextable: Optional["Flextable"] = None
    ) -> "Flextable":
        if not flextable:
            flextable = type(self)()  # type: ignore

        if props and isinstance(props, dict):
            for k, v in [(k, v) for k, v in self.__dict__.items() if k in props]:
                if k == "__flexmeta_uniqid__":
                    continue

                if isinstance(v, Flextable) and v != self:
                    setattr(flextable, k, v.clone(props[k]))
                else:
                    setattr(flextable, k, flextable.on_compose(k, props[k]))

        return flextable

    def select(self, callback: Callable[[DataFrame], DataFrame]) -> "Flexselect":
        return Flexselect(callback(self.table), self)

    def commit(self) -> bool:
        return self.flexmeta.update(self.to_dict(decompose=True))

    def delete(self) -> bool:
        return self.flexmeta.delete(self.id)

    def to_dict(self, decompose: bool = False) -> dict[str, Any]:
        props: dict[str, Any] = {}

        for k, v in self.__dict__.items():
            if k == "__flexmeta_uniqid__":
                continue

            if isinstance(v, Flextable) and v != self:
                props[k] = v.to_dict(decompose)
            elif hasattr(v, "to_dict") and callable(getattr(v, "to_dict")):
                props[k] = v.to_dict()
            elif hasattr(v, "__dict__") and v != self:
                props[k] = v.__dict__
            else:
                props[k] = v

        if decompose:
            props = {k: self.on_decompose(k, v) for k, v in props.items()}

        return props

    def to_json(self, indent: Optional[int] = 4) -> str:
        return json.dumps(self.to_dict(decompose=True), indent=indent)

    def on_compose(self, name: str, value: Any) -> Any:
        return value

    def on_decompose(self, name: str, value: Any) -> Any:
        return value


class Flexselect:
    Callback: TypeAlias = Optional[Callable[[Flextable], Flextable]]

    def __init__(self, table: pl.DataFrame, flextable: Flextable):
        self.__items: list[dict[str, Any]] = table.to_dicts()
        self.__flextable: Flextable = flextable

    def __len__(self) -> int:
        return len(self.__items)

    def __iter__(self) -> Generator[Flextable, Any, None]:
        for item in self.__items:
            yield self.__flextable.clone(item)

    def count(self) -> int:
        return len(self.__items)

    def map(self, callback: Callable[[dict[str, Any]], dict[str, Any]]):
        self.__items = list(map(callback, self.__items))

    def top(self, limit: int = 10, callback: Callback = None) -> list[Flextable]:
        items = self.__items[0:limit]
        items = list(map(self.__flextable.clone, items))

        if callable(callback):
            return list(map(callback, items))

        return items

    def tail(self, limit: int = 10, callback: Callback = None) -> list[Flextable]:
        items = self.__items[-limit:]
        items = list(map(self.__flextable.clone, items))

        if callable(callback):
            return list(map(callback, items))

        return items

    def fetch_one(self, callback: Callback = None) -> Optional[Flextable]:
        for item in self.__items:
            if callable(callback):
                return callback(self.__flextable.clone(item))

            return self.__flextable.clone(item)

    def fetch_all(
        self, page: int = -1, limit: int = 10, callback: Callback = None
    ) -> list[Flextable]:
        if page < 1 or limit < 1:
            items = self.__items
        else:
            paging = abs((page * limit) - limit)
            items = self.__items[paging : paging + limit]

        items = list(map(self.__flextable.clone, items))

        if callable(callback):
            return list(map(callback, items))

        return items


class Flex(object):
    FLEXSTORE_PATH: Path = Path("/app/src/flexstore")
    REGISTERED_FLEXMETA: dict[str, "Flexmeta"] = {}

    Pl: TypeAlias = pl
    DataFrame: TypeAlias = pl.DataFrame

    class Flexobject:
        def __init__(self, attributes: dict[str, Any] = {}):
            for k, v in attributes.items():
                if isinstance(v, (list, tuple)):
                    setattr(
                        self,
                        k,
                        [Flex.Flexobject(x) if isinstance(x, dict) else x for x in v],
                    )
                else:
                    setattr(self, k, Flex.Flexobject(v) if isinstance(v, dict) else v)

    class Flexmeta(Flexmeta):
        pass

    class Flextable(Flextable):
        pass

    class Flexselect(Flexselect):
        pass
