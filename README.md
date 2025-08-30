# Flex

Flex is an ORM-style library without database connections. It's ideally designed to help quickly set up small projects, demos/POCs, or for data mining.

You can perform searches like this:

```
select = MyClass().select()
select.where(select.string_attribute.contains("xxx"))
select.where(select.int_attribute > 10)
```

The advantage of using Flex is that only class definitions are required to have persistent data. The structure of a class can change at any time, and this will not impact data already stored.

_However, to respect attribute typing and make sure you import the classes correctly beforehand to avoid generating errors._

> [CAUTION]\
> Flex can execute arbitrary Python code. Please see the [Security](#security) section for more details.

Bug reports and merge requests are encouraged at the Flex repository on github.

## How Flex works:

Only the class attributes are stored locally with Pickle during serialization. For deserialization, the data is reinjected into the corresponding attributes.

> Regarding searching, the data *is stored in RAM in dict format*. To avoid slowness or crashes, do not search a large amount of data at once. Consider splitting the data [see example](examples/large_data.py)

## Good to know?

- The ID and a UUID are generated automatically.
- You can decide how to serialiaze/deserialize the data.
- Comparison operations can be performed on attributes and methods.
- It is possible to use the "string dot notation" to access attributes.


## Usage

The following is a very simple example of how one can use Flex in their scripts/projects.

```
from pathlib import Path
from typing import Optional
from datetime import datetime
from app.libs.flex import Flexmeta, Flextable

CURRENT_YEAR = datetime.now().year


class Contact:
    def __init__(self):
        self.mail: str = ""


class Person(Flextable):
    def __init__(self):
        super().__init__(Flexmeta(self, "persons", 10000, 100))
        self.name: str = ""
        self.birth_year: int = 0
        self.contact: Contact = Contact()

    @staticmethod
    def load(selected_id: int) -> Optional["Flextable"]:
        return Flextable._load(Person(), selected_id)

    def actual_age(self) -> int:
        return CURRENT_YEAR - self.birth_year


Flexmeta.setPath(Path("./src")) # important if you are not using docker working directory: /app/src
person = Person()
person.name = "Juan Green"
person.birth_year = 2002
person.contact.mail = "juan.green@yahoo.com"
person.commit()

person = Person()
person.name = "Juan Mann"
person.birth_year = 2012
person.contact.mail = "jmann@hotmail.com"
person.commit()

person = Person()
person.name = "Mary Alvarez"
person.birth_year = 1998
person.contact.mail = "alvarez.mary@gmail.com"
person.commit()

persons = person.select()

print("Person count (before):", persons.count())

persons.where(persons.id >= 1)
persons.where(persons.name.contains("juan"))
# string dot notation - accessing "contact.name" attribute
persons.where(persons["contact.mail"].not_suffix("@gmail.com"))
# comparaison with method - persons["actual_age"]() >= 18 is also possible
persons.where(persons.actual_age() >= 18)

print("Person count (after):", persons.count())

for person in persons.fetch_all():
    print(person.to_json(indent=4))
```

See the [examples](examples) directory on GitHub for example scripts. These can be run on docker to see how Flex works and behaves, and how to use it. Your contributions are most welcome!


# Install

Install from github for the latest changes:

```
pip install git+https://github.com/zinosaure/flex.git
```

# Security

Flex should be treated the same as the Python stdlib pickle module from a security perspective.

- Only unpickle data you trust.

- It is possible to construct malicious pickle data which will execute arbitrary code during unpickling. Never unpickle data that could have come from an untrusted source, or that could have been tampered with.

- Consider signing data with an HMAC if you need to ensure that it has not been tampered with.