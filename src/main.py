from pathlib import Path
from flex import Flexobject, Flexmeta


Flexmeta.setup(Path("/app/src/flexstore"))


class C(Flexobject):
    flexmeta: Flexmeta = Flexmeta("testdc")

    def __init__(self):
        super().__init__()
        self.z = "z"


class N:
    def __init__(self):
        self.a = "a"
        self.b = "b"
        self.c = C()
        self.d = [C(), C()]


class Yz(Flexobject):
    def __init__(self):
        self.intval = 1
        self.strval = "Hello"
        self.listval = [N(), N()]


class Dc(Flexobject):
    flexmeta: Flexmeta = Flexmeta("testdc")

    def __init__(self):
        super().__init__()
        self.intval = 1
        self.strval = "World"
        self.yzval = Yz()


class Ab(Flexobject):
    flexmeta: Flexmeta = Flexmeta("tests")

    def __init__(self):
        super().__init__()
        self.floatval = 120.10
        self.yzval = Yz()
        self.listval = [Yz(), Yz()]
        self.dictval = {"y": 1523, "z": 9652.002, "a": Dc()}


# ab = Ab()
# print(ab.commit())

if ab := Ab.load(2):
    print(ab.dictval["a"].yzval.listval)
