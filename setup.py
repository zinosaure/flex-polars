from setuptools import setup

setup(
    name="Flex/Polars",
    version="0.1.0",
    url="https://github.com/zinosaure/flex-polars",
    description="Flex with Polars",
    python_requires=">3.12",
    py_modules=["src/package"],
    install_requires=["polars"],
)
