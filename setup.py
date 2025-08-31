import os
import sys
from shutil import rmtree

from setuptools import setup, Command

VERSION = "0.1.0"
DESCRIPTION = "Flex with Polars"
PWD = os.path.abspath(os.path.dirname(__file__))

# Import the README and use it as the long-description.
# Note: this will only work if 'README.md' is present in your MANIFEST.in file!
try:
    with open(os.path.join(PWD, "README.md"), encoding="utf-8", mode="rb") as f:
        long_description = "\n" + f.read()
except FileNotFoundError:
    long_description = DESCRIPTION


class UploadCommand(Command):
    """Support setup.py upload."""

    description = "Build and publish the package."
    user_options = []

    @staticmethod
    def status(s):
        """Prints things in bold."""
        print("\033[1m{0}\033[0m".format(s))

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        try:
            self.status("Removing previous builds…")
            rmtree(os.path.join(PWD, "dist"))
        except OSError:
            pass

        self.status("Building Source and Wheel (universal) distribution…")
        os.system("{0} setup.py sdist bdist_wheel --universal".format(sys.executable))

        self.status("Uploading the package to PyPI via Twine…")
        os.system("twine upload dist/*")

        self.status("Pushing git tags…")
        os.system("git tag v{0}".format(VERSION))
        os.system("git push --tags")

        sys.exit()


setup(
    name="flex",
    version=VERSION,
    url="https://github.com/zinosaure/flex-polars",
    description=DESCRIPTION,
    long_description=long_description,
    long_description_content_type="text/markdown",
    python_requires=">3.12",
    py_modules=["src/package"],
    install_requires=[
        "polars",
    ],
    extras_require={},
    include_package_data=True,
    cmdclass={
        "upload": UploadCommand,
    },
)
