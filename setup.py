import re
import os
import sys
from setuptools import setup, find_packages

PY_VER = sys.version_info
if PY_VER < (3, 6):
    raise RuntimeError("trq doesn't support Python version prior 3.6")

install_requires = list(x.strip() for x in open('requirements.txt'))
tests_require = [
    'pytest',
    'pytest-asyncio',
    'testing.redis'
]

def read_version():
    regexp = re.compile(r"^__version__\W*=\W*'([\d.abrc]+)'")
    init_py = os.path.join(os.path.dirname(__file__),
                           'trq', '__init__.py')
    with open(init_py) as f:
        for line in f:
            match = regexp.match(line)
            if match is not None:
                return match.group(1)
        else:
            raise RuntimeError('Cannot find version in trq/__init__.py')

setup(name='trq',
      version=read_version(),
      description=("Simple asynio based redis task queue"),
      platforms=["POSIX"],
      author="Tristan King",
      author_email="mail@tristan.sh",
      url="https://github.com/tristan/trq",
      license="MIT",
      setup_requires=['pytest-runner'],
      packages=find_packages(exclude=["test"]),
      install_requires=install_requires,
      include_package_data=True,
      tests_require=tests_require
      )
