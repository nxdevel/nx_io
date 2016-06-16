import pathlib
from setuptools import setup, find_packages

name = 'nx_io'
description = 'Miscellaneous I/O utilities'

here = pathlib.Path(__file__).resolve().parent
with (here / name / 'version.txt').open() as fp:
    version = fp.read().strip()

with (here / 'README.rst').open() as fp:
    long_description = fp.read()

setup(name=name,
      version=version,
      description=description,
      long_description=long_description,
      url='https://github.com/nxdevel/nx_io',
      author='A Bradford',
      author_email='nxdevel@users.noreply.github.com',
      license='MIT',
      classifiers=[
          'Development Status :: 5 - Production/Stable',
          'License :: OSI Approved :: MIT License',
          'Programming Language :: Python :: 3 :: Only',
          'Topic :: Software Development :: Libraries',
          'Topic :: Utilities',
          'Intended Audience :: Developers'],
      keywords='development utilities io readline',
      packages=find_packages(),
      package_data={
          name: ['version.txt']},
      setup_requires=['pytest-runner'],
      tests_require=['pytest'])
