from setuptools import setup, find_packages
import os

with open("requirements.txt") as f:
    requirements = f.read()

setup(
    name='pydatabase',
    version='v1.0.0',
    requirements=requirements,
    packages=find_packages(),
    url='',
    license='',
    author='José LOPEZ',
    author_email='',
    description=''
)
