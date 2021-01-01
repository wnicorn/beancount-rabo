import os
from setuptools import setup

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

version = '0.1.0'

setup(
    name='beancountRABO',
    version=version,
    author='elisevanwijngaarden',
    author_email='elise_13@hotmail.no',
    description='Converts RABO .csv to Beancount',
    long_description=read('README.md'),
    url='https://github.com/wnicorn/beancount-rabo',
    license='GPLv2',
    packages=['beancountRABO']
    )
