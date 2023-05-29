#!/usr/bin/env python

from distutils.core import setup

version = '1.3'

long_description = "Сache module for batch loading of data"

setup(name='idbadapter',
      version='1.4',
      description='Сache module for batch loading of data',
      long_description=long_description,
      url="https://github.com/AnatolyPershinov/gpn_cache_module",
      download_url='https://github.com/AnatolyPershinov/gpn_cache_module/archive/master.zip',
      author='Anatoly Pershinov',
      author_email='anatoliypershinov@gmail.com',
      packages=['idbadapter'],
      install_requires=['requests', 'pandas'],
      classifiers=[
            'License :: OSI Approved :: Apache Software License',
            'Operating System :: OS Independent',
            'Intended Audience :: End Users/Desktop',
            'Intended Audience :: Developers',
            'Programming Language :: Python',
            'Programming Language :: Python :: 3',
            ]
      )