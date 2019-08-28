#!/usr/bin/env python
from setuptools import setup, find_packages

install_requires = [
    'click',
    'boto3',
    'requests'
]

setup(
    name='antenna',
    version='0.1.8',
    description='Cloud Scraping System',
    author='Morgan McDermott',
    author_email='morganmcdermott@gmail.com',
    long_description='',
    url='https://github.com/mmcdermo/antenna',
    license='All rights reserved',
    zip_safe=False,
    keywords='',
    packages=find_packages(),
    install_requires=install_requires,
    entry_points={
        'console_scripts': [
            'antenna = antenna.cli:main',
        ]
    },
    package_data={'': ['lambda_env/*']},
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 2.7',
    ]
)
