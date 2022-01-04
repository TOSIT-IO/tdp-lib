from setuptools import setup

setup(
    name='TDP Lib',
    version='0.0.1',
    install_requires=[
        'pytest==6.2.5',
        'ansible==2.9.27',
        'networkx==2.5.1',
        'PyYAML==6.0',
    ],
    packages=[
        'tdp.core',
    ],
    package_data={
    }
)
