from setuptools import setup, find_packages

setup(
    name='colin_utility',
    version='1.0.0',
    description="常用工具集",
    author="colin chang",
    packages=find_packages(),
    install_requires=[
        'oss2',
        'redis',
        'pymysql'
    ]
)
