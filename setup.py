#!/usr/bin/env python
from setuptools import find_namespace_packages, setup

package_name = "dbt-dremio"
package_version = "1.1.0b_odbc"
description = """The Dremio adapter plugin for dbt"""

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=description,
    author="Dremio",
    url="https://github.com/dremio/dbt-dremio",
    packages=find_namespace_packages(include=["dbt", "dbt.*"]),
    include_package_data=True,
    install_requires=[
        'dbt-core==1.1.2',
        'pyodbc>=4.0.27',
    ]
)
