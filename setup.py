#!/usr/bin/env python
from setuptools import find_namespace_packages, setup

package_name = "dbt-dremio"
# make sure this always matches dbt/adapters/{adapter}/__version__.py
package_version = "1.2.2"
description = """The DremioAdapter plugin for dbt"""

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=description,
    author="Dremio",
    author_email="dremio@dremio.com",
    url="https://github.com/dremio/dbt-dremio",
    packages=find_namespace_packages(include=["dbt", "dbt.*"]),
    include_package_data=True,
    install_requires=[
        'dbt-core~=1.2.0',
        'pyodbc>=4.0.27',
    ]
)
