#!/usr/bin/env python
from setuptools import find_packages
from setuptools import setup

package_name = "dbt-dremio"
package_version = "1.0.6.5"
description = """The dremio adapter plugin for dbt (data build tool)"""

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=description,
    author="Fabrice Etanchaud",
    author_email="fabrice.etanchaud@netc.fr",
    url="https://github.com/fabrice-etanchaud/dbt-dremio",
    packages=find_packages(),
    package_data={
        'dbt': [
            'include/dremio/macros/*.sql',
            'include/dremio/macros/**/*.sql',
            'include/dremio/macros/**/**/*.sql',
            'include/dremio/dbt_project.yml',
        ]
    },
    install_requires=[
        'dbt-core==1.2.2',
        'pyodbc>=4.0.27',
    ]
)
