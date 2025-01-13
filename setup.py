# Copyright (C) 2022 Dremio Corporation
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/usr/bin/env python
from pathlib import Path
from setuptools import find_namespace_packages, setup


# pull the long description from the README
README = Path(__file__).parent / "README.md"


README

package_name = "dbt-dremio"

package_version = "1.8.1"

description = """The Dremio adapter plugin for dbt"""

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=README.read_text(),
    license="Apache Software License 2.0 (http://www.apache.org/licenses/LICENSE-2.0)",
    author="Dremio",
    url="https://github.com/dremio/dbt-dremio",
    packages=find_namespace_packages(include=["dbt", "dbt.*"]),
    include_package_data=True,
    install_requires=[
        "dbt-core>=1.8",
        "dbt-adapters>=1.0.0, <2.0.0",
        "requests>=2.31.0",
    ],
    classifiers=[
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
)
