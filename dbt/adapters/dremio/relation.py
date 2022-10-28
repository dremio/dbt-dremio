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

from dataclasses import dataclass
from dbt.adapters.base.relation import BaseRelation, Policy, ComponentName, RelationType
from typing import Optional, Tuple, Iterator


@dataclass
class DremioQuotePolicy(Policy):
    database: bool = True
    schema: bool = True
    identifier: bool = True


@dataclass
class DremioIncludePolicy(Policy):
    database: bool = True
    schema: bool = True
    identifier: bool = True


@dataclass(frozen=True, eq=False, repr=False)
class DremioRelation(BaseRelation):
    quote_policy: DremioQuotePolicy = DremioQuotePolicy()
    include_policy: DremioIncludePolicy = DremioIncludePolicy()
    no_schema = "no_schema"
    format: Optional[str] = None
    format_clause: Optional[str] = None

    def quoted_by_component(self, identifier, componentName):
        if componentName == ComponentName.Schema:
            return ".".join(self.quoted(folder) for folder in identifier.split("."))
        else:
            return self.quoted(identifier)

    def render(self) -> str:
        rendered = super().render()
        if self.format is not None and self.format_clause is not None:
            rendered = "".join(("table( ", rendered, " ( ", self.format_clause, " ) )"))
        return rendered

    def __post_init__(self):
        if self.path.schema is None:
            self.path.schema = DremioRelation.no_schema

    def _render_iterator(
        self,
    ) -> Iterator[Tuple[Optional[ComponentName], Optional[str]]]:

        for key in ComponentName:
            path_part: Optional[str] = None
            if self.include_policy.get_part(key):
                tmp_path_part = self.path.get_part(key)
                if not (
                    key == ComponentName.Schema and tmp_path_part == self.no_schema
                ):
                    path_part = tmp_path_part
                if path_part is not None and (
                    self.quote_policy.get_part(key)
                ):  # or key == ComponentName.Schema):
                    path_part = self.quoted_by_component(path_part, key)
            yield key, path_part
