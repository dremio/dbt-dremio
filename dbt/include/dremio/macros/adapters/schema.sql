/*Copyright (C) 2022 Dremio Corporation 
Licensed under the Apache License, Version 2.0 (the "License"); 
you may not use this file except in compliance with the License. 
You may obtain a copy of the License at 
http://www.apache.org/licenses/LICENSE-2.0 
Unless required by applicable law or agreed to in writing, software 
distributed under the License is distributed on an "AS IS" BASIS, 
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
See the License for the specific language governing permissions and 
limitations under the License.*/

{% macro dremio__create_schema(relation) -%}
  {{ log('create_schema macro (' + relation.render() + ') not implemented yet for adapter ' + adapter.type(), info=True) }}
{% endmacro %}

{% macro dremio__drop_schema(relation) -%}
{{ exceptions.raise_not_implemented(
  'drop_schema macro not implemented for adapter '+adapter.type()) }}
{% endmacro %}
