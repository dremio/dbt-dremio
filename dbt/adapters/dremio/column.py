from dataclasses import dataclass

from dbt.adapters.base.column import Column

@dataclass(init=False)
class DremioColumn(Column):
    TYPE_LABELS = {
        "TEXT": "VARCHAR",
        "STRING": "VARCHAR",
    }
