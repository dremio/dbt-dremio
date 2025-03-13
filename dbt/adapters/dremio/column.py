from dataclasses import dataclass
#from typing import TypeVar

from dbt.adapters.base.column import Column

#Self = TypeVar("Self", bound="DremioColumn")

@dataclass(init=False)
class DremioColumn(Column):
    TYPE_LABELS = {
        "TEXT": "VARCHAR",
        "STRING": "VARCHAR",
    }
