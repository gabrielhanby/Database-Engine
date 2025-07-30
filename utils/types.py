# utils/types.py

from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional

# --- literals for operators and logic ---
Operator      = Literal["begins", "ends", "contains", "equals"]
Logic         = Literal["and", "or", "nand", "nor"]
SplitPosition = Literal["before", "after", "none"]

# --- flat filter with grouping and formatting hints ---
@dataclass
class FlatFilter:
    table: str                  = "*"      # "*" ⇒ all tables
    field: str                  = "*"      # "*" ⇒ all columns
    operator: Operator          = "contains"
    value: Any                  = None
    logic: Logic                = "and"    # default chain
    index_by: Optional[str]     = None     # delimiter for formatting
    position: SplitPosition     = "none"   # how to keep delimiter
    group: int                  = 1        # default filter group

# --- how to combine multiple groups ---
@dataclass
class GroupLogic:
    groups: List[int]           # e.g. [1,2]
    logic: Logic                # "and", "or", "nand", "nor"

# --- the package you pass into search() ---
@dataclass
class SearchPackageFlat:
    filters: List[FlatFilter]                   = field(default_factory=list)
    group_logic: List[GroupLogic]               = field(default_factory=list)

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "SearchPackageFlat":
        raw_filters = d.get("filters")
        if not isinstance(raw_filters, list):
            raise ValueError("SearchPackage must have a top-level 'filters': []")
        flist: List[FlatFilter] = []
        for idx, f in enumerate(raw_filters, start=1):
            if "operator" not in f or "value" not in f:
                raise ValueError(f"Filter #{idx} missing 'operator' or 'value'")
            flist.append(FlatFilter(
                table    = f.get("table", "*"),
                field    = f.get("field", "*"),
                operator = f["operator"],
                value    = f["value"],
                logic    = f.get("logic", "and"),
                index_by = f.get("index_by"),
                position = f.get("position", "none"),
                group    = f.get("group", 1)
            ))

        raw_group_logic = d.get("group_logic", [])
        glist: List[GroupLogic] = []
        for idx, gl in enumerate(raw_group_logic, start=1):
            if "groups" not in gl or "logic" not in gl:
                raise ValueError(f"group_logic #{idx} missing 'groups' or 'logic'")
            glist.append(GroupLogic(
                groups = gl["groups"],
                logic  = gl["logic"]
            ))

        return SearchPackageFlat(filters=flist, group_logic=glist)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "filters":     [f.__dict__ for f in self.filters],
            "group_logic": [g.__dict__ for g in self.group_logic]
        }

# --- the package returned from search() ---
@dataclass
class SearchResultPackage:
    filters:     List[FlatFilter]
    group_logic: List[GroupLogic]
    uuids:       List[str]

# --- CRUD packages ---
@dataclass
class CreatePackage:
    table:   str
    records: List[Dict[str, Any]]

@dataclass
class ReadPackage:
    table: str
    uuids: List[str]

@dataclass
class UpdatePackage:
    table:   str
    updates: List[Dict[str, Any]]

@dataclass
class DeletePackage:
    table: str
    uuids: List[str]

# --- formatting package for search results ---
@dataclass
class ReadFormatPackage:
    filters:     List[FlatFilter]
    group_logic: List[GroupLogic]
    records:     List[Dict[str, Any]]

# --- batch helpers ---
@dataclass
class ChangeOp:
    """
    A single create/update operation on one table.
    """
    table: str
    fields: Dict[str, Any]

@dataclass
class BatchPackage:
    """
    A batch of named groups, where each key is either:
      • an existing UUID → apply updates, or
      • a new “group name” → insert new records
    """
    groups: Dict[str, List[ChangeOp]]
