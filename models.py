from typing import NamedTuple, List


class Table(NamedTuple):
    name: str
    database: str
    schema: str
    columns: List = []
    columns_type: List = []
    import_data: List = []
    primary_key: List = []
    drop_table: bool = False
