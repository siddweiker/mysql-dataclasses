
import dataclasses
from datetime import datetime

from sql_dataclasses import SqlTable


def parse_datetime(date_val):
    if isinstance(date_val, int) or (isinstance(date_val, str) and date_val.isdigit()):
        try:
            return datetime.fromtimestamp(int(date_val)/1000)
        except Exception as e:
            raise ValueError(f'Not a valid epoch millis date for {date_val}: {str(e)}')
    try:
        return datetime.strptime(date_val, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        pass
    raise ValueError(f'No valid date format found for {date_val}')


@dataclasses.dataclass
class Customer(SqlTable):
    customer: str = dataclasses.field(
        metadata={'sql_type': 'VARCHAR(255) COMMENT "Customer name override"'})
    original: str = dataclasses.field(
        metadata={'sql_key': True, 'sql_type': 'VARCHAR(255) COMMENT "Customer name from order data"'})


@dataclasses.dataclass
class Orders(SqlTable):
    orderNumber: str = dataclasses.field(metadata={'sql_key': True})
    orderDate: datetime = dataclasses.field(metadata={'pre_init': parse_datetime})
    comments: str = dataclasses.field(metadata={'sql_ignore': True})
    price: float
    quantity: int

    # Optional, should appear after required
    customer: str = dataclasses.field(default=None, metadata={
        'sql_name': 'customerId',
        'sql_type': 'INT COMMENT "Customer ID to customer table"',
        'sql_fk': (Customer.sql_table(), 'original'),
    })
    shippedDate: datetime = dataclasses.field(
        default=None,
        metadata={'pre_init': lambda x: None}  # Ignore the value from the file
    )
    active: bool = False


@dataclasses.dataclass
class OrdersMonthly(Orders):
    monthlyDate: datetime = dataclasses.field(default=None, metadata={'sql_key': True})
