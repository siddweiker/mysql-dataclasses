
from pprint import pprint

from tables import *


def main():
    print('Starting')
    print('====')
    print(Orders.sql_create())
    print('====')
    print(Orders.sql_table())
    print('====')
    print(Orders.sql_insert())
    print('====')
    print(Orders.sql_update())
    print('====')
    print(Orders.sql_prune(180))
    print('====')
    print(Orders.sql_drop())
    print('====')

    data = {
        'orderNumber': 1,
        'orderDate': '2023-01-01 09:30:00',
        'comments': 'testing',
        'price': 100.0,
        'quantity': 10,
        'customer': 'Foo',
    }
    order = Orders.from_dict(data)
    pprint(order)
    # To insert into a mysql db:
    # cursor.execute(Orders.sql_insert(), order)

    print('====')

    try:
        wrong = dict(data)
        wrong['orderDate'] = '2023-01-01'
        Orders.from_dict(wrong)
    except Exception as e:
        print(e)

    print('====')

    try:
        wrong = dict(data)
        del wrong['price']
        Orders.from_dict(wrong)
    except Exception as e:
        print(e)

    print('====')

    monthly = OrdersMonthly(**dataclasses.asdict(order))  # Copy
    monthly.monthlyDate = monthly.orderDate
    pprint(monthly)

    print('====')
    print('Completed')


if __name__ == '__main__':
    main()
