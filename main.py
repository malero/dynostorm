from dynostorm.entities import Table
from dynostorm.attributes import PartitionKey, Attribute, GlobalSecondaryIndex, \
    AccessPattern, EntityKey, SortKey


class OrderTable(Table):
    region_name = 'us-east-1'


class TestTable(Table):
    region_name = 'us-east-15'


class Order(OrderTable.Entity):
    id = PartitionKey(int)
    date = Attribute(str)

    gsi1 = GlobalSecondaryIndex(date, id)

    order_by_id = AccessPattern(id)
    orders_by_date = AccessPattern(gsi1)


class OrderItem(OrderTable.EntityItem):
    order = EntityKey(Order)
    sk = SortKey(str)
    quantity = Attribute(int)

    order_items_by_order = AccessPattern(order)


class Payment(OrderTable.EntityItem):
    sk = Attribute(str)
    amount = Attribute(int)


if __name__ == '__main__':
    Order.order_by_id(1)
    Order.orders_by_date(date__gte='2022-11-24')
