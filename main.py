from dynostorm.entities import Table
from dynostorm.attributes import PartitionKey, Attribute, GlobalSecondaryIndex, \
    AccessPattern, EntityKey, SortKey, EntitySortKey, AccessPatternMany


class OrderTable(Table):
    region_name = 'us-east-1'


class TestTable(Table):
    region_name = 'us-east-15'


class Product(OrderTable.Entity):
    sku = PartitionKey(str)
    name = Attribute(str)

    order_by_sku = AccessPattern(sku)


class Order(OrderTable.Entity):
    id = PartitionKey(int)
    date_placed = Attribute(str)

    gsi1 = GlobalSecondaryIndex(date_placed, id)

    order_by_id = AccessPattern(id)
    orders_by_date = AccessPattern(gsi1)


class OrderItem(OrderTable.EntityItem):
    order_id = EntityKey(Order)
    product_id = EntitySortKey(Product)
    quantity = Attribute(int)

    order_items_by_order = AccessPatternMany(order_id)


class Payment(OrderTable.EntityItem):
    order = EntityKey(Order)
    id = SortKey(str)
    amount = Attribute(int)


if __name__ == '__main__':
    print(OrderItem.order_items_by_order(101))
