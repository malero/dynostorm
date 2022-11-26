from dynostorm.entities import Table
from dynostorm.attributes import PartitionKey, Attribute, GlobalSecondaryIndex, \
    EntityKey, SortKey, EntitySortKey, AccessPatternMany, AccessPatternSingle


class OrderTable(Table):
    region_name = 'us-east-1'


class Product(OrderTable.Entity):
    sku = PartitionKey(str)
    name = Attribute(str)

    order_by_sku = AccessPatternSingle(sku)


class Order(OrderTable.Entity):
    id = PartitionKey(int)
    date_placed = Attribute(str)

    gsi1 = GlobalSecondaryIndex(date_placed, id)

    order_by_id = AccessPatternSingle(id)
    orders_by_date = AccessPatternMany(gsi1)


class OrderItem(OrderTable.EntityItem):
    order_id = EntityKey(Order)
    product_sku = EntitySortKey(Product)
    quantity = Attribute(int)

    order_items_by_order = AccessPatternMany(order_id)


class Payment(OrderTable.EntityItem):
    order = EntityKey(Order)
    id = SortKey(str)
    amount = Attribute(int)


if __name__ == '__main__':
    for item in OrderItem.order_items_by_order(101):
        print(item.__dict__)
