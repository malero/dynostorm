# DynamoDB Single Table Object Relational Mapping

```python
from dynostorm.entities import Table
from dynostorm.attributes import PartitionKey, Attribute, \
    GlobalSecondaryIndex, AccessPatternSingle, AccessPatternMany, EntityKey, \ 
    EntitySortKey

class OrderTable(Table):
    region_name = 'us-east-1'


class Product(OrderTable.Entity):
    sku = PartitionKey(str)
    name = Attribute(str)

    product_by_sku = AccessPatternSingle(sku)


class Order(OrderTable.Entity):
    id = PartitionKey(int)
    date_placed = Attribute(str)

    gsi1 = GlobalSecondaryIndex(date_placed, id)

    order_by_id = AccessPatternSingle(id)
    orders_by_date = AccessPatternMany(gsi1)


class OrderItem(OrderTable.EntityItem):
    order_id = EntityKey(Order)
    product_id = EntitySortKey(Product)
    quantity = Attribute(int)

    order_items_by_order = AccessPatternMany(order_id)

# Usage
OrderTable.create_table()

product = Product(sku='test-product')
product.save()

order = Order(id=1, date_placed='2022-11-25T18:37')
order.save()

order_item = OrderItem(
    order_id=order.id,
    product_id=product.id,
    quantity=1
)
order_item.save()

product = Product.product_by_sku('test-product')
order = Order.order_by_id(1)
order_items = OrderItem.order_items_by_order(order.id)

```

