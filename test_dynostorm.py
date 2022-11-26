from dynostorm.attributes import PartitionKey, Attribute, GlobalSecondaryIndex, \
    AccessPattern, EntityKey, SortKey, AccessPatternMany, EntitySortKey
from dynostorm.entities import Table


class TestTable(Table):
    region_name = 'us-test-1'


class Test(TestTable.Entity):
    id = PartitionKey(int)
    date_created = Attribute(str)

    gsi1 = GlobalSecondaryIndex(date_created, id)

    record_by_id = AccessPattern(id)
    records_by_date = AccessPattern(gsi1)


class Bar(TestTable.Entity):
    id = PartitionKey(int)
    record_by_id = AccessPattern(id)


class TestItem(TestTable.EntityItem):
    test_id = EntityKey(Test)
    id = SortKey(str)
    quantity = Attribute(int)

    test_items_by_test = AccessPatternMany(test_id)


class TestBar(TestTable.EntityItem):
    test_id = EntityKey(Test)
    bar_id = EntitySortKey(Bar)

    quantity = Attribute(int)

    test_bars_by_test = AccessPatternMany(test_id)


def test_pk_sk_getters():
    test = Test(id=1, date_created='2022-11-24')
    assert test.pk == 'Test#1'
    assert test.sk == '$'

    test_item = TestItem(test_id=1, id='1', quantity=1)
    assert test_item.pk == 'Test#1'
    assert test_item.sk == 'TestItem#1'

    test_bar = TestBar(test_id=1, bar_id=2, quantity=1)
    assert test_bar.pk == 'Test#1'
    assert test_bar.sk == 'Bar#2'


def test_entity_access_pattern_keys():
    assert Test.record_by_id.get_keys() == ('pk', None)
    assert Test.records_by_date.get_keys() == ('pk0', 'sk0')
    assert TestItem.test_items_by_test.get_keys() == ('pk', None)
    assert Test.record_by_id.get_access_kwargs(1) == {
        'pk': 'Test#1',
        'sk': '$'
    }
    assert Test.records_by_date.get_access_kwargs('2022-11-24') == {
        'pk0': '2022-11-24',
        'sk0__begins_with': 'Test#'
    }

    assert TestItem.test_items_by_test.get_access_kwargs(1) == {
        'pk': 'Test#1',
        'sk__begins_with': 'TestItem#'
    }

    assert TestBar.test_bars_by_test.get_access_kwargs(1) == {
        'pk': 'Test#1',
        'sk__begins_with': 'Bar#'
    }


def test_entity_update_attributes():
    test = Test(id=1, date_created='2022-11-24')
    update_keys = test.get_update_keys()
    update_attributes = test.get_update_attributes()
    assert update_keys == {
        'pk': {
            'S': 'Test#1',
        },
        'sk': {
            'S': '$',
        }
    }
    print(update_attributes)
    assert update_attributes['values'] == {
        ':pk0': '2022-11-24',
        ':sk0': 'Test#1',
        ':date_created': '2022-11-24'
    }
    assert update_attributes['map'] == {
        'pk0': ':pk0',
        'sk0': ':sk0',
        '#date_created': ':date_created'
    }
    assert update_attributes['names'] == {
        '#date_created': 'date_created'
    }

    test_item = TestItem(test_id=1, id='1', quantity=1)
    update_keys = test_item.get_update_keys()
    update_attributes = test_item.get_update_attributes()
    assert update_keys == {
        'pk': {
            'S': 'Test#1',
        },
        'sk': {
            'S': 'TestItem#1',
        }
    }
    assert update_attributes['values'] == {
        ':quantity': 1
    }
    assert update_attributes['map'] == {
        '#quantity': ':quantity'
    }
    assert update_attributes['names'] == {
        '#quantity': 'quantity'
    }
