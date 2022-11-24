import boto3

from attributes import PartitionKey, Attribute, GlobalSecondaryIndex, \
    AccessPattern, SortKey, BaseAttribute


class EntityMeta(type):
    def __new__(mcs, clsname, bases, clsdict):
        fields = {}
        attributes = {}
        partition_keys = {}
        sort_keys = {}
        access_patterns = {}
        global_secondary_indexes = {}

        for name, val in clsdict.items():
            if isinstance(val, BaseAttribute):
                val.local_key = name
                fields[name] = val

            if isinstance(val, AccessPattern):
                access_patterns[name] = val
            elif isinstance(val, Attribute):
                attributes[name] = val
            elif isinstance(val, PartitionKey):
                partition_keys[name] = val
                val.table_key = 'pk'
            elif isinstance(val, SortKey):
                sort_keys[name] = val
                val.table_key = 'sk'
            elif isinstance(val, GlobalSecondaryIndex):
                global_secondary_indexes[name] = val

        clsdict['fields'] = fields
        clsdict['access_patterns'] = access_patterns
        clsdict['attributes'] = attributes
        clsdict['partition_keys'] = partition_keys
        clsdict['sort_keys'] = sort_keys
        clsdict['global_secondary_indexes'] = global_secondary_indexes

        clsobj = super().__new__(mcs, clsname, bases, clsdict)

        # Setup entities on table
        table = getattr(clsobj, 'table', None)
        if table is not None:
            table.entities[clsname] = clsobj

        for access_pattern in access_patterns.values():
            access_pattern.for_entity = clsobj

        return clsobj


class Entity(metaclass=EntityMeta):
    table = None
    fields = {}
    access_patterns = {}
    attributes = {}
    partition_keys = {}
    sort_keys = {}
    global_secondary_indexes = {}

    def __init__(self, *fields, **kwargs):
        pass

    @classmethod
    def get(cls, gsi=None, **kwargs):
        key_conditions = {}
        for table_key, value in kwargs.items():
            key_conditions[table_key] = {
                'ComparisonOperator': 'EQ',
                'AttributeValueList': {
                    'S': value
                }
            }
        print(dict(
            TableName=cls.table.table_name,
            IndexName=gsi and gsi.local_key or None,
            KeyConditions=key_conditions
        ))

    @classmethod
    def get_gsi_key(cls, local_key, gsi):
        gsi_index = cls.table.get_gsi_index(gsi)
        if gsi.partition.local_key == local_key:
            return f'pk{gsi_index}'
        else:
            return f'sk{gsi_index}'

    @classmethod
    def get_key_value(cls, local_key, value):
        field = cls.fields.get(local_key)
        if field is None:
            raise ValueError(f'Field {local_key} not found on {cls}')

        if isinstance(field, PartitionKey):
            return f'{cls.__name__}#{value}'

        return value


class TableMeta(type):
    def __new__(mcs, clsname, bases, clsdict):
        if clsdict.get('table_name', None) is None:
            clsdict['table_name'] = clsname

        clsobj = super().__new__(mcs, clsname, bases, clsdict)
        clsobj.Entity = type(f'{clsname}Entity', (Entity,), {})
        clsobj.Entity.table = clsobj
        clsobj.EntityItem = type(f'{clsname}EntityItem', (Entity,), {})
        clsobj.EntityItem.table = clsobj
        return clsobj


class Table(metaclass=TableMeta):
    region_name = 'us-east-1'
    table_name = None
    Entity = Entity
    EntityItem = Entity
    entities = {}

    @classmethod
    def client(cls):
        if not hasattr(cls, '_client'):
            setattr(cls, '_client', boto3.client('dynamodb', region_name=cls.region_name))
        return getattr(cls, '_client')

    @classmethod
    def enumerate_gsis(cls):
        gsi_keys = []
        for entity in cls.entities.values():
            gsi_keys.extend(entity.global_secondary_indexes.keys())

        gsi_keys = sorted(list(set(gsi_keys)))
        for i, gsi in enumerate(gsi_keys):
            yield i, gsi

    @classmethod
    def get_gsi_index(cls, gsi):
        for i, gsi_key in cls.enumerate_gsis():
            if gsi.local_key == gsi_key:
                return i
        return None

    @classmethod
    def create_table(cls):
        attribute_definitions = [
            {
                'AttributeName': 'pk',
                'AttributeType': 'S',
            },
            {
                'AttributeName': 'sk',
                'AttributeType': 'S',
            },
        ]

        for i, gsi_key in cls.enumerate_gsis():
            attribute_definitions.append({
                'AttributeName': f'pk{i}',
                'AttributeType': 'S',
            })
            attribute_definitions.append({
                'AttributeName': f'sk{i}',
                'AttributeType': 'S',
            })

        cls.client().create_table(
            TableName=cls.table_name,
            KeySchema=[
                {
                    'AttributeName': 'pk',
                    'KeyType': 'HASH',
                },
                {
                    'AttributeName': 'sk',
                    'KeyType': 'RANGE',
                },
            ],
            AttributeDefinitions=attribute_definitions,
            GlobalSecondaryIndexes=[
                {
                    'IndexName': gsi_key,
                    'KeySchema': [
                        {
                            'AttributeName': f'pk{i}',
                            'KeyType': 'HASH',
                        },
                        {
                            'AttributeName': f'sk{i}',
                            'KeyType': 'RANGE',
                        },
                    ],
                    'Projection': {
                        'ProjectionType': 'ALL',
                    },
                }
                for i, gsi_key in cls.enumerate_gsis()
            ],
            BillingMode='PAY_PER_REQUEST',
        )
