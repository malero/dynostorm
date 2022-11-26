import boto3

from dynostorm import constants
from dynostorm.attributes import PartitionKey, Attribute, GlobalSecondaryIndex, \
    AccessPattern, SortKey, BaseField, EntityKey, EntitySortKey


class EntityMeta(type):
    def __new__(mcs, clsname, bases, clsdict):
        partition_field = None
        sort_field = None
        fields = {}
        attributes = {}
        access_patterns = {}
        global_secondary_indexes = {}

        for name, val in clsdict.items():
            if isinstance(val, BaseField):
                val.logical_key = name
                fields[name] = val

            if isinstance(val, AccessPattern):
                access_patterns[name] = val
            elif isinstance(val, Attribute):
                attributes[name] = val
                val.physical_key = name
            elif isinstance(val, PartitionKey):
                partition_field = val
                val.physical_key = 'pk'
            elif isinstance(val, SortKey):
                sort_field = val
                val.physical_key = 'sk'
            elif isinstance(val, GlobalSecondaryIndex):
                global_secondary_indexes[name] = val

        clsdict['partition_field'] = partition_field
        clsdict['sort_field'] = sort_field
        clsdict['fields'] = fields
        clsdict['access_patterns'] = access_patterns
        clsdict['attributes'] = attributes
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
    partition_field = None
    sort_field = None
    fields = {}
    access_patterns = {}
    attributes = {}
    global_secondary_indexes = {}

    def __init__(self, **kwargs):
        self._partition_attribute = None
        self._sort_attribute = None

        for logical_key, field in self.__class__.fields.items():
            if isinstance(field, PartitionKey):
                self._partition_attribute = field
            elif isinstance(field, SortKey):
                self._sort_attribute = field

            if logical_key in kwargs:
                setattr(self, logical_key, kwargs[logical_key])
            else:
                setattr(self, logical_key, None)

    @property
    def pk(self):
        return self.get_field_value(self._partition_attribute.logical_key)

    @property
    def sk(self):
        if self._sort_attribute is None:
            return '$'
        return self.get_field_value(self._sort_attribute.logical_key)

    def get_field_value(self, logical_key):
        return self.__class__.get_key_value(logical_key, getattr(self, logical_key))

    def get_update_keys(self):
        return {
            'pk': {'S': self.pk},
            'sk': {'S': self.sk},
        }

    def get_update_attributes(self):
        name_attributes = {}
        value_attributes = {}
        value_map = {}
        for logical_key, field in self.__class__.attributes.items():
            value = getattr(self, logical_key)
            if value is not None:
                set_field_key = f'#{logical_key}'
                set_value_key = f':{field.logical_key}'

                value_map[set_field_key] = set_value_key
                value_attributes[set_value_key] = value
                name_attributes[set_field_key] = field.logical_key

        for gsi_key, gsi in self.__class__.global_secondary_indexes.items():
            i = self.__class__.table.get_gsi_index(gsi)
            pk_set_key = f':pk{i}'
            sk_set_key = f':sk{i}'
            value_map[f'pk{i}'] = pk_set_key
            value_map[f'sk{i}'] = sk_set_key
            pk_logical_key = gsi.partition.logical_key
            sk_logical_key = gsi.sort.logical_key
            value_attributes[pk_set_key] = self.get_field_value(
                pk_logical_key)
            value_attributes[sk_set_key] = self.get_field_value(
                sk_logical_key)

        return {
            'names': name_attributes,
            'values': value_attributes,
            'map': value_map,
        }

    def get_value_type_index(self, value):
        if isinstance(value, int):
            return 'N'
        elif isinstance(value, float):
            return 'N'
        elif isinstance(value, str):
            return 'S'
        elif isinstance(value, bool):
            return 'BOOL'

    def save(self):
        keys = self.get_update_keys()
        update_attributes = self.get_update_attributes()
        update_expression = f'set {", ".join([f"{field_key} = {set_key}" for field_key, set_key in update_attributes["map"].items()])}'
        update_expression_values = {
            set_key: {
                self.get_value_type_index(value): str(value)
            } for set_key, value in update_attributes['values'].items()
        }
        update_expression_names = {
            field_key: field_name for field_key, field_name in update_attributes['names'].items()
        }

        self.__class__.table.client().update_item(
            TableName=self.__class__.table.table_name,
            Key=keys,
            UpdateExpression=update_expression,
            ExpressionAttributeValues=update_expression_values,
            ExpressionAttributeNames=update_expression_names,
        )

    @classmethod
    def from_response(cls, data):
        kwargs = {}
        for physical_name, value_dict in data.items():
            value_type, value = list(value_dict.items())[0]
            if physical_name == 'pk':
                entity_type, value = cls.parse_key(value)
                kwargs[cls.partition_field.logical_key] = cls.parse_physical_value(
                    cls.partition_field.logical_key,
                    value
                )
            elif physical_name == 'sk':
                entity_type, value = cls.parse_key(value)
                kwargs[cls.sort_field.logical_key] = cls.parse_physical_value(
                    cls.sort_field.logical_key,
                    value
                )
            else:
                kwargs[physical_name] = cls.parse_physical_value(physical_name, value)
        return cls(**kwargs)

    @classmethod
    def parse_key(cls, key):
        return key.split('#')

    @classmethod
    def get_key_prefix(cls):
        return f'{cls.__name__}#'

    @classmethod
    def get(cls, gsi=None, **kwargs):
        key_conditions = {}
        for attribute_key, value in kwargs.items():
            comparison_operator = 'EQ'
            if '__' in attribute_key:
                attribute_key, op = attribute_key.split('__')
                comparison_operator = constants.OP_MAP.get(op, 'EQ')

            key_conditions[attribute_key] = {
                'ComparisonOperator': comparison_operator,
                'AttributeValueList': [
                    {'S': value}
                ]
            }

        query_kwargs = dict(
            TableName=cls.table.table_name,
            KeyConditions=key_conditions
        )
        if gsi is not None:
            query_kwargs['IndexName'] = gsi.logical_key

        return cls.table.client().query(**query_kwargs)

    @classmethod
    def get_gsi_key(cls, logical_key, gsi):
        gsi_index = cls.table.get_gsi_index(gsi)
        if gsi.partition.logical_key == logical_key:
            return f'pk{gsi_index}'
        else:
            return f'sk{gsi_index}'

    @classmethod
    def get_key_value(cls, logical_key, value):
        field = cls.fields.get(logical_key)
        if field is None:
            raise ValueError(f'Field {logical_key} not found on {cls}')

        if isinstance(field, EntityKey):
            return f'{field.for_entity.get_key_prefix()}{value}'
        elif isinstance(field, EntitySortKey):
            return f'{field.for_entity.get_key_prefix()}{value}'
        elif isinstance(field, PartitionKey):
            return f'{cls.get_key_prefix()}{value}'
        elif isinstance(field, SortKey):
            return f'{cls.get_key_prefix()}{value}'

        return value

    @classmethod
    def parse_physical_value(cls, physical_key, value):
        field = cls.fields.get(physical_key)
        return field.parse(value)


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
            if gsi.logical_key == gsi_key:
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
