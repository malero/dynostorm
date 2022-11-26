
class BaseField:
    logical_key = None
    physical_key = None

    def __init__(self, parse_fn, *args, **kwargs):
        self.parse_fn = parse_fn

    def parse(self, value):
        return self.parse_fn(value)


class PartitionKey(BaseField):
    pass


class SortKey(BaseField):
    pass


class EntityKey(PartitionKey):
    def __init__(self, for_entity, *fields, **kwargs):
        self.for_entity = for_entity
        parse_fn = for_entity.partition_field.parse_fn
        super().__init__(parse_fn, *fields, **kwargs)


class EntitySortKey(SortKey):
    def __init__(self, for_entity, *fields, **kwargs):
        self.for_entity = for_entity
        parse_fn = for_entity.partition_field.parse_fn
        super().__init__(parse_fn, *fields, **kwargs)


class GlobalSecondaryIndex(BaseField):
    def __init__(self, partition, sort, **kwargs):
        super().__init__(None, **kwargs)
        self.partition = partition
        self.sort = sort


class Attribute(BaseField):
    pass


class AccessPattern(BaseField):
    for_entity = None

    def __init__(self, *fields, return_collection=None):
        super().__init__(None)
        self.gsi = None
        self.partition = None
        self.sort = None
        self.return_collection = return_collection

        for field in fields:
            if isinstance(field, PartitionKey):
                self.partition = field
            elif isinstance(field, SortKey):
                self.sort = field
            elif isinstance(field, GlobalSecondaryIndex):
                self.gsi = field
                self.partition = field.partition
                self.sort = field.sort

    def get_keys(self):
        sort_key = None
        if self.gsi is None:
            partition_key = self.partition.physical_key
            if self.gsi is not None:
                sort_key = self.sort.physical_key
        else:
            partition_key = self.for_entity.get_gsi_key(
                self.partition.logical_key,
                self.gsi
            )
            if self.gsi is not None:
                sort_key = self.for_entity.get_gsi_key(
                    self.sort.logical_key,
                    self.gsi
                )

        return partition_key, sort_key

    def get_key_key_value(self, logical_key, value):
        return self.for_entity.get_key_value(logical_key, value)

    def get_access_kwargs(self, *args, **kwargs):
        access_kwargs = {}
        logical_partition_key = self.partition.logical_key
        logical_sort_key = self.sort and self.sort.logical_key or None
        partition_key, sort_key = self.get_keys()

        if len(args) > 0:
            access_kwargs[partition_key] = self.for_entity.get_key_value(
                self.partition.logical_key,
                args[0]
            )

        if self.sort is None:
            if not self.return_collection:
                access_kwargs['sk'] = '$'
            elif self.for_entity.sort_field is not None:
                if isinstance(self.for_entity.sort_field, EntitySortKey):
                    sort_key_prefix = self.for_entity.sort_field.for_entity.get_key_prefix()
                else:
                    sort_key_prefix = self.for_entity.get_key_prefix()
                access_kwargs['sk__begins_with'] = sort_key_prefix
        elif len(args) > 1:
            access_kwargs[sort_key] = self.for_entity.get_key_value(
                self.sort.logical_key,
                args[1]
            )
        elif self.gsi is not None:
            access_kwargs[f'{sort_key}__begins_with'] = self.for_entity.get_key_prefix()

        for kwarg_key, kwarg_value in kwargs.items():
            if '__' in kwarg_key:
                k, dec = kwarg_key.split('__')
            else:
                k = kwarg_key
                dec = None

            key = None
            if k == logical_partition_key:
                key = partition_key
            elif k == logical_sort_key:
                key = sort_key

            if key:
                if dec:
                    access_kwargs[f'{key}__{dec}'] = kwarg_value
                else:
                    access_kwargs[key] = kwarg_value
        return access_kwargs

    def __call__(self, *args, **kwargs):
        access_kwargs = self.get_access_kwargs(*args, **kwargs)

        response = self.for_entity.get(self.gsi, **access_kwargs)
        items = response.get('Items', [])
        """
        {
            'Items': [
                {
                    'quantity': {
                        'N': '1'
                    },
                    'sk': {
                        'S': 'Product#123'
                    }, 
                    pk': {
                        'S': 'Order#101'
                    }
                }
            ],
            'Count': 1,
            'ScannedCount': 1,
            'ResponseMetadata': {
                'RequestId': 'JECMEEB51FJERPJ08EUB1HRV27VV4KQNSO5AEMVJF66Q9ASUAAJG',
                'HTTPStatusCode': 200,
                'HTTPHeaders': {
                    'server': 'Server',
                    'date': 'Fri, 25 Nov 2022 23:59:55 GMT',
                    'content-type': 'application/x-amz-json-1.0',
                    'content-length': '109', 
                    'connection': 'keep-alive',
                    'x-amzn-requestid': 'JECMEEB51FJERPJ08EUB1HRV27VV4KQNSO5AEMVJF66Q9ASUAAJG',
                    'x-amz-crc32': '3030131771'
                },
                'RetryAttempts': 0
            }
        }
        """
        if self.return_collection is True:
            collection = []
            for item in items:
                collection.append(self.for_entity.from_response(item))
            return collection
        elif self.return_collection is False:
            return self.for_entity.from_response(items[0])
        else:
            return items


class AccessPatternSingle(AccessPattern):
    def __init__(self, *args, **kwargs):
        kwargs['return_collection'] = False
        super().__init__(*args, **kwargs)


class AccessPatternMany(AccessPattern):
    def __init__(self, *args, **kwargs):
        kwargs['return_collection'] = True
        super().__init__(*args, **kwargs)
