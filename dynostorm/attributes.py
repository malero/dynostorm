
class BaseAttribute:
    local_key = None
    table_key = None


class PartitionKey(BaseAttribute):
    def __init__(self, *fields, **kwargs):
        pass


class SortKey(BaseAttribute):
    def __init__(self, *fields, **kwargs):
        pass


class EntityKey(BaseAttribute):
    def __init__(self, *fields, **kwargs):
        pass


class GlobalSecondaryIndex(BaseAttribute):
    def __init__(self, partition, sort, **kwargs):
        self.partition = partition
        self.sort = sort


class Attribute(BaseAttribute):
    def __init__(self, *fields, **kwargs):
        pass


class AccessPattern(BaseAttribute):
    for_entity = None

    def __init__(self, *fields):
        self.gsi = None
        self.partition = None
        self.sort = None

        for field in fields:
            if isinstance(field, PartitionKey):
                self.partition = field
            elif isinstance(field, SortKey):
                self.sort = field
            elif isinstance(field, GlobalSecondaryIndex):
                self.gsi = field
                self.partition = field.partition
                self.sort = field.sort

    def __call__(self, *args, **kwargs):
        access_kwargs = {}
        local_partition_key = self.partition.local_key
        local_sort_key = self.sort and self.sort.local_key or None
        sort_key = None
        if self.gsi is None:
            partition_key = self.partition.table_key
            if self.gsi is not None:
                sort_key = self.sort.table_key
        else:
            partition_key = self.for_entity.get_gsi_key(
                self.partition.local_key,
                self.gsi
            )
            if self.gsi is not None:
                sort_key = self.for_entity.get_gsi_key(
                    self.sort.local_key,
                    self.gsi
                )

        if len(args) > 0:
            access_kwargs[partition_key] = self.for_entity.get_key_value(
                self.partition.local_key,
                args[0]
            )

        if self.sort is None:
            access_kwargs['sk'] = '$'
        elif len(args) > 1:
            access_kwargs[sort_key] = self.for_entity.get_key_value(
                self.sort.local_key,
                args[1]
            )
        elif self.gsi is not None:
            sk_prefix = f'{self.for_entity.__name__}#'
            access_kwargs[f'{sort_key}__starts_with'] = sk_prefix

        for kwarg_key, kwarg_value in kwargs.items():
            k, dec = kwarg_key.split('__')
            key = None
            if k == local_partition_key:
                key = partition_key
            elif k == local_sort_key:
                key = sort_key

            if key:
                access_kwargs[f'{key}__{dec}'] = kwarg_value

        return self.for_entity.get(self.gsi, **access_kwargs)
