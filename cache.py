import time

store = {}
namespace = 'pcache'

class Cache(object):
    def __init__(self, cache_name):
        super(Cache, self).__init__()
        registration_table_key = '{}.{}'.format(namespace, 'registration_table')
        registration_table = store[registration_table_key]
        cache_info = registration_table[cache_name]
        self._cache_name = cache_name
        self._version, self._data_type, self._timeout = cache_info

    @classmethod
    def register(self, cache_name, data_type, timeout):
        if (data_type != int and
            data_type != float and
            data_type != long and
            data_type != basestring and
            data_type != complex):
                raise TypeError('Unsupported type')

        registration_table_key = '{}.{}'.format(namespace, 'registration_table')
        registration_table = store.setdefault(registration_table_key, {})

        if not registration_table.has_key(cache_name):
            cache_info = (1, data_type, timeout)
            registration_table[cache_name] = cache_info

        return Cache(cache_name)

    def timeout(self):
        return self._timeout

    def cache_name(self):
        return self._cache_name

    def version(self):
        return self._version

    def bump_version(self):
        registration_table_key = '{}.{}'.format(namespace, 'registration_table')
        registration_table = store[registration_table_key]
        version, _, _ = registration_table[self._cache_name]
        self._version = version + 1
        registration_table[self._cache_name] = (self._version, self._data_type, self._timeout)

    def write(self, key, datum):
        if not isinstance(datum, self._data_type):
            raise TypeError

        full_key = '{}.{}.{}.{}'.format(namespace, self._cache_name, self._version, key)
        ttl = time.time() + self._timeout

        store[full_key] = (datum, ttl)

    def write_collection(self, key, datum_collection):
        # To maintain 'memcache' like semantics, make a copy of collection
        collection = [datum for datum in datum_collection]

        if not all(isinstance(datum, self._data_type) for datum in collection):
            raise TypeError

        full_key = '{}.{}.{}.{}'.format(namespace, self._cache_name, self._version, key)
        ttl = time.time() + self._timeout
        store[full_key] = (collection, ttl)

    def read(self, key):
        full_key = '{}.{}.{}.{}'.format(namespace, self._cache_name, self._version, key)

        datum, ttl = store[full_key]
        if ttl < time.time():               # simplistic expiration logic
            del store[full_key]
            raise KeyError

        if not isinstance(datum, self._data_type):
            raise TypeError

        ttl = time.time() + self._timeout
        store[full_key] = (datum, ttl)
        return datum

    def read_collection(self, key):
        full_key = '{}.{}.{}.{}'.format(namespace, self._cache_name, self._version, key)

        collection, ttl = store[full_key]
        if ttl < time.time():               # simplistic expiration logic
            del store[full_key]
            return KeyError

        if not all(isinstance(datum, self._data_type) for datum in collection):
            raise TypeError

        ttl = time.time() + self._timeout
        store[full_key] = (collection, ttl)

        return collection
