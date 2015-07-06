import sys
import asynctorndb
import datetime
from tornado import gen
from storm.db import Database, ConnectionPool
from storm import error


class MySql(Database):
    @gen.coroutine
    def connect(self, callback=None):
        if not self.is_connected:
            self.db = asynctorndb.Connect(user=self.connection.user,
                                          passwd=self.connection.password,
                                          database=self.connection.database)

            yield self.db.connect()
            self.is_connected = True

        if callback is None:
            raise gen.Return(True)

        callback(True)

    @gen.coroutine
    def close(self, callback=None):
        if self.is_connected:
            yield self.db.close()
            self.is_connected = False

        if callback is None:
            raise gen.Return(True)

        callback(True)

    @staticmethod
    def _quote(value):
        if value is None:
            return 'null'

        if isinstance(value, datetime.datetime):
            return "'%s'" % str(value)

        if value == 'NOW()':
            return value

        # convert python3 byte strings
        if sys.version_info >= (3,0,0) and isinstance(value, bytes):
            value = value.decode('utf-8')

        if isinstance(value, float):
            return str(value)

        try:
            value = str(int(value))
        except:
            value = "'%s'" % asynctorndb.escape_string(value)

        return value


    @gen.coroutine
    def select_one(self, table, **kwargs):
        yield self.connect()

        field = None
        value = None
        for key in kwargs:
            field = key
            value = kwargs[key]

        value = MySql._quote(value)

        result = yield self.db.get("SELECT * FROM `%s` WHERE BINARY `%s` = %s" % (table, field, value))

        if result is None:
            raise error.StormNotFoundError("Object of type: %s not found with args: %s" % (table, kwargs))

        callback = kwargs.get('callback')
        if callback is None:
            raise gen.Return(result)

        callback(result)


    @gen.coroutine
    def select_multiple(self, table, query, **kwargs):
        yield self.connect()

        query.bind(':table', table)

        page = kwargs.get('page')
        if page:
            page_size = kwargs.get('page_size', 10)
            query.limit = page_size
            query.offset = (page - 1) * page_size

        raw_sql = query.sql

        total_count = 0
        tasks = [self.db.query(raw_sql)]
        if page:
            tasks.append(self.db.query(query.count_sql))

        results = yield tasks

        data = results[0]
        total_count = len(data)
        if len(results) == 2:
            total_count = results[1][0]['count']

        data, filtered_out_count = query.apply_filters(data)
        total_count -= filtered_out_count

        callback = kwargs.get('callback', None)
        if callback is None:
            raise gen.Return([data, total_count])

        callback([data, total_count])


    @gen.coroutine
    def insert(self, table, data, callback=None):
        yield self.connect()

        fields = []
        values = []
        for key in data:
            fields.append(key)
            value = MySql._quote(data[key])
            values.append(value)

        sql = "INSERT INTO `%s` (`%s`) VALUES (%s)" % (table, '`, `'.join(fields), ', '.join(values))

        # double escape % sign so we don't get an error if one of the fields
        # we are trying to insert has a % in it
        sql = sql.replace('%', '%%')

        insert_id = yield self.db.execute(sql)

        if callback is None:
            raise gen.Return(insert_id)

        callback(insert_id)

    @gen.coroutine
    def update(self, table, data, changes, primary_key, callback=None):
        if len(changes) == 0:
            raise gen.Return(False)

        yield self.connect()

        if 'modified_on' in data:
            changes.append('modified_on')
            data['modified_on'] = 'NOW()'

        pairs = []
        for key in changes:
            if key == primary_key:
                continue

            pairs.append("`%s` = %s" % (key, MySql._quote(data[key])))

        sql = "UPDATE `%s` SET %s WHERE `%s` = %s" % (table, ', '.join(pairs), primary_key, MySql._quote(data[primary_key]))
        result = yield self.db.execute(sql)

        if callback is None:
            raise gen.Return(result)

        callback(result)


class QueryFilter(object):
    TYPE_EQUAL = '='
    TYPE_NOT_EQUAL = '!='
    TYPE_GREATER_THAN = '>'
    TYPE_GREATER_THAN_OR_EQUAL = '>='
    TYPE_LESS_THAN = '<'
    TYPE_LESS_THAN_OR_EQUAL = '<='
    TYPE_IN = 'in'
    TYPE_NOT_IN = 'not in'

    def __init__(self, key, comparison, value):
        self.key = key
        self.comparison = comparison.lower()
        self.value = value

    def matches(self, row):
        if self.comparison == self.TYPE_EQUAL:
            return row[self.key] == self.value
        elif self.comparison == self.TYPE_NOT_EQUAL:
            return row[self.key] != self.value
        elif self.comparison == self.TYPE_GREATER_THAN:
            return row[self.key] > self.value
        elif self.comparison == self.TYPE_GREATER_THAN_OR_EQUAL:
            return row[self.key] >= self.value
        elif self.comparison == self.TYPE_LESS_THAN:
            return row[self.key] < self.value
        elif self.comparison == self.TYPE_LESS_THAN_OR_EQUAL:
            return row[self.key] <= self.value
        elif self.comparison == self.TYPE_IN:
            return row[self.key] in self.value
        elif self.comparison == self.TYPE_NOT_IN:
            return row[self.key] not in self.value

        return True

class Query(object):
    def __init__(self, sql):
        self._sql = sql
        self.count_sql = None
        self.to_bind = {}
        self.limit = None
        self.offset = None
        self.filters = []

    def bind(self, key, value):
        self.to_bind[key] = value
        return self

    def filter(self, key, comparison, value):
        self.filters.append(QueryFilter(key, comparison, value))
        return self

    def all_filters_allow(self, row):
        for f in self.filters:
            if not f.matches(row):
                return False

        return True

    def apply_filters(self, data):
        if self.limit or self.offset:
            raise error.StormError("""You cannot apply filters when using page
                                   and page_size to limit the mysql data""")

        if len(data) == 0 or len(self.filters) == 0:
            return (data, 0)

        new_data = []
        num_removed = 0
        for row in data:
            if self.all_filters_allow(row):
                new_data.append(row)
                continue

            num_removed += 1

        return (new_data, num_removed)

    @property
    def sql(self):
        sql = self._sql
        if ':table' in self.to_bind:
            sql = sql.replace(':table', "`%s`" % self.to_bind[':table'])
            del(self.to_bind[':table'])

        for key in self.to_bind:
            sql = sql.replace(key, MySql._quote(self.to_bind[key]))

        if self.limit:
            self.count_sql = "SELECT count(*) count FROM %s" % sql.split(' FROM ')[1]

        if self.limit:
            sql += " LIMIT %d" % self.limit

        if self.offset:
            sql += " OFFSET %d" % self.offset

        return sql


class ConnectionPool(ConnectionPool):
    def get_db_class(self):
        return MySql
