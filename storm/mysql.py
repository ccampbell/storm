import sys
import datetime
from tornado import gen
from storm.db import Database, ConnectionPool
from storm import error
import tornado_mysql
from tornado_mysql.pools import Pool

cursor_type = tornado_mysql.cursors.DictCursor


class MySql(Database):
    @gen.coroutine
    def connect(self, callback=None):
        if not self.is_connected:
            self.db = Pool(
                dict(user=self.connection.user,
                     passwd=self.connection.password,
                     db=self.connection.database,
                     cursorclass=cursor_type),
                max_idle_connections=1,
                max_open_connections=1)

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
            value = "'%s'" % tornado_mysql.converters.escape_string(value)

        return value

    @gen.coroutine
    def select_one(self, table, **kwargs):
        yield self.connect()

        where_bits = []
        for key in kwargs:
            where_bits.append("`%s` = %s" % (key, MySql._quote(kwargs[key])))

        sql = "SELECT * FROM `%s` WHERE BINARY %s" % (table, ' AND BINARY '.join(where_bits))

        cur = yield self.db.execute(sql)
        result = cur.fetchone()

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

        tasks = [self.db.execute(raw_sql)]
        if page:
            tasks.append(self.db.execute(query.count_sql))

        cursors = yield tasks

        results = [cursors[0].fetchall()]
        if len(cursors) > 1:
            results.append(cursors[1].fetchall())

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

        cur = yield self.db.execute(sql)
        insert_id = cur.lastrowid

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
        compound_primary_key = isinstance(primary_key, list)
        for key in changes:
            if compound_primary_key and key in primary_key:
                continue

            if key == primary_key:
                continue

            pairs.append("`%s` = %s" % (key, MySql._quote(data[key])))

        if not compound_primary_key:
            primary_key = [primary_key]

        where_bits = []
        for key in primary_key:
            where_bits.append("`%s` = %s" % (key, MySql._quote(data[key])))

        sql = "UPDATE `%s` SET %s WHERE %s" % (table, ', '.join(pairs), ' AND '.join(where_bits))

        result = yield self.db.execute(sql)
        if callback is None:
            raise gen.Return(result)

        callback(result)

    @gen.coroutine
    def delete(self, table, primary_key_fields, primary_key_values, callback=None):
        self.connect()

        where_bits = []
        for i, key in enumerate(primary_key_fields):
            where_bits.append("`%s` = %s" % (key, MySql._quote(primary_key_values[i])))

        result = False
        if len(where_bits) > 0:
            sql = "DELETE FROM `%s` WHERE %s" % (table, ' AND '.join(where_bits))
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
    def __init__(self, connection, count=10, lifetime=3600):
        super(ConnectionPool, self).__init__(connection, count, lifetime)
        db = MySql(self.connection)
        db.db = Pool(
            dict(user=connection.user,
                 passwd=connection.password,
                 db=connection.database,
                 cursorclass=cursor_type),
            max_idle_connections=self.count,
            max_recycle_sec=self.lifetime,
            max_open_connections=self.count+10)

        db.is_connected = True
        self._db = db

    def get_db_class(self):
        return MySql

    @gen.coroutine
    def get_db(self, callback=None):
        if callback is not None:
            callback(self._db)
            return

        raise gen.Return(self._db)
