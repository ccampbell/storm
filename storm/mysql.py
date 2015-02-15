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

        result = yield self.db.get("SELECT * FROM `%s` WHERE `%s` = %s" % (table, field, value))

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
    def update(self, table, data, changes, callback=None):
        if len(changes) == 0:
            raise gen.Return(False)

        yield self.connect()

        if 'modified_on' in data:
            changes.append('modified_on')
            data['modified_on'] = 'NOW()'

        pairs = []
        for key in changes:
            if key == 'id':
                continue

            pairs.append("`%s` = %s" % (key, MySql._quote(data[key])))

        sql = "UPDATE `%s` SET %s WHERE id = %d" % (table, ', '.join(pairs), data['id'])
        result = yield self.db.execute(sql)

        if callback is None:
            raise gen.Return(result)

        callback(result)


class Query(object):
    def __init__(self, sql):
        self._sql = sql
        self.count_sql = None
        self.to_bind = {}
        self.limit = None
        self.offset = None

    def bind(self, key, value):
        self.to_bind[key] = value
        return self

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
