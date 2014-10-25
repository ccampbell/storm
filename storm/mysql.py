import sys
import asynctorndb
import datetime
from tornado import gen
from storm.db import Database
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
    def insert(self, table, data, callback=None):
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
