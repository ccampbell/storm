import motor
from tornado import gen
from bson.objectid import ObjectId
from storm.db import Database, ConnectionPool
from storm import error


class MongoDb(Database):
    def connect(self):
        if self.is_connected:
            return

        self.motor_client = motor.MotorClient(
            self.connection.host,
            self.connection.port
        ).open_sync()

        self.db = self.motor_client[self.connection.database]
        self.is_connected = True

    @gen.coroutine
    def select_one(self, table, **kwargs):
        self.connect()

        if '_id' in kwargs:
            kwargs['_id'] = ObjectId(kwargs['_id'])

        result = yield motor.Op(getattr(self.db, table).find_one, kwargs)

        if result is None:
            raise error.StormNotFoundError("Object of type: %s not found with args: %s" % (table, kwargs))

        callback = kwargs.get('callback')

        if callback is None:
            raise gen.Return(result)

        callback(result)

    @gen.coroutine
    def select_multiple(self, table, data, **kwargs):
        cursor = getattr(self.db, table).find(data)

        # handle pagination
        if 'page' in kwargs:
            page = kwargs['page']
            page_size = kwargs.get('page_size', 10)
            offset = (page - 1) * page_size
            cursor.limit(page_size)
            cursor.skip(offset)

        # default sort to newest first
        sort = kwargs.get('sort', [('_id', 0)])
        cursor.sort(sort)

        data = []
        while (yield cursor.fetch_next):
            data.append(cursor.next_object())

        total_count = yield motor.Op(cursor.count)

        callback = kwargs.get('callback')

        if callback is None:
            raise gen.Return([data, total_count])

        callback([data, total_count])

    @gen.coroutine
    def insert(self, table, data, callback=None):
        result = yield self.update(table, data, callback)

        if callback is None:
            raise gen.Return(result)

        callback(result)

    @gen.coroutine
    def update(self, table, data, changes, primary_key, callback=None):
        self.connect()

        if primary_key in data:
            data[primary_key] = ObjectId(data[primary_key])

        result = yield motor.Op(self.db[table].save, data)

        if callback is None:
            raise gen.Return(result)

        callback(result)

    @gen.coroutine
    def delete(self, table, primary_key_fields, primary_key_values, callback=None):
        self.connect()

        to_delete = {
            primary_key_fields[0]: ObjectId(primary_key_values[0])
        }

        result = yield motor.Op(self.db[table].remove, to_delete)

        if callback is None:
            raise gen.Return(result)

        callback(result)


class ConnectionPool(ConnectionPool):
    def get_db_class(self):
        return MongoDb
