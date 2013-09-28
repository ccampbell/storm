import motor
import error
from tornado import gen


class Connection(object):
    def __init__(self, host='localhost', port=None, db=None):
        self.host = host
        self.port = port
        self.db = db


class Database(object):
    def __init__(self, connection):
        if not isinstance(connection, Connection):
            raise error.StormError('connection must be instance of storm.db.Connection')

        self.connection = connection
        self.is_connected = False


class MongoDb(Database):
    def connect(self):
        if self.is_connected:
            return

        self.motor_client = motor.MotorClient(
            self.connection.host,
            self.connection.port
        ).open_sync()

        self.db = self.motor_client[self.connection.db]
        self.is_connected = True

    @gen.coroutine
    def select_one(self, table, **args):
        self.connect()

        result = yield motor.Op(getattr(self.db, table).find_one, args)

        if result is None:
            raise error.StormNotFoundError("Object of type: %s not found with args: %s" % (table, args))

        callback = args.get('callback')

        if callback is None:
            raise gen.Return(result)

        callback(result)

    @gen.coroutine
    def insert(self, table, data, callback=None):
        self.connect()

        result = yield motor.Op(self.db[table].insert, data)

        if callback is None:
            raise gen.Return(result)

        callback(result)
