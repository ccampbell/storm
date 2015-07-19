import time
from storm import error
from tornado import gen


class Connection(object):
    def __init__(self, host='localhost', port=None, database=None, user=None, password=None):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password


class ConnectionPool(object):
    def __init__(self, connection, count=10, lifetime=3600):
        self.connection = connection
        self.count = count
        self.lifetime = lifetime

    @gen.coroutine
    def get_db(self, callback=None):
        raise NotImplementedError('The "get_db" method is not implemented')

    def get_db_class(self):
        raise NotImplementedError('The "get_db_class" method is not implemented')


class Database(object):
    def __init__(self, connection):
        if not isinstance(connection, Connection):
            raise error.StormError('connection must be instance of storm.db.Connection')

        self.connection = connection
        self.is_connected = False
        self.start_time = time.time()
