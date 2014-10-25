from random import randrange
import time
from storm import error


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
        self.db_connections = [];

    def create_new_connection(self):
        cls = self.get_db_class()
        instance = cls(self.connection)
        self.db_connections.append(instance)
        return instance

    def get_db(self):
        if len(self.db_connections) < self.count:
            return self.create_new_connection()

        index = randrange(0, len(self.db_connections))
        connection = self.db_connections[index]
        if (time.time() - connection.start_time) > self.lifetime:
            removed = self.db_connections.pop(index)
            removed.close()
            return self.create_new_connection()

        return self.db_connections[index]

    def get_db_class(self):
        raise NotImplementedError('The "get_db_class" method is not implemented')


class Database(object):
    def __init__(self, connection):
        if not isinstance(connection, Connection):
            raise error.StormError('connection must be instance of storm.db.Connection')

        self.connection = connection
        self.is_connected = False
        self.start_time = time.time()
