from storm import error


class Connection(object):
    def __init__(self, host='localhost', port=None, database=None, user=None, password=None):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password


class Database(object):
    def __init__(self, connection):
        if not isinstance(connection, Connection):
            raise error.StormError('connection must be instance of storm.db.Connection')

        self.connection = connection
        self.is_connected = False
