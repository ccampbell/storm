import inspect
from storm.db import Database
from storm.error import StormError
from tornado import gen
from tornado.web import RequestHandler
from storm.collection import Collection

class Model(object):
    TYPE_MONGO_DB = 'mongodb'
    TYPE_MYSQL = 'mysql'
    db = None
    check_for_handler = False

    def __init__(self):
        self._type = type(self).__name__.lower()
        self._changes = []

        if not hasattr(self, '_table'):
            self._table = self._type

        if not hasattr(self, '_primary_key'):
            self._primary_key = '_id'

    def __setattr__(self, name, value):
        if name[0] == '_':
            self.__dict__[name] = value
            return

        if name in self.__dict__:
            old_value = self.__dict__[name]
            if value != old_value:
                self._changes.append(name)

            self.__dict__[name] = value
            return

        self._changes.append(name)
        self.__dict__[name] = value

    @staticmethod
    def set_db(database):
        if not isinstance(database, Database):
            raise StormError('database must be instance of storm.db.Database')

        Model.db = database

        if (not hasattr(Model, '_primary_key') and
            Model.get_database_type() == Model.TYPE_MYSQL):
            Model._primary_key = 'id'

    @staticmethod
    def get_db():
        if Model.check_for_handler:

            # loop over where this came from and if it came from an instance of
            # tornado.web.RequestHandler that has a db property on it then use
            # that.  this isn't really great, but it is a huge convenience to
            # be able to tie a db connection to a request if you want
            for trace in inspect.stack()[3:]:
                local_vars = trace[0].f_locals
                instance = local_vars.get('self', None)
                if instance and isinstance(instance, RequestHandler) and hasattr(instance, 'db'):
                    return instance.db

        return Model.db

    @classmethod
    def get_table(class_name):
        table = class_name.__name__.lower()
        if hasattr(class_name, '_table'):
            table = getattr(class_name, '_table')

        return table

    @staticmethod
    def get_database_type(db_object=None):
        name = type(Model.db) if db_object is None else type(db_object)
        return name.__name__.lower()

    @classmethod
    def _convert_object(class_name, obj):
        return_obj = class_name()

        # set all properties
        for key in obj:
            setattr(return_obj, key, obj[key])

        # make sure the primary key is set to a string
        # for mongodb
        if Model.get_database_type() == Model.TYPE_MONGO_DB:
            setattr(return_obj, return_obj._primary_key,
                    str(getattr(return_obj, return_obj._primary_key)))

        # reset changes
        return_obj._changes = []

        return return_obj

    @classmethod
    @gen.coroutine
    def find_all(class_name, data, **args):
        table = getattr(class_name, 'get_table')()

        callback = None
        if 'callback' in args:
            callback = args['callback']
            del(args['callback'])

        objects, total_count = yield Model.get_db().select_multiple(table, data, **args)

        as_dict = args.get('as_dict', False)

        collection = Collection()

        if 'page' in args:
            collection.page = args['page']
            collection.page_size = args.get('page_size', 10)

        collection.total_count = total_count

        for obj in objects:
            new_obj = getattr(class_name, '_convert_object')(obj)
            collection.append(new_obj if not as_dict else new_obj.__dict__)

        if callback is None:
            raise gen.Return(collection)

        callback(collection)

    @classmethod
    @gen.coroutine
    def find(class_name, **args):
        table = getattr(class_name, 'get_table')()

        callback = None
        if 'callback' in args:
            callback = args['callback']
            del(args['callback'])

        obj = yield Model.get_db().select_one(table, **args)

        return_obj = None
        if obj is not None:
            return_obj = getattr(class_name, '_convert_object')(obj)

        if callback is None:
            raise gen.Return(return_obj)

        callback(return_obj)

    @gen.coroutine
    def save(self, callback=None):
        to_save = {}
        for k in [key for key in self.__dict__ if not key[0] == '_']:
            to_save[k] = self.__dict__[k]

        if not hasattr(self, self._primary_key):
            result = yield Model.get_db().insert(self._table, to_save)

            if Model.get_database_type() == Model.TYPE_MONGO_DB:
                result = str(result)

            setattr(self, self._primary_key, result)
        else:
            to_save[self._primary_key] = self.__dict__[self._primary_key]
            result = yield Model.get_db().update(self._table, to_save, self._changes)

        self._changes = []

        if callback is None:
            raise gen.Return(result)

        callback(result)

    @gen.coroutine
    def delete(self, callback=None):
        result = False

        if hasattr(self, self._primary_key):
            result = yield Model.get_db().delete(self._table, self._primary_key,
                                           getattr(self, self._primary_key))

        if callback is None:
            raise gen.Return(result)

        callback(result)
