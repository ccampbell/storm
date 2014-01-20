from db import Database
from error import StormError
from tornado import gen

class Model(object):
    db = None

    def __init__(self):
        self._type = type(self).__name__.lower()

        if not hasattr(self, '_table'):
            self._table = self._type

        if not hasattr(self, '_primary_key'):
            self._primary_key = '_id'

    @staticmethod
    def set_db(database):
        if not isinstance(database, Database):
            raise StormError('database must be instance of storm.db.Database')

        Model.db = database

    @classmethod
    def get_table(class_name):
        table = class_name.__name__.lower()
        if hasattr(class_name, '_table'):
            table = getattr(class_name, '_table')

        return table

    @classmethod
    def _convert_object(class_name, obj):
        return_obj = class_name()

        # set all properties
        for key in obj:
            setattr(return_obj, key, obj[key])

        # make sure the primary key is set to a string
        setattr(return_obj, return_obj._primary_key,
                str(getattr(return_obj, return_obj._primary_key)))

        return return_obj

    @classmethod
    @gen.coroutine
    def find(class_name, **args):
        table = getattr(class_name, 'get_table')()

        callback = None
        if 'callback' in args:
            callback = args['callback']
            del(args['callback'])

        obj = yield Model.db.select_one(table, **args)

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
            result = yield Model.db.insert(self._table, to_save)
            setattr(self, self._primary_key, str(result))
        else:
            to_save[self._primary_key] = self.__dict__[self._primary_key]
            result = yield Model.db.update(self._table, to_save)

        if callback is None:
            raise gen.Return(result)

        callback(result)

    @gen.coroutine
    def delete(self, callback=None):
        result = False

        if hasattr(self, self._primary_key):
            result = yield Model.db.delete(self._table, self._primary_key,
                                           getattr(self, self._primary_key))

        if callback is None:
            raise gen.Return(result)

        callback(result)
