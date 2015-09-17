import inspect
import json
from storm.db import Database, ConnectionPool
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

        if not hasattr(self, '_json_fields'):
            self._json_fields = []

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
        if (not isinstance(database, Database) and
            not isinstance(database, ConnectionPool)):

            raise StormError("""database must be instance of storm.db.Database
                or storm.db.ConnectionPool""")

        Model.db = database

        if (not hasattr(Model, '_primary_key') and
            Model.get_database_type() == Model.TYPE_MYSQL):
            Model._primary_key = 'id'

    @staticmethod
    @gen.coroutine
    def get_db(callback=None):
        if Model.check_for_handler:

            # loop over where this came from and if it came from an instance of
            # tornado.web.RequestHandler that has a db property on it then use
            # that.  this isn't really great, but it is a huge convenience to
            # be able to tie a db connection to a request if you want
            for trace in inspect.stack()[3:]:
                local_vars = trace[0].f_locals
                instance = local_vars.get('self', None)
                if instance and isinstance(instance, RequestHandler) and hasattr(instance, 'db'):
                    if callback is not None:
                        callback(instance.db)
                        return

                    raise gen.Return(instance.db)

        if isinstance(Model.db, ConnectionPool):
            db = yield Model.db.get_db()
            if callback is not None:
                callback(db)
                return

            raise gen.Return(db)

        if callback is not None:
            callback(Model.db)
            return

        raise gen.Return(Model.db)

    @classmethod
    def get_table(class_name):
        table = class_name.__name__.lower()
        if hasattr(class_name, '_table'):
            table = getattr(class_name, '_table')

        return table

    @staticmethod
    def get_database_type(db_object=None):
        name = type(Model.db) if db_object is None else type(db_object)
        if db_object is None and isinstance(Model.db, ConnectionPool):
            name = Model.db.get_db_class()

        return name.__name__.lower()

    @gen.coroutine
    def before_save(self, changes):
        pass

    @gen.coroutine
    def after_save(self, changes):
        pass

    @gen.coroutine
    def after_load(self):
        pass

    @classmethod
    def _convert_object(class_name, obj):
        return_obj = class_name()

        # set all properties
        for key in obj:
            if key in return_obj._json_fields:
                try:
                    obj[key] = json.loads(obj[key])
                except:
                    obj[key] = {}

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

        db = yield Model.get_db()
        objects, total_count = yield db.select_multiple(table, data, **args)

        as_dict = args.get('as_dict', False)

        collection = Collection()

        if 'page' in args:
            collection.page = args['page']
            collection.page_size = args.get('page_size', 10)

        collection.total_count = total_count

        for obj in objects:
            new_obj = getattr(class_name, '_convert_object')(obj)
            yield new_obj.after_load()
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

        db = yield Model.get_db()
        obj = yield db.select_one(table, **args)

        return_obj = None
        if obj is not None:
            return_obj = getattr(class_name, '_convert_object')(obj)
            yield return_obj.after_load()

        if callback is None:
            raise gen.Return(return_obj)

        callback(return_obj)

    @gen.coroutine
    def save(self, callback=None):
        yield self.before_save(self._changes)

        to_save = {}
        for k in [key for key in self.__dict__ if not key[0] == '_']:
            val = self.__dict__[k]
            if k in self._json_fields:
                try:
                    val = json.dumps(val)
                except:
                    pass

            to_save[k] = val

        is_compound_primary_key = isinstance(self._primary_key, list)
        primary_key_not_included = not is_compound_primary_key and not hasattr(self, self._primary_key)
        primary_key_was_set = self._primary_key in self._changes

        if is_compound_primary_key:
            primary_key_was_set = True
            for field in self._primary_key:
                if field not in self._changes:
                    primary_key_was_set = False
                    break

        if primary_key_not_included or primary_key_was_set:
            db = yield Model.get_db()
            result = yield db.insert(self._table, to_save)

            if Model.get_database_type() == Model.TYPE_MONGO_DB:
                result = str(result)

            if primary_key_not_included:
                setattr(self, self._primary_key, result)
        else:

            # I do not remember why this is here, but I think it might be for
            # mongodb where the primary key starts with an underscore and
            # therefore won't be included in the to_save dictionary
            if not is_compound_primary_key:
                to_save[self._primary_key] = self.__dict__[self._primary_key]

            db = yield Model.get_db()
            result = yield db.update(self._table, to_save, self._changes, self._primary_key)

        yield self.after_save(self._changes)
        self._changes = []

        if callback is None:
            raise gen.Return(result)

        callback(result)

    @gen.coroutine
    def delete(self, callback=None):
        result = False

        is_compound_primary_key = isinstance(self._primary_key, list)
        if is_compound_primary_key or hasattr(self, self._primary_key):
            primary_key_fields = self._primary_key
            primary_key_values = []

            if not is_compound_primary_key:
                primary_key_fields = [primary_key_fields]

            for key in primary_key_fields:
                primary_key_values.append(getattr(self, key))

            db = yield Model.get_db()
            result = yield db.delete(self._table,
                                     primary_key_fields,
                                     primary_key_values)

        if callback is None:
            raise gen.Return(result)

        callback(result)
