# Storm

### A simple ORM for Tornado

Storm stands for **S**imple **T**ornado **O**bject **R**elational **M**apping.

It uses Mongo DB for the backend, but it should be swappable for any storage by implementing the `storm.db.Database` interface.

This is the result of a few hours of work so I would not recommend using it in production at the moment.

## Usage

```python
from storm.model import Model
from storm.db import Connection, MongoDb
from tornado.gen import coroutine
from tornado.ioloop import IOLoop

class User(Model):
    pass

@coroutine
def main():
    db = MongoDb(Connection(
                    host='localhost',
                    port=27017,
                    database='test'))

    db.connect()

    Model.set_db(db)

    # save a new object
    user = User()
    user.name = 'Craig'
    user.type = 'basic'
    id = yield user.save()

    # find an object
    try:
        user = yield User.find(name='Craig')
        print user.name
        print user.type
    except Exception, e:
        print e

    IOLoop.instance().stop()

main()
IOLoop.instance().start()
```

## Note

This is in no way affiliated with the storm ORM developed by Canonical: http://storm.canonical.com.  I didn't know there was another ORM with the same name until I checked PyPi.
