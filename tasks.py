import base64

from urllib.parse import urlparse

from redis import StrictRedis
from redis.lock import Lock

from celery_app import celery

url = urlparse("redis://localhost:6379/0")
redis = StrictRedis(host=url.hostname,
                    port=int(url.port),
                    db=url.path[1:],
                    decode_responses=True)


class MyLock(Lock):
    def acquire(self, *args, **kwargs):
        r = super(MyLock, self).acquire(*args, **kwargs)
        if r:
            print("Locked {}".format(self.name))
        return r

    def do_release(self, *args, **kwargs):
        super(MyLock, self).do_release(*args, **kwargs)
        print("Released {}".format(self.name))

@celery.task(bind=True)
def release_lock(self, name, token):
    lock = redis.lock(name, lock_class=MyLock)
    lock.do_release(base64.b64decode(token.encode("utf-8")))

@celery.task(bind=True)
def calculate(self, x, token):
    lock = redis.lock(token, timeout=5, lock_class=MyLock)
    if lock.acquire(blocking=False):
        print(x*2)
        release_lock.apply_async(args=[token, base64.b64encode(lock.local.token).decode("utf-8")], countdown=1)
    else:
        calculate.apply_async(args=[x, token], countdown=1)
