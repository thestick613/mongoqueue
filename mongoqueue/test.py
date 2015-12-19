
import os
import time

from datetime import datetime

import pymongo
from unittest import TestCase

from mongoqueue import MongoQueue
from lock import MongoLock, lock
import multiprocessing as mp


class MongoLockTest(TestCase):

    def setUp(self):
        self.client = pymongo.MongoClient()
        self.db = self.client.test_queue
        self.collection = self.db.locks

    def tearDown(self):
        self.client.drop_database("test_queue")

    def test_lock_acquire_release_context_manager(self):
        with lock(self.collection, 'test1') as l:
            self.assertTrue(l.locked)
        self.assertEqual(self.collection.find().count(), 0)

    def test_auto_expires_old(self):
        lock = MongoLock(self.collection, 'test2', lease=2)
        self.assertTrue(lock.acquire())

        time.sleep(2.2)
        # Reports truthfully it doesn't have the local anymore,
        # using ttl time, stale db record extant.
        self.assertFalse(lock.locked)

        lock2 = MongoLock(self.collection, 'test2')
        # New lock acquire will take ownership based on old ttl\
        self.assertTrue(lock2.acquire())

        # Releasing the original doesn't change ownership
        self.assertTrue(lock.release())
        records = list(self.collection.find())
        self.assertEqual(len(records), 1)

        self.assertEqual(records.pop()['client_id'], lock2.client_id)
        self.assertFalse(lock.acquire(wait=False))

        lock2.release()
        self.assertFalse(list(self.collection.find()))

def dequeue(n):
    q = MongoQueue(pymongo.MongoClient().test_queue.queue_1, "consumer_1")
    j = q.next()
    if j:
        return j.payload["context_id"]


class MongoQueueRetryTimeTests(TestCase):

    def setUp(self):
        self.client = pymongo.MongoClient()
        self.db = self.client.test_queue
        self.queue = MongoQueue(self.db.queue_1, "consumer_1", retry_after=2)

    def tearDown(self):
        self.client.drop_database("test_queue")

    def test_complete_scenario(self):
        self.queue.put({"message": "hello"})

        job = self.queue.next()
        with job as data:
            raise Exception

        time.sleep(1)
        job2 = self.queue.next()
        self.assertEqual(job2, None)
        time.sleep(1.1)

        job3 = self.queue.next()
        with job as data:
            self.assertEqual(data["message"], "hello")
        job4 = self.queue.next()
        self.assertEqual(job4, None)

    def test_error_with_increased_retry(self):
        self.queue.put({"message": "hello"})
        job = self.queue.next()
        job.error(custom_retry_after=3)

        time.sleep(1)
        job = self.queue.next()
        self.assertEqual(job, None)

        time.sleep(2.1)

        job2 = self.queue.next()
        with job2 as data:
            self.assertEqual(data["message"], "hello")

        job3 = self.queue.next()
        self.assertEqual(job3, None)

    def test_release_with_increased_retry(self):
        self.queue.put({"message": "hello"})
        job = self.queue.next()
        job.release(custom_retry_after=3)

        time.sleep(1)
        job = self.queue.next()
        self.assertEqual(job, None)

        time.sleep(2.1)

        job2 = self.queue.next()
        with job2 as data:
            self.assertEqual(data["message"], "hello")

        job3 = self.queue.next()
        self.assertEqual(job3, None)


class MongoQueueTest(TestCase):

    def setUp(self):
        self.client = pymongo.MongoClient()
        self.db = self.client.test_queue
        self.queue = MongoQueue(self.db.queue_1, "consumer_1")

    def tearDown(self):
        self.client.drop_database("test_queue")

    def assert_job_equal(self, job, data):
        for k, v in data.items():
            self.assertEqual(job.payload[k], v)

    def test_put_next(self):
        data = {"context_id": "alpha",
                "data": [1, 2, 3],
                "more-data": time.time()}
        self.queue.put(dict(data))
        job = self.queue.next()
        self.assert_job_equal(job, data)

        job = self.queue.next()
        self.assertEqual(job, None)

    def test_atomic_next(self):
        data = {"context_id": "alpha321",
                "data": [1, 2, 3],
                "more-data": time.time()}
        self.queue.put(dict(data))

        p = mp.Pool()
        q = self.queue
        jobs = p.map(dequeue, [1,2])
        self.assertNotEqual(jobs[0], jobs[1])

    def test_get_empty_queue(self):
        job = self.queue.next()
        self.assertEqual(job, None)

    def test_priority(self):
        self.queue.put({"name": "alice"}, priority=1)
        self.queue.put({"name": "bob"}, priority=2)
        self.queue.put({"name": "mike"}, priority=0)

        self.assertEqual(
            ["bob", "alice", "mike"],
            [self.queue.next().payload['name'],
             self.queue.next().payload['name'],
             self.queue.next().payload['name']])

        job = self.queue.next()
        self.assertEqual(job, None)

    def test_complete(self):
        data = {"context_id": "alpha",
                "data": [1, 2, 3],
                "more-data": datetime.now()}

        self.queue.put(data)
        self.assertEqual(self.queue.size(), 1)
        job = self.queue.next()
        job.complete()
        self.assertEqual(self.queue.size(), 0)

        job = self.queue.next()
        self.assertEqual(job, None)

    def test_release(self):
        data = {"context_id": "alpha",
                "data": [1, 2, 3],
                "more-data": time.time()}

        self.queue.put(data)
        job = self.queue.next()
        job.release()
        self.assertEqual(self.queue.size(), 1)
        job = self.queue.next()
        self.assert_job_equal(job, data)

        job = self.queue.next()
        self.assertEqual(job, None)

    def test_max_attempts(self):
        data = {"context_id": "alpha",
                "ts": time.time()}
        self.queue.put(dict(data))
        attempts = 0
        for i in xrange(0, self.queue.max_attempts):
            job = self.queue.next()
            if not job:
                break
            with job:
                attempts += 1
                raise Exception()
        self.assertEqual(attempts, self.queue.max_attempts)

    def test_error(self):
        pass

    def test_progress(self):
        pass

    def test_stats(self):

        for i in range(5):
            data = {"context_id": "alpha",
                    "data": [1, 2, 3],
                    "more-data": time.time()}
            self.queue.put(data)
        job = self.queue.next()
        job.error("problem")

        stats = self.queue.stats()
        self.assertEqual({'available': 5,
                          'total': 5,
                          'locked': 0,
                          'errors': 0}, stats)

    def test_context_manager_error(self):
        self.queue.put({"foobar": 1})
        job = self.queue.next()
        try:
            with job as data:
                self.assertEqual(data['payload']["foobar"], 1)
                # Item is returned to the queue on error
                raise SyntaxError
        except SyntaxError:
            pass

        job = self.queue.next()
        self.assertEqual(job.data['attempts'], 1)

    def test_context_manager_complete(self):
        self.queue.put({"foobar": 1})
        job = self.queue.next()
        with job as data:
            self.assertEqual(data['payload']["foobar"], 1)
        job = self.queue.next()
        self.assertEqual(job, None)

    def test_next_by_payload(self):
        self.queue.put({"type": "first_type", "param":"param1"})
        self.queue.put({"type": "second_type", "param":"param2"})
        self.queue.put({"type": "third_type", "param":"param3"})

        job = self.queue.next({"payload.type": "second_type"})
        with job as data:
            self.assertEqual(data["payload"]["param"], "param2")

        job = self.queue.next({"payload.type": "third_type"})
        with job as data:
            self.assertEqual(data["payload"]["param"], "param3")

        job = self.queue.next({"payload.type": "fourth_type"})
        self.assertEqual(job, None)

        job = self.queue.next()
        with job as data:
            self.assertEqual(data["payload"]["param"], "param1")

        job = self.queue.next()
        self.assertEqual(job, None)

