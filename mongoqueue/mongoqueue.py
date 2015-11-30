#   Copyright 2012 Kapil Thangavelu
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.


import pymongo

from datetime import datetime, timedelta
import traceback


DEFAULT_INSERT = {
    "priority": 0,
    "time": None,
    "period": 0,
    "attempts": 0,
    "locked_by": None,
    "locked_at": None,
    "last_error": None
}


class MongoQueue(object):
    """A queue class
    """

    def __init__(self, collection, consumer_id, timeout=300, max_attempts=3):
        """
        """
        self.collection = collection
        self.consumer_id = consumer_id
        self.timeout = timeout
        self.max_attempts = max_attempts

    def close(self):
        """Close the in memory queue connection.
        """
        self.collection.connection.close()

    def clear(self):
        """Clear the queue.
        """
        return self.collection.drop()

    def size(self):
        """Total size of the queue
        """
        return self.collection.count()

    def repair(self):
        """Clear out stale locks.

        Increments per job attempt counter.
        """
        self.collection.find_and_modify(
            query={
                "locked_by": {"$ne": None},
                "locked_at": {
                    "$lt": datetime.now() - timedelta(seconds=self.timeout)}},
            update={
                "$set": {"locked_by": None, "locked_at": None},
                "$inc": {"attempts": 1}}
        )

    def put(self, payload, priority=0, time=None, period=None):
        """Place a job into the queue
        """
        job = dict(DEFAULT_INSERT)
        job['priority'] = priority
        job['time'] = time
        # Store period as an integer representing the number of seconds
        # because BSON format doesn't support timedelta
        if period and type(period) == timedelta:
            job['period'] = period.total_seconds()
        job['payload'] = payload
        if self.is_dupe(job):
            return
        return self.collection.insert(job)

    def is_dupe(self, job):
        jobs = self.collection.find({
            'payload': job['payload'],
            'time': job['time'],
            'period': job['period'],
            'attempts': 0},
            limit=1
        )
        for job in jobs:
            if job:
                return True
        return False

    def next(self, filter_payload={}):
        scheduled_job = self.next_scheduled_job(filter_payload=filter_payload)
        free_job = self.next_free_job(filter_payload=filter_payload)
        next_job = None

        if scheduled_job and scheduled_job['time'] < datetime.utcnow():
            next_job = scheduled_job
        else:
            next_job = free_job

        if next_job is not None:
            return self._wrap_one(self.collection.find_and_modify({
                    "_id": next_job["_id"],
                    "locked_by": None,
                },
                update={
                        "$set": {
                            "locked_by": self.consumer_id,
                            "locked_at": datetime.now()
                        }},
                new=True,
                limit=1
            ))
        else:
            return None

    def next_scheduled_job(self, filter_payload={}):
        _query = {
                "locked_by": None,
                "locked_at": None,
                "time": {"$ne": None},
                "attempts": {"$lt": self.max_attempts},
            }

        jobs = self.collection.find(
            {"$and": [_query, filter_payload]},
            sort=[('time', pymongo.ASCENDING)],
            limit=1
        )
        for job in jobs:
            return job
        return None

    def next_free_job(self, filter_payload={}):
        _query = {
                "locked_by": None,
                "locked_at": None,
                "time": None,
                "attempts": {"$lt": self.max_attempts},
            }

        jobs = self.collection.find(
            {"$and":[_query, filter_payload]},
            sort=[('priority', pymongo.DESCENDING)],
            limit=1
        )
        for job in jobs:
            return job
        return None

    def _jobs(self):
        return self.collection.find(
            query={"locked_by": None,
                   "locked_at": None,
                   "attempts": {"$lt": self.max_attempts}},
            sort=[('priority', pymongo.DESCENDING)],
        )

    def _wrap_one(self, data):
        return data and Job(self, data) or None

    def stats(self):
        """Get statistics on the queue.

        Use sparingly requires a collection lock.
        """

        js = """function queue_stat(){
        return db.eval(
        function(){
           var a = db.%(collection)s.count(
               {'locked_by': null,
                'attempts': {$lt: %(max_attempts)i}});
           var l = db.%(collection)s.count({'locked_by': /.*/});
           var e = db.%(collection)s.count(
               {'attempts': {$gte: %(max_attempts)i}});
           var t = db.%(collection)s.count();
           return [a, l, e, t];
           })}""" % {
             "collection": self.collection.name,
             "max_attempts": self.max_attempts}

        return dict(zip(
            ["available", "locked", "errors", "total"],
            self.collection.database.eval(js)))


class Job(object):

    def __init__(self, queue, data):
        """
        """
        self._queue = queue
        self._data = data

    @property
    def data(self):
        return self._data

    @property
    def payload(self):
        return self._data['payload']

    @property
    def job_id(self):
        return self._data["_id"]

    ## Job Control

    def complete(self):
        """Job has been completed.
        """
        if self._data['period']:
            updated_time = self._data['time'] + timedelta(seconds=self._data['period'])

            return self._queue.collection.find_and_modify(
                {"_id": self.job_id, "locked_by": self._queue.consumer_id},
                update={"$set":{
                    "locked_by": None,
                    "locked_at": None,
                    "time": updated_time
                }})

        return self._queue.collection.find_and_modify(
            {"_id": self.job_id, "locked_by": self._queue.consumer_id},
            remove=True)

    def error(self, message=None):
        """Note an error processing a job, and return it to the queue.
        """
        self._queue.collection.find_and_modify(
            {"_id": self.job_id, "locked_by": self._queue.consumer_id},
            update={"$set": {
                "locked_by": None, "locked_at": None, "last_error": message},
                "$inc": {"attempts": 1}})

        if self._data['attempts'] == self._queue.max_attempts - 1 and self._data['period']:
            updated_time = self._data['time'] + timedelta(seconds=self._data['period'])

            self._queue.put(self._data['payload'],
                priority=self._data['priority'],
                time=updated_time,
                period=timedelta(seconds=self._data['period']))


    def progress(self, count=0):
        """Note progress on a long running task.
        """
        return self._queue.collection.find_and_modify(
            {"_id": self.job_id, "locked_by": self._queue.consumer_id},
            update={"$set": {"progress": count, "locked_at": datetime.now()}})

    def release(self):
        """Put the job back into_queue.
        """
        return self._queue.collection.find_and_modify(
            {"_id": self.job_id, "locked_by": self._queue.consumer_id},
            update={"$set": {"locked_by": None, "locked_at": None},
                    "$inc": {"attempts": 1}})

    def abort(self):
        """Intentionally terminate execution of a job, and remove it from the queue
        """
        return self._queue.collection.find_and_modify(
            {"_id": self.job_id, "locked_by": self._queue.consumer_id},
            remove=True)

    ## Context Manager support

    def __enter__(self):
        return self.data

    def __exit__(self, type, value, tb):
        if (type, value, tb) == (None, None, None):
            self.complete()
        else:
            error = traceback.format_exc()
            self.error(error)
            return True
