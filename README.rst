mongoqueue
----------

Properties
==========

 - Isolation

   Do not let different consumers process the same message.

 - Reliablity

   Do not let a failed consumer disappear an item.

 - Atomic

   Operations on the queue are atomic.

Usage
=====

A queue can be instantiated with a mongo collection and a consumer
identifier. The consumer identifier helps distinguish multiple queue
consumers that are taking jobs from the queue::

  >> from pymongo import MongoClient
  >> from mongoqueue import MongoQueue
  >> queue = MongoQueue(
  ...   MongoClient().test_db.doctest_queue,
  ...   consumer_id="consumer-1",
  ...   timeout=300,
  ...   max_attempts=3,
  ...   retry_after=0)

The ``MongoQueue`` class ``timeout`` parameters specifies how long in a
seconds a how long a job may be held by a consumer before its
considered failed.

A job which timeouts or errors more than the ``max_attempts``
parameter is considered permanently failed, and will no longer be
processed.

The ``retry_after`` parameter is the default waiting time, in seconds,
between two attempts of the same job, when the first one failed. This property
supersedes scheduling and priority.

New jobs/items can be placed in the queue by passing a dictionary::

  >> queue.put({"foobar": 1})

A job ``priority`` key and integer value can be specified in the
dictionary which will cause the job to be processed before lower
priority items::

  >> queue.put({"foobar": 0}, priority=1})

An item can be fetched out by calling the ``next`` method on a queue.
This returns a Job object::

  >> job = queue.next()
  >> job.payload
  {"foobar": 1}

You can prefer to select objects from the queue which have certain
properties, which are translated directly into mongo syntax. You only have
to remember that the saved message is put in the "payload" key::

  >> queue.put({"type":"alert"})
  >> queue.put({"type":"message"})
  >> job = queue.next({"payload.type":"message"})
  >> job.payload
  {"type":"message"}
  >> job.complete()
  >> job = queue.next({"payload.type":"message"})
  >> job
  None
  >> job = queue.next()
  >> job.payload
  {"type":"alert"}
  >> job.complete()

The job class exposes some control methods on the job, for marking progress,
completion, errors, or releasing the job back into the queue.

  - ``complete`` Marks a job as complete and removes it from the queue.
     It will not successfully release the job if it had run for more than ``timeout`` seconds. The queueing mechanism will assume that the job has failed.

  - ``error`` Optionally specified with a message, releases the job back to the queue, and increments its attempts, and stores the error message on the job.
     It has an optional parameter, called ``custom_retry_after``, which supersedes the queue's internal ``retry_after`` property only one time.

  - ``progress`` Optionally takes a progress count integer, notes progress on the job and resets the lock timeout.

  - ``release`` Release a job back to the pool. The attempts counter is not modified.
     It has an optional parameter, called ``custom_retry_after``, which supersedes the queue's internal ``retry_after`` property only one time.


As a convience the job supports the context manager protocol::

  >> with job as data:
  ...   print data['payload']

  {"foobar: 0}

If the context closure is exited without the job is marked complete,
if there's an exception the error is stored on the job.


Inspired By
===========

- [0] https://github.com/skiz/mongo_queue/blob/master/lib/mongo_queue.rb
- [1] http://blog.boxedice.com/2011/09/28/replacing-rabbitmq-with-mongodb/
- [2] http://blog.boxedice.com/2011/04/13/queueing-mongodb-using-mongodb/
- [3] https://github.com/lunaru/mongoqueue
- [4] http://www.captaincodeman.com/2011/05/28/simple-service-bus-message-queue-mongodb/


Running Tests
=============

Unit tests can be run with

 $ python setup.py nosetests

Changes
=======

- 0.7.7 - Dec 29th, 2015 - Added function to repair stale locks on sharded clusters.
- 0.7.6 - Dec 19th, 2015 - Allow to delay failed or re-released jobs.
- 0.7.5 - Nov 30th, 2015 - Allow to query by partial payload message.
- 0.6.0 - Feb 4th, 2013 - Isolate passed in data from metadata in Job.
- 0.5.2 - Dec 9th, 2012 - Fix for regression in sort parameters from pymongo 2.4
- 0.5.1 - Dec 2nd, 2012 - Packaging fix for readme data file.

Credits
=======

- Kapil Thangavelu, author & maintainer
- Dustin Laurence, sort fix for pymongo 2.4
- Jonathan Sackett, Job data isolation.
