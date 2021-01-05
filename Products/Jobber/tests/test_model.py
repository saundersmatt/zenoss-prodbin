##############################################################################
#
# Copyright (C) Zenoss, Inc. 2019, all rights reserved.
#
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
#
##############################################################################

from __future__ import absolute_import, print_function

import itertools

from celery import states
from mock import patch
from unittest import TestCase

from Products.Zuul.interfaces import IMarshallable
from zope.component import getGlobalSiteManager

from ..jobs import Job
from ..model import (
    build_redis_record, IJobRecord, IJobStore, JobRecord, update_job_status,
)
from ..storage import JobStore, Fields
from ..zenjobs import app
from .utils import subTest, RedisLayer

UNEXPECTED = type("UNEXPECTED", (object,), {})()
PATH = {"src": "Products.Jobber.model"}


class JobRecordTest(TestCase):
    """Test the JobRecord class."""

    def setUp(t):
        t.data = {
            "jobid": "123",
            "name": "Products.Jobber.jobs.FooJob",
            "summary": "FooJob",
            "description": "A foo job",
            "userid": "zenoss",
            "logfile": "/opt/zenoss/log/jobs/123.log",
            "created": 1551804881.024517,
            "started": 1551804891.863857,
            "finished": 1551805270.154359,
            "status": "SUCCESS",
            "details": {
                "foo": "foo string",
                "bar": 123,
                "baz": 34597945.00234,
            },
        }

    def test_interfaces(t):
        for intf in (IJobRecord, IMarshallable):
            with subTest(interface=intf):
                t.assertTrue(intf.implementedBy(JobRecord))
                j = JobRecord.make({})
                t.assertTrue(intf.providedBy(j))

    def test_attributes(t):
        # Assert that JobRecord has all the attributes specified by Fields.
        j = JobRecord.make({})
        missing_names = set(Fields.viewkeys()) - set(dir(j))
        t.assertSetEqual(set(), missing_names)

    def test_make_badfield(t):
        with t.assertRaises(AttributeError):
            JobRecord.make({"foo": 1})

    def test_uuid(t):
        job = JobRecord.make(t.data)
        t.assertEqual(job.jobid, job.uuid)

    def test_make(t):
        actual = JobRecord.make(t.data)
        expected = itertools.chain(
            t.data.items(),
            (
                ("complete", True),
                ("duration", t.data["finished"] - t.data["started"]),
            ),
        )
        for attribute, expected_value in expected:
            actual_value = getattr(actual, attribute, UNEXPECTED)
            with subTest(attribute=attribute):
                t.assertEqual(expected_value, actual_value)

    def test_complete(t):
        parameters = {
            ("PENDING", False),
            ("RETRY", False),
            ("RECEIVED", False),
            ("STARTED", False),
            ("REVOKED", True),
            ("ABORTED", True),
            ("FAILURE", True),
            ("SUCCESS", True),
        }
        record = JobRecord.make(t.data)
        for status, expected in parameters:
            with subTest(status=status):
                record.status = status
                t.assertEqual(
                    expected, record.complete, msg="status=%s" % status,
                )

    @patch("{src}.time".format(**PATH), autospec=True)
    def test_duration(t, time):
        current_tm = 1551804885.298103
        completed_duration = t.data["finished"] - t.data["started"]
        incomplete_duration = current_tm - t.data["started"]
        time.time.return_value = current_tm
        parameters = {
            ("PENDING", None),
            ("RETRY", incomplete_duration),
            ("RECEIVED", None),
            ("STARTED", incomplete_duration),
            ("REVOKED", completed_duration),
            ("ABORTED", completed_duration),
            ("FAILURE", completed_duration),
            ("SUCCESS", completed_duration),
        }
        record = JobRecord.make(t.data)
        for status, expected in parameters:
            with subTest(status=status):
                record.status = status
                t.assertEqual(expected, record.duration)


class BaseBuildRedisRecord(object):

    def setUp(t):
        t.args = (10,)
        t.kw = {"named": "charger"}
        t.jobid = "12345"
        t.expected = {
            "logfile": "/opt/zenoss/log/jobs/%s.log" % t.jobid,
            "description": t.task.description_from(*t.args, **t.kw),
            "summary": t.task.summary,
            "name": t.task.name,
            "jobid": t.jobid,
        }

    def tearDown(t):
        del t.task
        del t.args
        del t.kw
        del t.jobid
        del t.expected

    def test_bad_jobid(t):
        with t.assertRaises(ValueError):
            build_redis_record(t.task, None, (), {})

    def test_minimum_args(t):
        actual = build_redis_record(t.task, t.jobid, t.args, t.kw)
        t.assertDictEqual(t.expected, actual)

    def test_non_default_description(t):
        description = "alternate description"
        t.expected["description"] = description
        actual = build_redis_record(
            t.task, t.jobid, t.args, t.kw, description=description,
        )
        t.assertDictEqual(t.expected, actual)

    def test_status(t):
        status = "PENDING"
        t.expected["status"] = status
        actual = build_redis_record(
            t.task, t.jobid, t.args, t.kw, status=status,
        )
        t.assertDictEqual(t.expected, actual)

    def test_created(t):
        created = 1234970434.303
        t.expected["created"] = created
        actual = build_redis_record(
            t.task, t.jobid, t.args, t.kw, created=created,
        )
        t.assertDictEqual(t.expected, actual)

    def test_userid(t):
        userid = "someuser"
        t.expected["userid"] = userid
        actual = build_redis_record(
            t.task, t.jobid, t.args, t.kw, userid=userid,
        )
        t.assertDictEqual(t.expected, actual)

    def test_details(t):
        details = {"a": 1, "b": 2}
        t.expected["details"] = details
        actual = build_redis_record(
            t.task, t.jobid, t.args, t.kw, details=details,
        )
        t.assertDictEqual(t.expected, actual)

    def test_all_defaulted_args(t):
        status = "PENDING"
        created = 1234970434.303
        userid = "someuser"
        details = {"a": 1, "b": 2}
        t.expected.update({
            "status": status,
            "created": created,
            "userid": userid,
            "details": details,
        })
        actual = build_redis_record(
            t.task, t.jobid, t.args, t.kw,
            status=status, created=created, userid=userid, details=details,
        )
        t.assertDictEqual(t.expected, actual)


class BuildRedisRecordFromJobTest(BaseBuildRedisRecord, TestCase):
    """Test the build_redis_record function with a Job."""

    class TestJob(Job):

        @classmethod
        def getJobType(cls):
            return "Test Job"

        @classmethod
        def getJobDescription(cls, *args, **kw):
            return "TestJob %s %s" % (args, kw)

    def setUp(t):
        t.task = t.TestJob()
        BaseBuildRedisRecord.setUp(t)


class BuildRedisRecordFromZenTaskTest(BaseBuildRedisRecord, TestCase):
    """Test the build_redis_record function with a ZenTask."""

    @app.task(
        bind=True,
        name="zen.zenjobs.test.test_task",
        summary="Test ZenTask",
        description_template="Test {0} named={named}",
    )
    def noop_task(self, *args, **kw):
        pass

    def setUp(t):
        t.task = t.noop_task
        BaseBuildRedisRecord.setUp(t)


class UpdateJobStatusTest(TestCase):

    layer = RedisLayer

    initial = {
        "jobid": "123",
        "name": "TestJob",
        "summary": "Products.Jobber.jobs.TestJob",
        "description": "A test job",
        "userid": "zenoss",
        "logfile": "/opt/zenoss/log/jobs/123.log",
        "created": 1551804881.024517,
        "status": "PENDING",
    }

    def setUp(t):
        t.store = JobStore(t.layer.redis)
        t.store[t.initial["jobid"]] = t.initial
        getGlobalSiteManager().registerUtility(
            t.store, IJobStore, name="redis",
        )

    def tearDown(t):
        t.layer.redis.flushall()
        getGlobalSiteManager().unregisterUtility(
            t.store, IJobStore, name="redis",
        )
        del t.store

    @patch("Products.Jobber.model.app.backend", autospec=True)
    def test_no_such_task(t, _backend):
        update_job_status("1")
        _backend.get_status.assert_not_called()

    @patch("Products.Jobber.model.time", autospec=True)
    @patch("Products.Jobber.model.app.backend", autospec=True)
    def test_unready_state(t, _backend, _time):
        tm = 1597059131.762538
        _backend.get_status.return_value = states.STARTED
        _time.time.return_value = tm

        expected_status = states.STARTED
        expected_started = tm
        expected_finished = None

        jobid = t.initial["jobid"]
        update_job_status(jobid)

        status = t.store.getfield(jobid, "status")
        started = t.store.getfield(jobid, "started")
        finished = t.store.getfield(jobid, "finished")

        t.assertEqual(expected_status, status)
        t.assertEqual(expected_started, started)
        t.assertEqual(expected_finished, finished)

    @patch("Products.Jobber.model.time", autospec=True)
    @patch("Products.Jobber.model.app.backend", autospec=True)
    def test_ready_state(t, _backend, _time):
        tm = 1597059131.762538
        _backend.get_status.return_value = states.SUCCESS
        _time.time.return_value = tm

        expected_status = states.SUCCESS
        expected_started = None
        expected_finished = tm

        jobid = t.initial["jobid"]
        update_job_status(jobid)

        status = t.store.getfield(jobid, "status")
        started = t.store.getfield(jobid, "started")
        finished = t.store.getfield(jobid, "finished")

        t.assertEqual(expected_status, status)
        t.assertEqual(expected_started, started)
        t.assertEqual(expected_finished, finished)

    @patch("Products.Jobber.model.time", autospec=True)
    @patch("Products.Jobber.model.app.backend", autospec=True)
    def test_task_aborted_state(t, _backend, _time):
        tm = 1597059131.762538
        _backend.get_status.return_value = states.ABORTED
        _time.time.return_value = tm

        expected_status = states.ABORTED
        expected_started = None
        expected_finished = tm

        jobid = t.initial["jobid"]
        update_job_status(jobid)

        status = t.store.getfield(jobid, "status")
        started = t.store.getfield(jobid, "started")
        finished = t.store.getfield(jobid, "finished")

        t.assertEqual(expected_status, status)
        t.assertEqual(expected_started, started)
        t.assertEqual(expected_finished, finished)

    @patch("Products.Jobber.model.time", autospec=True)
    @patch("Products.Jobber.model.app.backend", autospec=True)
    def test_job_aborted_state(t, _backend, _time):
        tm = 1597059131.762538
        _backend.get_status.return_value = states.FAILURE
        _time.time.return_value = tm

        jobid = t.initial["jobid"]
        t.store.update(jobid, status=states.ABORTED)

        expected_status = states.ABORTED
        expected_started = None
        expected_finished = tm

        update_job_status(jobid)

        status = t.store.getfield(jobid, "status")
        started = t.store.getfield(jobid, "started")
        finished = t.store.getfield(jobid, "finished")

        t.assertEqual(expected_status, status)
        t.assertEqual(expected_started, started)
        t.assertEqual(expected_finished, finished)
