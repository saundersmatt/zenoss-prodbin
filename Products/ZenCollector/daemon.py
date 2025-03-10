##############################################################################
#
# Copyright (C) Zenoss, Inc. 2009, 2010, 2012, all rights reserved.
#
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
#
##############################################################################

import json
import logging
import re
import signal
import time

from optparse import SUPPRESS_HELP

import zope.interface

from metrology import Metrology
from metrology.instruments import Gauge
from twisted.internet import defer, reactor, task
from twisted.python.failure import Failure
from zope.component import getUtilitiesFor

from Products.ZenHub.PBDaemon import PBDaemon, FakeRemote
from Products.ZenRRD.RRDDaemon import RRDDaemon
from Products.ZenUtils import metrics
from Products.ZenUtils.Utils import importClass, unused
from Products.ZenUtils.deprecated import deprecated
from Products.ZenUtils.picklezipper import Zipper
from Products.ZenUtils.observable import ObservableProxy

from .interfaces import (
    ICollector,
    ICollectorPreferences,
    IConfigurationDispatchingFilter,
    IConfigurationListener,
    IDataService,
    IEventService,
    IFrameworkFactory,
    IStatistic,
    IStatisticsService,
    ITaskSplitter,
)
from .utils.maintenance import MaintenanceCycle

log = logging.getLogger("zen.daemon")


class DummyListener(object):
    zope.interface.implements(IConfigurationListener)

    def deleted(self, configurationId):
        """
        Called when a configuration is deleted from the collector
        """
        log.debug("DummyListener: configuration %s deleted", configurationId)

    def added(self, configuration):
        """
        Called when a configuration is added to the collector
        """
        log.debug("DummyListener: configuration %s added", configuration)

    def updated(self, newConfiguration):
        """
        Called when a configuration is updated in collector
        """
        log.debug("DummyListener: configuration %s updated", newConfiguration)


class ConfigListenerNotifier(object):
    zope.interface.implements(IConfigurationListener)

    _listeners = []

    def addListener(self, listener):
        self._listeners.append(listener)

    def deleted(self, configurationId):
        """
        Called when a configuration is deleted from the collector
        """
        for listener in self._listeners:
            listener.deleted(configurationId)

    def added(self, configuration):
        """
        Called when a configuration is added to the collector
        """
        for listener in self._listeners:
            listener.added(configuration)

    def updated(self, newConfiguration):
        """
        Called when a configuration is updated in collector
        """
        for listener in self._listeners:
            listener.updated(newConfiguration)


class DeviceGuidListener(object):
    zope.interface.implements(IConfigurationListener)

    def __init__(self, daemon):
        self._daemon = daemon

    def deleted(self, configurationId):
        """
        Called when a configuration is deleted from the collector
        """
        self._daemon._deviceGuids.pop(configurationId, None)

    def added(self, configuration):
        """
        Called when a configuration is added to the collector
        """
        deviceGuid = getattr(configuration, "deviceGuid", None)
        if deviceGuid:
            self._daemon._deviceGuids[configuration.id] = deviceGuid

    def updated(self, newConfiguration):
        """
        Called when a configuration is updated in collector
        """
        deviceGuid = getattr(newConfiguration, "deviceGuid", None)
        if deviceGuid:
            self._daemon._deviceGuids[newConfiguration.id] = deviceGuid


DUMMY_LISTENER = DummyListener()
CONFIG_LOADER_NAME = "configLoader"


class CollectorDaemon(RRDDaemon):
    """
    The daemon class for the entire ZenCollector framework. This class bridges
    the gap between the older daemon framework and ZenCollector. New collectors
    no longer should extend this class to implement a new collector.
    """

    zope.interface.implements(ICollector, IDataService, IEventService)

    _frameworkFactoryName = ""

    # So users (subclasses) can check for metric tag support without inspect.
    metricExtraTags = True

    @property
    def preferences(self):
        """
        Preferences for this daemon
        """
        return self._prefs

    def __init__(
        self,
        preferences,
        taskSplitter,
        configurationListener=DUMMY_LISTENER,
        initializationCallback=None,
        stoppingCallback=None,
    ):
        """
        Constructs a new instance of the CollectorDaemon framework. Normally
        only a singleton instance of a CollectorDaemon should exist within a
        process, but this is not enforced.

        @param preferences: the collector configuration
        @type preferences: ICollectorPreferences
        @param taskSplitter: the task splitter to use for this collector
        @type taskSplitter: ITaskSplitter
        @param initializationCallback: a callable that will be executed after
                                       connection to the hub but before
                                       retrieving configuration information
        @type initializationCallback: any callable
        @param stoppingCallback: a callable that will be executed first during
                                 the stopping process. Exceptions will be
                                 logged but otherwise ignored.
        @type stoppingCallback: any callable
        """
        # create the configuration first, so we have the collector name
        # available before activating the rest of the Daemon class hierarchy.
        if not ICollectorPreferences.providedBy(preferences):
            raise TypeError("configuration must provide ICollectorPreferences")
        else:
            self._prefs = ObservableProxy(preferences)
            self._prefs.attachAttributeObserver(
                "configCycleInterval", self._rescheduleConfig
            )

        if not ITaskSplitter.providedBy(taskSplitter):
            raise TypeError("taskSplitter must provide ITaskSplitter")
        else:
            self._taskSplitter = taskSplitter

        if not IConfigurationListener.providedBy(configurationListener):
            raise TypeError(
                "configurationListener must provide IConfigurationListener"
            )
        self._configListener = ConfigListenerNotifier()
        self._configListener.addListener(configurationListener)
        self._configListener.addListener(DeviceGuidListener(self))
        self._initializationCallback = initializationCallback
        self._stoppingCallback = stoppingCallback

        # register the various interfaces we provide the rest of the system so
        # that collector implementors can easily retrieve a reference back here
        # if needed
        zope.component.provideUtility(self, ICollector)
        zope.component.provideUtility(self, IEventService)
        zope.component.provideUtility(self, IDataService)

        # register the collector's own preferences object so it may be easily
        # retrieved by factories, tasks, etc.
        zope.component.provideUtility(
            self.preferences,
            ICollectorPreferences,
            self.preferences.collectorName,
        )

        super(CollectorDaemon, self).__init__(
            name=self.preferences.collectorName
        )
        self._statService = StatisticsService()
        zope.component.provideUtility(self._statService, IStatisticsService)

        if self.options.cycle:
            # setup daemon statistics (deprecated names)
            self._statService.addStatistic("devices", "GAUGE")
            self._statService.addStatistic("dataPoints", "DERIVE")
            self._statService.addStatistic("runningTasks", "GAUGE")
            self._statService.addStatistic("taskCount", "GAUGE")
            self._statService.addStatistic("queuedTasks", "GAUGE")
            self._statService.addStatistic("missedRuns", "GAUGE")

            # namespace these a bit so they can be used in ZP monitoring.
            # prefer these stat names and metrology in future refs
            self._dataPointsMetric = Metrology.meter(
                "collectordaemon.dataPoints"
            )
            daemon = self

            class DeviceGauge(Gauge):
                @property
                def value(self):
                    return len(daemon._devices)

            Metrology.gauge("collectordaemon.devices", DeviceGauge())

            # Scheduler statistics
            class RunningTasks(Gauge):
                @property
                def value(self):
                    return daemon._scheduler._executor.running

            Metrology.gauge("collectordaemon.runningTasks", RunningTasks())

            class TaskCount(Gauge):
                @property
                def value(self):
                    return daemon._scheduler.taskCount

            Metrology.gauge("collectordaemon.taskCount", TaskCount())

            class QueuedTasks(Gauge):
                @property
                def value(self):
                    return daemon._scheduler._executor.queued

            Metrology.gauge("collectordaemon.queuedTasks", QueuedTasks())

            class MissedRuns(Gauge):
                @property
                def value(self):
                    return daemon._scheduler.missedRuns

            Metrology.gauge("collectordaemon.missedRuns", MissedRuns())

        self._deviceGuids = {}
        self._devices = set()
        self._unresponsiveDevices = set()
        self._rrd = None
        self._metric_writer = None
        self._derivative_tracker = None
        self.reconfigureTimeout = None

        # keep track of pending tasks if we're doing a single run, and not a
        # continuous cycle
        if not self.options.cycle:
            self._completedTasks = 0
            self._pendingTasks = []

        frameworkFactory = zope.component.queryUtility(
            IFrameworkFactory, self._frameworkFactoryName
        )
        self._configProxy = frameworkFactory.getConfigurationProxy()
        self._scheduler = frameworkFactory.getScheduler()
        self._scheduler.maxTasks = self.options.maxTasks
        self._ConfigurationLoaderTask = (
            frameworkFactory.getConfigurationLoaderTask()
        )

        # OLD - set the initialServices attribute so that the PBDaemon class
        # will load all of the remote services we need.
        self.initialServices = PBDaemon.initialServices + [
            self.preferences.configurationService
        ]

        # trap SIGUSR2 so that we can display detailed statistics
        signal.signal(signal.SIGUSR2, self._signalHandler)

        # let the configuration do any additional startup it might need
        self.preferences.postStartup()
        self.addedPostStartupTasks = False

        # Variables used by enterprise collector in resmgr
        #
        # flag that indicates we have finished loading the configs for the
        # first time after a restart
        self.firstConfigLoadDone = False
        # flag that indicates the daemon has received the encryption key
        # from zenhub
        self.encryptionKeyInitialized = False
        # flag that indicates the daemon is loading the cached configs
        self.loadingCachedConfigs = False

    def buildOptions(self):
        """
        Method called by CmdBase.__init__ to build all of the possible
        command-line options for this collector daemon.
        """
        super(CollectorDaemon, self).buildOptions()

        maxTasks = getattr(self.preferences, "maxTasks", None)
        defaultMax = maxTasks if maxTasks else 500

        self.parser.add_option(
            "--maxparallel",
            dest="maxTasks",
            type="int",
            default=defaultMax,
            help="Max number of tasks to run at once, default %default",
        )
        self.parser.add_option(
            "--logTaskStats",
            dest="logTaskStats",
            type="int",
            default=0,
            help="How often to logs statistics of current tasks, "
            "value in seconds; very verbose",
        )
        addWorkerOptions(self.parser)
        self.parser.add_option(
            "--traceMetricName",
            dest="traceMetricName",
            type="string",
            default=None,
            help="trace metrics whose name matches this regex",
        )
        self.parser.add_option(
            "--traceMetricKey",
            dest="traceMetricKey",
            type="string",
            default=None,
            help="trace metrics whose key value matches this regex",
        )

        frameworkFactory = zope.component.queryUtility(
            IFrameworkFactory, self._frameworkFactoryName
        )
        if hasattr(frameworkFactory, "getFrameworkBuildOptions"):
            # During upgrades we'll be missing this option
            self._frameworkBuildOptions = (
                frameworkFactory.getFrameworkBuildOptions()
            )
            if self._frameworkBuildOptions:
                self._frameworkBuildOptions(self.parser)

        # give the collector configuration a chance to add options, too
        self.preferences.buildOptions(self.parser)

    def parseOptions(self):
        super(CollectorDaemon, self).parseOptions()
        self.preferences.options = self.options

        configFilter = parseWorkerOptions(self.options.__dict__)
        if configFilter:
            self.preferences.configFilter = configFilter

    def connected(self):
        """
        Method called by PBDaemon after a connection to ZenHub is established.
        """
        return self._startup()

    def _getInitializationCallback(self):
        def doNothing():
            pass

        if self._initializationCallback is not None:
            return self._initializationCallback
        else:
            return doNothing

    def connectTimeout(self):
        super(CollectorDaemon, self).connectTimeout()
        return self._startup()

    def _startup(self):
        d = defer.maybeDeferred(self._getInitializationCallback())
        d.addCallback(self._initEncryptionKey)
        d.addCallback(self._startConfigCycle)
        d.addCallback(self._startMaintenance)
        d.addErrback(self._errorStop)
        return d

    @defer.inlineCallbacks
    def _initEncryptionKey(self, prv_cb_result=None):
        # encrypt dummy msg in order to initialize the encryption key
        data = yield self._configProxy.encrypt(
            "Hello"
        )  # block until we get the key
        if data:  # encrypt returns None if an exception is raised
            self.encryptionKeyInitialized = True
            self.log.info("Daemon's encryption key initialized")

    def watchdogCycleTime(self):
        """
        Return our cycle time (in minutes)

        @return: cycle time
        @rtype: integer
        """
        return self.preferences.cycleInterval * 2

    def getRemoteConfigServiceProxy(self):
        """
        Called to retrieve the remote configuration service proxy object.
        """
        return self.services.get(
            self.preferences.configurationService, FakeRemote()
        )

    def generateEvent(self, event, **kw):
        eventCopy = super(CollectorDaemon, self).generateEvent(event, **kw)
        if eventCopy and eventCopy.get("device"):
            device_id = eventCopy.get("device")
            guid = self._deviceGuids.get(device_id)
            if guid:
                eventCopy["device_guid"] = guid
        return eventCopy

    def should_trace_metric(self, metric, contextkey):
        """
        Tracer implementation - use this function to indicate whether a given
        metric/contextkey combination is to be traced.
        :param metric: name of the metric in question
        :param contextkey: context key of the metric in question
        :return: boolean indicating whether to trace this metric/key
        """
        tests = []
        if self.options.traceMetricName:
            tests.append((self.options.traceMetricName, metric))
        if self.options.traceMetricKey:
            tests.append((self.options.traceMetricKey, contextkey))

        result = [bool(re.search(exp, subj)) for exp, subj in tests]

        return len(result) > 0 and all(result)

    @defer.inlineCallbacks
    def writeMetric(
        self,
        contextKey,
        metric,
        value,
        metricType,
        contextId,
        timestamp="N",
        min="U",
        max="U",
        threshEventData=None,
        deviceId=None,
        contextUUID=None,
        deviceUUID=None,
        extraTags=None,
    ):

        """
        Writes the metric to the metric publisher.
        @param contextKey: This is who the metric applies to. This is usually
                            the return value of rrdPath() for a component or
                            device.
        @param metric: the name of the metric, we expect it to be of the form
            datasource_datapoint
        @param value: the value of the metric
        @param metricType: type of the metric (e.g. 'COUNTER', 'GAUGE',
            'DERIVE' etc)
        @param contextId: used for the threshold events, the id of who this
            metric is for
        @param timestamp: defaults to time.time() if not specified,
            the time the metric occurred
        @param min: used in the derive the min value for the metric
        @param max: used in the derive the max value for the metric
        @param threshEventData: extra data put into threshold events
        @param deviceId: the id of the device for this metric
        @return: a deferred that fires when the metric gets published
        """
        timestamp = int(time.time()) if timestamp == "N" else timestamp
        tags = {"contextUUID": contextUUID, "key": contextKey}
        if self.should_trace_metric(metric, contextKey):
            tags["mtrace"] = "{}".format(int(time.time()))

        metric_name = metric
        if deviceId:
            tags["device"] = deviceId

        # compute (and cache) a rate for COUNTER/DERIVE
        if metricType in {"COUNTER", "DERIVE"}:
            if metricType == "COUNTER" and min == "U":
                # COUNTER implies only positive derivatives are valid.
                min = 0

            dkey = "%s:%s" % (contextUUID, metric)
            value = self._derivative_tracker.derivative(
                dkey, (float(value), timestamp), min, max
            )

        # check for threshold breaches and send events when needed
        if value is not None:
            if extraTags:
                tags.update(extraTags)

            # write the  metric to Redis
            try:
                yield defer.maybeDeferred(
                    self._metric_writer.write_metric,
                    metric_name,
                    value,
                    timestamp,
                    tags,
                )
            except Exception as e:
                self.log.debug("Error sending metric %s", e)
            yield defer.maybeDeferred(
                self._threshold_notifier.notify,
                contextUUID,
                contextId,
                metric,
                timestamp,
                value,
                threshEventData,
            )

    def writeMetricWithMetadata(
        self,
        metric,
        value,
        metricType,
        timestamp="N",
        min="U",
        max="U",
        threshEventData=None,
        metadata=None,
        extraTags=None,
    ):

        metadata = metadata or {}
        try:
            key = metadata["contextKey"]
            contextId = metadata["contextId"]
            deviceId = metadata["deviceId"]
            contextUUID = metadata["contextUUID"]
            if metadata:
                metric_name = metrics.ensure_prefix(metadata, metric)
            else:
                metric_name = metric
        except KeyError as e:
            raise Exception("Missing necessary metadata: %s" % e.message)

        return self.writeMetric(
            key,
            metric_name,
            value,
            metricType,
            contextId,
            timestamp=timestamp,
            min=min,
            max=max,
            threshEventData=threshEventData,
            deviceId=deviceId,
            contextUUID=contextUUID,
            deviceUUID=metadata.get("deviceUUID"),
            extraTags=extraTags,
        )

    @deprecated
    def writeRRD(
        self,
        path,
        value,
        rrdType,
        rrdCommand=None,
        cycleTime=None,
        min="U",
        max="U",
        threshEventData={},
        timestamp="N",
        allowStaleDatapoint=True,
    ):
        """
        Use writeMetric
        """
        # We rely on the fact that rrdPath now returns more information than
        # just the path
        metricinfo, metric = path.rsplit("/", 1)
        if "METRIC_DATA" not in str(metricinfo):
            raise Exception(
                "Unable to write Metric with given path { %s } "
                "please see the rrdpath method"
                % (metricinfo,)
            )

        metadata = json.loads(metricinfo)
        # reroute to new writeMetric method
        return self.writeMetricWithMetadata(
            metric,
            value,
            rrdType,
            timestamp,
            min,
            max,
            threshEventData,
            metadata,
        )

    def stop(self, ignored=""):
        if self._stoppingCallback is not None:
            try:
                self._stoppingCallback()
            except Exception:
                self.log.exception("Exception while stopping daemon")
        super(CollectorDaemon, self).stop(ignored)

    def remote_deleteDevice(self, devId):
        """
        Called remotely by ZenHub when a device we're monitoring is deleted.
        """
        # guard against parsing updates during a disconnect
        if devId is None:
            return
        self._deleteDevice(devId)

    def remote_deleteDevices(self, deviceIds):
        """
        Called remotely by ZenHub when devices we're monitoring are deleted.
        """
        # guard against parsing updates during a disconnect
        if deviceIds is None:
            return
        for devId in Zipper.load(deviceIds):
            self._deleteDevice(devId)

    def remote_updateDeviceConfig(self, config):
        """
        Called remotely by ZenHub when asynchronous configuration updates
        occur.
        """
        # guard against parsing updates during a disconnect
        if config is None:
            return
        self.log.debug("Device %s updated", config.configId)
        if self._updateConfig(config):
            self._configProxy.updateConfigProxy(self.preferences, config)
        else:
            self.log.debug("Device %s config filtered", config.configId)

    def remote_updateDeviceConfigs(self, configs):
        """
        Called remotely by ZenHub when asynchronous configuration updates
        occur.
        """
        if configs is None:
            return
        configs = Zipper.load(configs)
        self.log.debug(
            "remote_updateDeviceConfigs: workerid %s processing %s "
            "device configs",
            self.options.workerid,
            len(configs),
        )
        for config in configs:
            self.remote_updateDeviceConfig(config)

    def remote_notifyConfigChanged(self):
        """
        Called from zenhub to notify that the entire config should be updated
        """
        if self.reconfigureTimeout and self.reconfigureTimeout.active():
            # We will run along with the already scheduled task
            self.log.debug("notifyConfigChanged - using existing call")
            return

        self.log.debug("notifyConfigChanged - scheduling call in 30 seconds")
        self.reconfigureTimeout = reactor.callLater(30, self._rebuildConfig)

    def _rebuildConfig(self):
        """
        Delete and re-add the configuration tasks to completely re-build
        the configuration.
        """
        if self.reconfigureTimeout and not self.reconfigureTimeout.active():
            self.reconfigureTimeout = None
        self._scheduler.removeTasksForConfig(CONFIG_LOADER_NAME)
        self._startConfigCycle()

    def _rescheduleConfig(
        self, observable, attrName, oldValue, newValue, **kwargs
    ):
        """
        Delete and re-add the configuration tasks to start on new interval.
        """
        if oldValue != newValue:
            self.log.debug(
                "Changing config task interval from %s to %s minutes",
                oldValue,
                newValue,
            )
            self._scheduler.removeTasksForConfig(CONFIG_LOADER_NAME)
            # values are in minutes, scheduler takes seconds
            self._startConfigCycle(startDelay=newValue * 60)

    def _taskCompleteCallback(self, taskName):
        # if we're not running a normal daemon cycle then we need to shutdown
        # once all of our pending tasks have completed
        if not self.options.cycle:
            try:
                self._pendingTasks.remove(taskName)
            except ValueError:
                pass

            self._completedTasks += 1

            # if all pending tasks have been completed then shutdown the daemon
            if len(self._pendingTasks) == 0:
                self._displayStatistics()
                self.stop()

    def _updateConfig(self, cfg):
        """
        Update device configuration. Returns true if config is updated,
        false if config is skipped.
        """
        # guard against parsing updates during a disconnect
        if cfg is None:
            return False
        configFilter = getattr(self.preferences, "configFilter", None) or (
            lambda x: True
        )
        if not (
            (not self.options.device and configFilter(cfg))
            or self.options.device in (cfg.id, cfg.configId)
        ):
            self.log.info("Device %s config filtered", cfg.configId)
            return False

        configId = cfg.configId
        self.log.debug("Processing configuration for %s", configId)

        nextExpectedRuns = {}
        if configId in self._devices:
            tasksToRemove = self._scheduler.getTasksForConfig(configId)
            nextExpectedRuns = {
                taskToRemove.name: self._scheduler.getNextExpectedRun(
                    taskToRemove.name
                )
                for taskToRemove in tasksToRemove
            }
            self._scheduler.removeTasks(task.name for task in tasksToRemove)
            self._configListener.updated(cfg)
        else:
            self._devices.add(configId)
            self._configListener.added(cfg)

        newTasks = self._taskSplitter.splitConfiguration([cfg])
        self.log.debug("Tasks for config %s: %s", configId, newTasks)

        nowTime = time.time()
        for (taskName, task_) in newTasks.iteritems():
            # if not cycling run the task immediately otherwise let the
            # scheduler decide when to run the task
            now = not self.options.cycle
            nextExpectedRun = nextExpectedRuns.get(taskName, None)
            if nextExpectedRun:
                startDelay = nextExpectedRun - nowTime
                if startDelay <= 0:
                    # handle edge case where we are about to run
                    # so run immediately
                    now = True
                    task_.startDelay = 0
                else:
                    task_.startDelay = startDelay
            try:
                self._scheduler.addTask(task_, self._taskCompleteCallback, now)
            except ValueError:
                self.log.exception("Error adding device config")
                continue

            # TODO: another hack?
            if hasattr(cfg, "thresholds"):
                self.getThresholds().updateForDevice(configId, cfg.thresholds)

            # if we're not running a normal daemon cycle then keep track of the
            # tasks we just added for this device so that we can shutdown once
            # all pending tasks have completed
            if not self.options.cycle:
                self._pendingTasks.append(taskName)
        # Put tasks on pause after configuration update to prevent
        # unnecessary collections ZEN-25463
        if configId in self._unresponsiveDevices:
            self.log.debug("Pausing tasks for device %s", configId)
            self._scheduler.pauseTasksForConfig(configId)

        return True

    @defer.inlineCallbacks
    def _updateDeviceConfigs(self, updatedConfigs, purgeOmitted):
        """
        Update the device configurations for the devices managed by this
        collector.
        @param deviceConfigs a list of device configurations
        @type deviceConfigs list of name,value tuples
        """
        self.log.debug(
            "updateDeviceConfigs: updatedConfigs=%s",
            (map(str, updatedConfigs)),
        )

        for cfg in updatedConfigs:
            self._updateConfig(cfg)
            # yield time to reactor so other things can happen
            yield task.deferLater(reactor, 0, lambda: None)

        if purgeOmitted:
            self._purgeOmittedDevices(cfg.configId for cfg in updatedConfigs)

    def _purgeOmittedDevices(self, updatedDevices):
        """
        Delete all current devices that are omitted from the list of devices
        being updated.
        @param updatedDevices a collection of device ids
        @type updatedDevices a sequence of strings
        """
        # remove tasks for the deleted devices
        deletedDevices = set(self._devices) - set(updatedDevices)
        self.log.debug(
            "purgeOmittedDevices: deletedConfigs=%s", ",".join(deletedDevices)
        )
        for configId in deletedDevices:
            self._deleteDevice(configId)

    def _deleteDevice(self, deviceId):
        self.log.debug("Device %s deleted", deviceId)

        self._devices.discard(deviceId)
        self._configListener.deleted(deviceId)
        self._configProxy.deleteConfigProxy(self.preferences, deviceId)
        self._scheduler.removeTasksForConfig(deviceId)

    def _errorStop(self, result):
        """
        Twisted callback to receive fatal messages.

        @param result: the Twisted failure
        @type result: failure object
        """
        if isinstance(result, Failure):
            msg = result.getErrorMessage()
        else:
            msg = str(result)
        self.log.critical("Unrecoverable Error: %s", msg)
        self.stop()

    def _startConfigCycle(self, result=None, startDelay=0):
        configLoader = self._ConfigurationLoaderTask(
            CONFIG_LOADER_NAME, taskConfig=self.preferences
        )
        configLoader.startDelay = startDelay
        # Don't add the config loader task if the scheduler already has
        # an instance of it.
        if configLoader not in self._scheduler:
            # Run initial maintenance cycle as soon as possible
            # TODO: should we not run maintenance if running in non-cycle mode?
            self._scheduler.addTask(configLoader)
        else:
            self.log.info("%s already added to scheduler", configLoader.name)
        return defer.succeed("Configuration loader task started")

    def setPropertyItems(self, items):
        """
        Override so that preferences are updated
        """
        super(CollectorDaemon, self).setPropertyItems(items)
        self._setCollectorPreferences(dict(items))

    def _setCollectorPreferences(self, preferenceItems):
        for name, value in preferenceItems.iteritems():
            if not hasattr(self.preferences, name):
                # TODO: make a super-low level debug mode?  The following
                # message isn't helpful
                # self.log.debug(
                #     "Preferences object does not have attribute %s", name
                # )
                setattr(self.preferences, name, value)
            elif getattr(self.preferences, name) != value:
                self.log.debug("Updated %s preference to %s", name, value)
                setattr(self.preferences, name, value)

    def _loadThresholdClasses(self, thresholdClasses):
        self.log.debug("Loading classes %s", thresholdClasses)
        for c in thresholdClasses:
            try:
                importClass(c)
            except ImportError:
                log.exception("Unable to import class %s", c)

    def _configureThresholds(self, thresholds):
        self.getThresholds().updateList(thresholds)

    def _startMaintenance(self, ignored=None):
        unused(ignored)
        if not self.options.cycle:
            self._maintenanceCycle()
            return
        if self.options.logTaskStats > 0:
            log.debug("Starting Task Stat logging")
            loop = task.LoopingCall(self._displayStatistics, verbose=True)
            loop.start(self.options.logTaskStats, now=False)

        interval = self.preferences.cycleInterval
        self.log.debug("Initializing maintenance Cycle")
        heartbeatSender = self if self.worker_id == 0 else None
        maintenanceCycle = MaintenanceCycle(
            interval, heartbeatSender, self._maintenanceCycle
        )
        maintenanceCycle.start()

    @defer.inlineCallbacks
    def _maintenanceCycle(self, ignored=None):
        """
        Perform daemon maintenance processing on a periodic schedule. Initially
        called after the daemon configuration loader task is added,
        but afterward will self-schedule each run.
        """
        try:
            self.log.debug("Performing periodic maintenance")
            if not self.options.cycle:
                ret = "No maintenance required"
            elif getattr(self.preferences, "pauseUnreachableDevices", True):
                # TODO: handle different types of device issues
                ret = yield self._pauseUnreachableDevices()
            else:
                ret = None
            defer.returnValue(ret)
        except Exception:
            self.log.exception("failure in _maintenanceCycle")
            raise

    @defer.inlineCallbacks
    def _pauseUnreachableDevices(self):
        issues = yield self.getDevicePingIssues()
        self.log.debug("deviceIssues=%r", issues)
        if issues is None:
            defer.returnValue(issues)  # exception or some other problem

        # Device ping issues returns as a tuple of (deviceId, count, total)
        # and we just want the device id
        newUnresponsiveDevices = set(i[0] for i in issues)

        clearedDevices = self._unresponsiveDevices.difference(
            newUnresponsiveDevices
        )
        for devId in clearedDevices:
            self.log.debug("Resuming tasks for device %s", devId)
            self._scheduler.resumeTasksForConfig(devId)

        self._unresponsiveDevices = newUnresponsiveDevices
        for devId in self._unresponsiveDevices:
            self.log.debug("Pausing tasks for device %s", devId)
            self._scheduler.pauseTasksForConfig(devId)

        defer.returnValue(issues)

    def runPostConfigTasks(self, result=None):
        """
        Add post-startup tasks from the preferences.

        This may be called with the failure code as well.
        """
        if isinstance(result, Failure):
            pass

        elif not self.addedPostStartupTasks:
            postStartupTasks = getattr(
                self.preferences, "postStartupTasks", lambda: []
            )
            for task_ in postStartupTasks():
                self._scheduler.addTask(task_, now=True)
            self.addedPostStartupTasks = True

    def postStatisticsImpl(self):
        self._displayStatistics()

        # update and post statistics if we've been configured to do so
        if self.rrdStats:
            stat = self._statService.getStatistic("devices")
            stat.value = len(self._devices)

            # stat = self._statService.getStatistic("cyclePoints")
            # stat.value = self._rrd.endCycle()

            stat = self._statService.getStatistic("dataPoints")
            stat.value = self.metricWriter().dataPoints

            # Scheduler statistics
            stat = self._statService.getStatistic("runningTasks")
            stat.value = self._scheduler._executor.running

            stat = self._statService.getStatistic("taskCount")
            stat.value = self._scheduler.taskCount

            stat = self._statService.getStatistic("queuedTasks")
            stat.value = self._scheduler._executor.queued

            stat = self._statService.getStatistic("missedRuns")
            stat.value = self._scheduler.missedRuns

            diff = (
                self.metricWriter().dataPoints - self._dataPointsMetric.count
            )
            self._dataPointsMetric.mark(diff)

            self._statService.postStatistics(self.rrdStats)

    def _displayStatistics(self, verbose=False):
        if self.metricWriter():
            self.log.info(
                "%d devices processed (%d datapoints)",
                len(self._devices),
                self.metricWriter().dataPoints,
            )
        else:
            self.log.info(
                "%d devices processed (0 datapoints)", len(self._devices)
            )

        self._scheduler.displayStatistics(verbose)

    def _signalHandler(self, signum, frame):
        self._displayStatistics(True)

    @property
    def worker_count(self):
        """
        worker_count for this daemon
        """
        return getattr(self.options, "workers", 1)

    @property
    def worker_id(self):
        """
        worker_id for this particular peer
        """
        return getattr(self.options, "workerid", 0)


class Statistic(object):
    zope.interface.implements(IStatistic)

    def __init__(self, name, type, **kwargs):
        self.value = 0
        self.name = name
        self.type = type
        self.kwargs = kwargs


class StatisticsService(object):
    zope.interface.implements(IStatisticsService)

    def __init__(self):
        self._stats = {}

    def addStatistic(self, name, type, **kwargs):
        if name in self._stats:
            raise NameError("Statistic %s already exists" % name)

        if type not in ("DERIVE", "COUNTER", "GAUGE"):
            raise TypeError("Statistic type %s not supported" % type)

        stat = Statistic(name, type, **kwargs)
        self._stats[name] = stat

    def getStatistic(self, name):
        return self._stats[name]

    def postStatistics(self, rrdStats):
        for stat in self._stats.values():
            # figure out which function to use to post this statistical data
            try:
                func = {
                    "COUNTER": rrdStats.counter,
                    "GAUGE": rrdStats.gauge,
                    "DERIVE": rrdStats.derive,
                }[stat.type]
            except KeyError:
                raise TypeError("Statistic type %s not supported" % stat.type)

            # These should always come back empty now because DaemonStats
            # posts the events for us
            func(stat.name, stat.value, **stat.kwargs)

            # counter is an ever-increasing value, but otherwise...
            if stat.type != "COUNTER":
                stat.value = 0


def addWorkerOptions(parser):
    parser.add_option(
        "--dispatch", dest="configDispatch", type="string", help=SUPPRESS_HELP
    )
    parser.add_option(
        "--workerid",
        dest="workerid",
        type="int",
        default=0,
        help=SUPPRESS_HELP,
    )
    parser.add_option("--workers", type="int", default=1, help=SUPPRESS_HELP)


def parseWorkerOptions(options):
    dispatchFilterName = options.get("configDispatch", "") if options else ""
    filterFactories = dict(getUtilitiesFor(IConfigurationDispatchingFilter))
    filterFactory = filterFactories.get(
        dispatchFilterName, None
    ) or filterFactories.get("", None)
    if filterFactory:
        filter = filterFactory.getFilter(options)
        log.debug("Filter configured: %s:%s", filterFactory, filter)
        return filter
