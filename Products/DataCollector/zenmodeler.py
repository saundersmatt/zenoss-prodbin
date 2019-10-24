##############################################################################
#
# Copyright (C) Zenoss, Inc. 2007, all rights reserved.
#
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
#
##############################################################################

from __future__ import absolute_import

"""Discover (aka model) a device and its components.
For instance, find out what Ethernet interfaces and hard disks a server
has available.
This information should change much less frequently than performance metrics.
"""

import collections
import cPickle as pickle
import DateTime
import gzip
import os
import re
import socket
import sys
import time
import traceback
import zope.component

from itertools import chain
from metrology import Metrology
from random import randint
from twisted.internet import reactor
from twisted.internet.defer import succeed, inlineCallbacks

import Globals  # noqa: F401

from Products.DataCollector import Classifier
from Products.DataCollector import DeviceProxy, Plugins  # noqa: F401
from Products.DataCollector.plugins.DataMaps import PLUGIN_NAME_ATTR
from Products.DataCollector.PortscanClient import PortscanClient
from Products.DataCollector.PythonClient import PythonClient
from Products.DataCollector.SnmpClient import SnmpClient
from Products.DataCollector.SshClient import SshClient
from Products.DataCollector.TelnetClient import (
    TelnetClient,
    buildOptions as TCbuildOptions,
)
from Products.ZenCollector.daemon import parseWorkerOptions, addWorkerOptions
from Products.ZenCollector.interfaces import IEventService
from Products.ZenEvents.ZenEventClasses import Heartbeat, Error
from Products.ZenHub.PBDaemon import FakeRemote, PBDaemon, HubDown
from Products.ZenUtils.DaemonStats import DaemonStats
from Products.ZenUtils.Driver import drive, driveLater
from Products.ZenUtils.metricwriter import ThresholdNotifier
from Products.ZenUtils.Utils import zenPath
from Products.Zuul.utils import safe_hasattr as hasattr

defaultPortScanTimeout = 5
defaultParallel = 1
defaultProtocol = "ssh"
defaultPort = 22

_DEFAULT_CYCLE_INTERVAL = 720
_CONFIG_PULLING_TIMEOUT = 15  # seconds


class ZenModeler(PBDaemon):
    """Daemon that collects device config data and sends it to zenhub.
    """

    name = "zenmodeler"
    initialServices = PBDaemon.initialServices + ["ModelerService"]

    generateEvents = True
    configCycleInterval = 360

    classCollectorPlugins = ()

    def __init__(self, single=False):
        """Initialize a ZenModeler instance.

        @param single: collect from a single device?
        @type single: boolean
        """
        PBDaemon.__init__(self)
        # FIXME: cleanup --force option #2660
        self.options.force = True
        self.start = None
        self.startat = None
        self.rrdStats = DaemonStats()
        self.single = single
        if self.options.device:
            self.single = True
        self.modelerCycleInterval = self.options.cycletime
        # get the minutes and convert to fraction of a day
        self.collage = float(self.options.collage) / 1440.0
        self.pendingNewClients = False
        self.clients = []
        self.finished = []
        self.devicegen = None
        self.counters = collections.Counter()
        self.configFilter = None
        self.configLoaded = False

        # Make sendEvent() available to plugins
        zope.component.provideUtility(self, IEventService)

        # Delay start for between 10 and 60 seconds when run as a daemon.
        self.started = False
        self.startDelay = 0
        self.immediate = 1
        if self.options.daemon or self.options.cycle:
            if self.options.now:
                self.log.debug('option "now" specified, starting immediately.')
            else:
                # self.startDelay = randint(10, 60) * 60
                self.startDelay = randint(10, 60) * 1
                self.immediate = 0
                self.log.info(
                    'option "now" not specified, waiting %s seconds to start.'
                    % self.startDelay
                )
        else:
            self.log.debug("Run in foreground, starting immediately.")

        # ZEN-26637
        self.collectorLoopIteration = 0
        self.mainLoopGotDeviceList = False

        self.isMainScheduled = False

        self._modeledDevicesMetric = Metrology.meter(
            "zenmodeler.modeledDevices"
        )
        self._failuresMetric = Metrology.counter("zenmodeler.failures")

    def reportError(self, error):
        """
        Log errors that have occurred

        @param error: error message
        @type error: string
        """
        self.log.error("Error occured: %s", error)

    def connected(self):
        """
        Called after connected to the zenhub service
        """
        reactor.callLater(_CONFIG_PULLING_TIMEOUT, self._checkConfigLoad)
        d = self.configure()
        d.addCallback(self.heartbeat)
        d.addErrback(self.reportError)

    def _checkConfigLoad(self):
        """
        Looping call to check whether zenmodeler got configuration
        from ZenHub.
        """
        if not self.configLoaded:
            self.log.info(
                "Modeling has not started pending configuration "
                "pull from ZenHub. Is ZenHub overloaded?"
            )
            reactor.callLater(_CONFIG_PULLING_TIMEOUT, self._checkConfigLoad)

    @inlineCallbacks
    def configure(self):
        """Retrieve configuration from zenhub."""
        self.log.info("Getting configuration from ZenHub...")

        self.log.debug("fetching monitor properties")
        items = yield self.config().callRemote("propertyItems")

        # If the cycletime option is not specified or zero, then use the
        # modelerCycleInterval value in the database.
        if not self.options.cycletime:
            self.modelerCycleInterval = items.get(
                "modelerCycleInterval", _DEFAULT_CYCLE_INTERVAL
            )
        self.configCycleInterval = items.get(
            "configCycleInterval", self.configCycleInterval
        )
        reactor.callLater(self.configCycleInterval * 60, self.configure)

        self.log.debug("Getting threshold classes...")
        result = yield self.config().callRemote("getThresholdClasses")
        self.remote_updateThresholdClasses(result)

        self.log.debug("Getting collector thresholds...")
        thresholds = yield self.config().callRemote("getCollectorThresholds")
        threshold_notifier = ThresholdNotifier(self.sendEvent, thresholds)

        self.rrdStats.config(
            self.name,
            self.options.monitor,
            self.metricWriter(),
            threshold_notifier,
            self.derivativeTracker(),
        )

        self.log.debug("Getting collector plugins for each DeviceClass")
        plugins = yield self.config().callRemote("getClassCollectorPlugins")
        self.classCollectorPlugins = plugins

        self.configLoaded = True

    def config(self):
        """
        Get the ModelerService
        """
        return self.services.get("ModelerService", FakeRemote())

    def _load_plugins(self, plugins):
        instances = []
        valid = []
        for loader in plugins:
            try:
                plugin = loader.create()
                self.log.debug("Loaded plugin %s" % plugin.name())
                instances.append(plugin)
                valid.append(loader)
            except Plugins.PluginImportError as import_error:
                component, _ = os.path.splitext(os.path.basename(sys.argv[0]))
                collector_host = socket.gethostname()
                # NB: an import errror affects all devices,
                #     so report the issue against the collector
                evt = {
                    "eventClass": "/Status/Update",
                    "component": component,
                    "agent": collector_host,
                    "device": collector_host,
                    "severity": Error,
                }

                info = "Problem loading plugin %s" % import_error.plugin
                self.log.error(info)
                evt["summary"] = info

                info = import_error.traceback
                self.log.error(info)
                evt["message"] = info

                info = (
                    "Due to import errors, removing the %s plugin"
                    " from this collection cycle."
                ) % import_error.plugin
                self.log.error(info)
                evt["message"] += "%s\n" % info
                self.sendEvent(evt)
        return instances, valid

    def selectPlugins(self, device, transport):
        """
        Build a list of active plugins for a device, based on:

        * the --collect command-line option which is a regex
        * the --ignore command-line option which is a regex
        * transport which is a string describing the type of plugin

        @param device: device to collect against
        @type device: string
        @param transport: python, ssh, snmp, telnet, cmd
        @type transport: string
        @return: results of the plugin
        @type: string
        """
        # _load_plugins returns
        # ([plugin instances], [successfully loaded plugins])
        plugins, valid = self._load_plugins(device.plugins)

        # Make sure that we don't generate messages for bad loaders again
        # NB: doesn't update the device's zProperties
        if len(device.plugins) != len(valid):
            device.plugins = valid

        return [
            plugin
            for plugin in plugins
            if plugin.transport == transport
            and self._use_plugin(plugin.name(), device.id)
        ]

    def collectDevice(self, device):
        """
        Collect data from a single device.

        @param device: device to collect against
        @type device: string
        """
        clientTimeout = getattr(device, "zCollectorClientTimeout", 180)
        ip = device.manageIp
        timeout = clientTimeout + time.time()
        self.log.info(
            "Collect on device %s for collector loop #%3d",
            device.id, self.collectorLoopIteration,
        )
        self.pythonCollect(device, ip, timeout)
        self.cmdCollect(device, ip, timeout)
        self.snmpCollect(device, ip, timeout)
        self.portscanCollect(device, ip, timeout)

    def wmiCollect(self, device, ip, timeout):
        """
        DEPRECATED.
        Start the Windows Management Instrumentation (WMI) collector

        @param device: device to collect against
        @type device: string
        @param ip: IP address of device to collect against
        @type ip: string
        @param timeout: timeout before failing the connection
        @type timeout: integer
        """
        pass

    def pythonCollect(self, device, ip, timeout):
        """
        Start local Python collection client.

        @param device: device to collect against
        @type device: string
        @param ip: IP address of device to collect against
        @type ip: string
        @param timeout: timeout before failing the connection
        @type timeout: integer
        """
        client = None
        try:
            plugins = self.selectPlugins(device, "python")
            if not plugins:
                self.log.info("No Python plugins found for %s" % device.id)
                return
            if self.checkCollection(device):
                self.log.info("Python collection device %s" % device.id)
                self.log.info(
                    "plugins: %s", ", ".join(map(lambda p: p.name(), plugins))
                )
                client = PythonClient(device, self, plugins)
            if not client or not plugins:
                self.log.warn("Python client creation failed")
                return
        except Exception:
            self.log.exception("Error opening pythonclient")
        self.addClient(client, timeout, "python", device.id)

    def cmdCollect(self, device, ip, timeout):
        """
        Start shell command collection client.

        @param device: device to collect against
        @type device: string
        @param ip: IP address of device to collect against
        @type ip: string
        @param timeout: timeout before failing the connection
        @type timeout: integer
        """
        client = None
        clientType = (
            "snmp"
        )  # default to SNMP if we can't figure out a protocol

        hostname = device.id
        try:
            plugins = self.selectPlugins(device, "command")
            if not plugins:
                self.log.info("No command plugins found for %s" % hostname)
                return

            protocol = getattr(device, "zCommandProtocol", defaultProtocol)
            commandPort = getattr(device, "zCommandPort", defaultPort)

            # don't even create a client if we shouldn't collect/model yet
            if not self.checkCollection(device):
                return

            if protocol == "ssh":
                client = SshClient(
                    hostname,
                    ip,
                    commandPort,
                    options=self.options,
                    plugins=plugins,
                    device=device,
                    datacollector=self,
                    isLoseConnection=True,
                )
                clientType = "ssh"
                self.log.info(
                    "Using SSH collection method for device %s" % hostname
                )

            elif protocol == "telnet":
                if commandPort == 22:
                    commandPort = 23  # set default telnet
                client = TelnetClient(
                    hostname,
                    ip,
                    commandPort,
                    options=self.options,
                    plugins=plugins,
                    device=device,
                    datacollector=self,
                )
                clientType = "telnet"
                self.log.info(
                    "Using telnet collection method for device %s" % hostname
                )

            else:
                info = (
                    "Unknown protocol %s for device %s -- "
                    "defaulting to %s collection method"
                    % (protocol, hostname, clientType)
                )
                self.log.warn(info)
                import socket

                component, _ = os.path.splitext(os.path.basename(sys.argv[0]))
                collector_host = socket.gethostname()
                evt = {
                    "eventClass": "/Status/Update",
                    "agent": collector_host,
                    "device": hostname,
                    "severity": Error,
                }
                evt["summary"] = info
                self.sendEvent(evt)
                return

            if not client:
                self.log.warn("Shell command collector creation failed")
            else:
                self.log.info(
                    "plugins: %s", ", ".join(map(lambda p: p.name(), plugins))
                )
        except Exception:
            self.log.exception("Error opening command collector")
        self.addClient(client, timeout, clientType, device.id)

    def snmpCollect(self, device, ip, timeout):
        """
        Start SNMP collection client.

        @param device: device to collect against
        @type device: string
        @param ip: IP address of device to collect against
        @type ip: string
        @param timeout: timeout before failing the connection
        @type timeout: integer
        """
        client = None
        try:
            hostname = device.id
            if getattr(device, "zSnmpMonitorIgnore", True):
                self.log.info("SNMP monitoring off for %s" % hostname)
                return

            if not ip:
                self.log.info("No manage IP for %s" % hostname)
                return

            plugins = []
            plugins = self.selectPlugins(device, "snmp")
            if not plugins:
                self.log.info("No SNMP plugins found for %s" % hostname)
                return

            if self.checkCollection(device):
                self.log.info("SNMP collection device %s" % hostname)
                self.log.info(
                    "plugins: %s", ", ".join(map(lambda p: p.name(), plugins))
                )
                client = SnmpClient(
                    device.id, ip, self.options, device, self, plugins
                )
            if not client or not plugins:
                self.log.warn("SNMP collector creation failed")
                return
        except Exception:
            self.log.exception("Error opening the SNMP collector")
        self.addClient(client, timeout, "SNMP", device.id)

    def addClient(self, device, timeout, clientType, name):
        """
        If device is not None, schedule the device to be collected.
        Otherwise log an error.

        @param device: device to collect against
        @type device: string
        @param timeout: timeout before failing the connection
        @type timeout: integer
        @param clientType: description of the plugin type
        @type clientType: string
        @param name: plugin name
        @type name: string
        """
        if device:
            device.timeout = timeout
            device.timedOut = False
            self.clients.append(device)
            device.run()
        else:
            self.log.warn(
                "Unable to create a %s collector for %s", clientType, name
            )

    # XXX double-check this, once the implementation is in place
    def portscanCollect(self, device, ip, timeout):
        """
        Start portscan collection client.

        @param device: device to collect against
        @type device: string
        @param ip: IP address of device to collect against
        @type ip: string
        @param timeout: timeout before failing the connection
        @type timeout: integer
        """
        client = None
        try:
            hostname = device.id
            plugins = self.selectPlugins(device, "portscan")
            if not plugins:
                self.log.info("No portscan plugins found for %s" % hostname)
                return
            if self.checkCollection(device):
                self.log.info(
                    "Portscan collector method for device %s" % hostname
                )
                self.log.info(
                    "plugins: %s", ", ".join(map(lambda p: p.name(), plugins))
                )
                client = PortscanClient(
                    device.id, ip, self.options, device, self, plugins
                )
            if not client or not plugins:
                self.log.warn("Portscan collector creation failed")
                return
        except Exception:
            self.log.exception("Error opening portscan collector")
        self.addClient(client, timeout, "portscan", device.id)

    def checkCollection(self, device):
        """
        See how old the data is that we've collected

        @param device: device to collect against
        @type device: string
        @return: True if the last collection time + collage older than now
            or the SNMP status number > 0 ?
        @type: boolean
        """
        delay = device.getSnmpLastCollection() + self.collage
        if (
            delay >= float(DateTime.DateTime()) and
            device.getSnmpStatusNumber() == 0
        ):
            self.log.info("Skipped collection of %s" % device.id)
            return False
        return True

    @inlineCallbacks
    def clientFinished(self, collectorClient):
        """
        Callback that processes the return values from a device.
        Python iterable.
        @param collectorClient: collector instance
        @type collectorClient: collector class
        @return: Twisted deferred object
        @type: Twisted deferred object
        """
        device = collectorClient.device
        self.log.debug("Client for %s finished collecting", device.id)

        try:
            if (
                isinstance(collectorClient, SnmpClient)
                and collectorClient.connInfo.changed is True
            ):
                self.log.info(
                    "SNMP connection info for %s changed. Updating...",
                    device.id,
                )
                yield self.config().callRemote(
                    "setSnmpConnectionInfo",
                    device.id,
                    collectorClient.connInfo.zSnmpVer,
                    collectorClient.connInfo.zSnmpPort,
                    collectorClient.connInfo.zSnmpCommunity,
                )

            pluginStats = {}
            self.log.debug("Processing data for device %s", device.id)
            devchanged = False
            maps = []
            for plugin, results in collectorClient.getResults():
                if plugin is None:
                    continue
                self.log.debug(
                    "Processing plugin %s on device %s ...",
                    plugin.name(),
                    device.id,
                )
                if not results:
                    self.log.warn(
                        "The plugin %s returned no results.", plugin.name()
                    )
                    continue

                if self.options.save_raw_results:
                    self.savePluginData(
                        device.id, plugin.name(), "raw", results
                    )

                self.log.debug(
                    "Plugin %s results = %s", plugin.name(), results
                )
                datamaps = []
                try:
                    results = plugin.preprocess(results, self.log)
                    if results:
                        datamaps = plugin.process(
                            device, results, self.log
                        )
                    if datamaps:
                        pluginStats.setdefault(
                            plugin.name(), plugin.weight
                        )
                except Exception as ex:
                    # NB: don't discard the plugin, as it might be a
                    #     temporary issue. Also, report it against the
                    #     device, rather than at a collector as it
                    #     might be just for this device.
                    import socket

                    collector_host = socket.gethostname()
                    evt = {
                        "eventClass": "/Status/Update",
                        "agent": collector_host,
                        "device": device.id,
                        "severity": Error,
                    }

                    info = (
                        "Problem while executing plugin %s" % plugin.name()
                    )
                    self.log.error(info)
                    evt["summary"] = info

                    info = traceback.format_exc()
                    self.log.error(info)
                    evt["message"] = info
                    self.sendEvent(evt)
                    continue

                # allow multiple maps to be returned from one plugin
                if not isinstance(datamaps, (list, tuple)):
                    datamaps = [datamaps]
                if datamaps:
                    newmaps = [m for m in datamaps if m]
                    for m in newmaps:
                        setattr(m, PLUGIN_NAME_ATTR, plugin.name())
                    if self.options.save_processed_results:
                        self.savePluginData(
                            device.id, plugin.name(), "processed", newmaps
                        )
                    maps += newmaps

            if maps:
                deviceClass = Classifier.classifyDevice(
                    pluginStats, self.classCollectorPlugins
                )
                # If self.single is True, then call singleApplyDataMaps
                # instead of applyDataMaps.
                if not self.single:
                    method = "applyDataMaps"
                else:
                    method = "singleApplyDataMaps"
                result = yield self.config().callRemote(
                    method, device.id, maps, deviceClass, True
                )
                if result:
                    devchanged = True
            if devchanged:
                self.log.info("Changes in configuration applied")
            else:
                self.log.info("No change in configuration detected")
        except Exception as ex:
            self.log.exception(ex)
            self.log.error(
                "Client %s finished with message: %s" % (device.id, ex)
            )
            self._failuresMetric.increment()
        else:
            self.log.debug("Client %s finished" % device.id)

        self.counters["modeledDevicesCount"] += 1
        self._modeledDevicesMetric.mark()
        try:
            self.clients.remove(collectorClient)
            self.finished.append(collectorClient)
        except ValueError:
            self.log.debug(
                "Client %s not found in in the list" " of active clients",
                device.id,
            )
        self.log.info(
            "Finished processing client within collector loop #%03d",
            self.collectorLoopIteration
        )

        reactor.callLater(0, self.fillCollectionSlots)
        # d = drive(self.fillCollectionSlots)
        # d.addErrback(self.fillError)

    def savePluginData(self, deviceName, pluginName, dataType, data):
        filename = "/tmp/%s.%s.%s.pickle.gz" % (
            deviceName,
            pluginName,
            dataType,
        )
        try:
            with gzip.open(filename, "wb") as fd:
                pickle.dump(data, fd)
        except Exception as ex:
            self.log.warn(
                "Unable to save data into file '%s': %s", filename, ex
            )

    def fillError(self, reason):
        """
        Twisted errback routine to log an error when
        unable to collect some data

        @param reason: error message
        @type reason: string
        """
        self.log.error("Unable to fill collection slots: %s" % reason)

    def cycleTime(self):
        """
        Return our cycle time (in minutes)

        @return: cycle time
        @rtype: integer
        """
        return self.modelerCycleInterval * 60

    def heartbeat(self, ignored=None):
        """
        Twisted keep-alive mechanism to ensure that
        we're still connected to zenhub

        @param ignored: object (unused)
        @type ignored: object
        """
        ARBITRARY_BEAT = 30
        reactor.callLater(ARBITRARY_BEAT, self.heartbeat)
        if self.options.cycle:
            evt = dict(
                eventClass=Heartbeat,
                component="zenmodeler",
                device=self.options.monitor,
                timeout=self.options.heartbeatTimeout,
            )
            self.sendEvent(evt)
            self.niceDoggie(self.cycleTime())

        # We start modeling from here to accomodate the startup delay.

        if not self.started:
            if self.immediate == 0 and self.startat:
                # This stuff relies on ARBITRARY_BEAT being < 60s
                if self.timeMatches():
                    self.started = True
                    self.log.info("Starting modeling...")
                    reactor.callLater(1, self.main)
                elif not self.isMainScheduled:
                    self.isMainScheduled = True
                    reactor.callLater(self.cycleTime(), self.main)
            else:
                self.started = True
                self.log.info(
                    "Starting modeling in %s seconds.", self.startDelay
                )
                reactor.callLater(self.startDelay, self.main)

    def postStatisticsImpl(self):
        # save modeled device rate
        self.rrdStats.derive(
            "modeledDevices", self.counters["modeledDevicesCount"]
        )

        # save running count
        self.rrdStats.gauge(
            "modeledDevicesCount", self.counters["modeledDevicesCount"]
        )

    def _getCountersFile(self):
        return zenPath("var/%s_%s.pickle" % (self.name, self.options.monitor))

    @property
    def _devicegen_has_items(self):
        """check it self.devicegen (an iterator) is not empty and has at least
        one value. doing this check changes the iterator, so this method
        restores it to its original state before returning"""
        result = False
        if self.devicegen is not None:
            try:
                first = self.devicegen.next()
            except StopIteration:
                pass
            else:
                result = True
                self.devicegen = chain([first], self.devicegen)
        return result

    def checkStop(self, unused=None):
        """
        Check to see if there's anything to do.
        If there isn't, report our statistics and exit.

        @param unused: unused (unused)
        @type unused: string
        """
        if self.pendingNewClients or self.clients:
            return
        if self._devicegen_has_items:
            return
        if not self.mainLoopGotDeviceList:
            return  # ZEN-26637 to prevent race between checkStop and mainLoop

        if self.start:
            runTime = time.time() - self.start
            self.start = None
            if not self.didCollect:
                self.log.info("Did not collect during collector loop")
            self.log.info(
                "Scan time: %0.2f seconds for collector loop #%03d",
                runTime,
                self.collectorLoopIteration,
            )
            self.log.info(
                "Scanned %d of %d devices during collector loop #%03d",
                self.processedDevicesCount,
                self.iterationDeviceCount,
                self.collectorLoopIteration,
            )
            devices = len(self.finished)
            timedOut = len([c for c in self.finished if c.timedOut])
            self.rrdStats.gauge("cycleTime", runTime)
            self.rrdStats.gauge("devices", devices)
            self.rrdStats.gauge("timedOut", timedOut)
            if not self.options.cycle:
                self.stop()
            self.finished = []

    @inlineCallbacks
    def fillCollectionSlots(self):
        """
        An iterator which either returns a device to collect or
        calls checkStop()
        @param driver: driver object
        @type driver: driver object
        """
        count = len(self.clients)
        while (
            count < self.options.parallel
            and self._devicegen_has_items
            and not self.pendingNewClients
        ):
            self.pendingNewClients = True
            try:
                device = self.devicegen.next()
                devices = yield self.config().callRemote(
                    "getDeviceConfig", [device], self.options.checkStatus
                )
                # just collect one device, and let the timer add more
                if devices:
                    self.processedDevicesCount = self.processedDevicesCount + 1
                    self.log.info(
                        "Filled collection slots for %d of %d devices "
                        "during collector loop #%03d",
                        self.processedDevicesCount,
                        self.iterationDeviceCount,
                        self.collectorLoopIteration,
                    )  # TODO should this message be logged at debug level?
                    self.didCollect = True
                    d = devices[0]
                    if d.skipModelMsg:
                        self.log.info(d.skipModelMsg)
                    else:
                        self.collectDevice(d)
                else:
                    self.log.info("Device %s not returned is it down?", device)
            except StopIteration:
                self.devicegen = None
            finally:
                self.pendingNewClients = False
            break
        update = len(self.clients)
        if update != count and update != 1:
            self.log.info("Running %d clients", update)
        else:
            self.log.debug("Running %d clients", update)
        self.checkStop()

    def timeMatches(self):
        """
        Check whether the current time matches a cron-like
        specification, return a straight true or false
        """
        if self.startat is None:
            return True

        def match_entity(entity, value):
            if entity == "*":
                return True

            value = int(value)

            if entity.isdigit() and int(entity) == value:
                return True

            if entity.startswith("*/") and entity[2:].isdigit():
                if value % int(entity[2:]) == 0:
                    return True

            if "," in entity and any(
                segment.isdigit() and int(segment) == value
                for segment in entity.split(",")
            ):
                return True

            return False

        curtime = time.localtime()
        # match minutes, hours, date, and month fields
        if all(
            match_entity(self.startat[a], curtime[b])
            for a, b in ((0, 4), (1, 3), (2, 2), (3, 1))
        ):
            dayofweek = curtime[6] + 1
            if (
                match_entity(self.startat[4], dayofweek)
                or dayofweek == 7
                and match_entity(self.startat[4], 0)
            ):
                return True

        return False

    def buildOptions(self):
        """
        Build our list of command-line options
        """
        PBDaemon.buildOptions(self)
        self.parser.add_option(
            "--debug",
            dest="debug",
            action="store_true",
            default=False,
            help="Don't fork threads for processing",
        )
        self.parser.add_option(
            "--nowmi",
            dest="nowmi",
            action="store_true",
            default=True,
            help="Do not execute WMI plugins (deprecated; always True)",
        )
        self.parser.add_option(
            "--parallel",
            dest="parallel",
            type="int",
            default=defaultParallel,
            help="Number of devices to collect from in parallel",
        )
        self.parser.add_option(
            "--cycletime",
            dest="cycletime",
            type="int",
            help="Run collection every x minutes",
        )
        self.parser.add_option(
            "--ignore",
            dest="ignorePlugins",
            default="",
            help="Modeler plugins to ignore. Takes a regular expression",
        )
        self.parser.add_option(
            "--collect",
            dest="collectPlugins",
            default="",
            help="Modeler plugins to use. Takes a regular expression",
        )
        self.parser.add_option(
            "-p",
            "--path",
            dest="path",
            help="Start class path for collection ie /NetworkDevices",
        )
        self.parser.add_option(
            "-d",
            "--device",
            dest="device",
            help="Fully qualified device name ie www.confmon.com",
        )
        self.parser.add_option(
            "--startat", dest="startat", help="Start string in cron(8) format"
        )
        self.parser.add_option(
            "-a",
            "--collage",
            dest="collage",
            default=0,
            type="float",
            help="Do not collect from devices whose collect date "
            + "is within this many minutes",
        )
        self.parser.add_option(
            "--writetries",
            dest="writetries",
            default=2,
            type="int",
            help="Number of times to try to write if a "
            "read conflict is found",
        )
        # FIXME: cleanup --force option #2660
        self.parser.add_option(
            "-F",
            "--force",
            dest="force",
            action="store_true",
            default=True,
            help="Force collection of config data (deprecated)",
        )
        self.parser.add_option(
            "--portscantimeout",
            dest="portscantimeout",
            type="int",
            default=defaultPortScanTimeout,
            help="Time to wait for connection failures when port scanning",
        )
        self.parser.add_option(
            "--now",
            dest="now",
            action="store_true",
            default=False,
            help="Start daemon now, do not sleep before starting",
        )
        self.parser.add_option(
            "--communities",
            dest="discoverCommunity",
            action="store_true",
            default=False,
            help="If an snmp connection fails try and "
            "rediscover it's connection info",
        )
        self.parser.add_option(
            "--checkstatus",
            dest="checkStatus",
            action="store_true",
            default=False,
            help="Don't model if the device is ping or snmp down",
        )

        self.parser.add_option(
            "--save_raw_results",
            dest="save_raw_results",
            action="store_true",
            default=False,
            help="Save raw results for replay purposes in /tmp",
        )
        self.parser.add_option(
            "--save_processed_results",
            dest="save_processed_results",
            action="store_true",
            default=False,
            help="Save modeler plugin outputs for replay purposes in /tmp",
        )

        addWorkerOptions(self.parser)

        TCbuildOptions(self.parser, self.usage)

    def processOptions(self):
        """
        Check what the user gave us vs what we'll accept
        for command-line options
        """
        if not self.options.path and not self.options.device:
            self.options.path = "/Devices"
        if self.options.ignorePlugins and self.options.collectPlugins:
            self.parser.error(
                "Only one of --ignore or --collect" " can be used at a time"
            )
        self._use_plugin = _UsePlugin(
            self.options.collectPlugins, self.options.ignorePlugins,
        )
        if self.options.startat:
            cronmatch = re.match(
                r"^\s*([\*/,\d]+)"
                r"\s+([\*/,\d]+)"
                r"\s+([\*/,\d]+)"
                r"\s+([\*/,\d]+)"
                r"\s+([\*/,\d]+)"
                r"\s*$",
                self.options.startat,
            )
            if cronmatch:
                self.startat = cronmatch.groups()
            else:
                self.log.error(
                    'startat option "%s" was invalid, carrying on anyway',
                    self.options.startat,
                )

        configFilter = parseWorkerOptions(self.options.__dict__)
        if configFilter:
            self.configFilter = configFilter

    def _timeoutClients(self):
        """
        The guts of the timeoutClients method (minus the twisted reactor
        stuff). Breaking this part out as a separate method facilitates unit
        testing.
        """
        active = []
        for client in self.clients:
            if client.timeout < time.time():
                self.log.warn("Client %s timeout", client.hostname)
                self.finished.append(client)
                client.timedOut = True
                try:
                    client.stop()
                except AssertionError:
                    # session closed twice
                    pass
            else:
                active.append(client)
        self.clients = active

    def timeoutClients(self, unused=None):
        """
        Check to see which clients have timed out and which ones haven't.
        Stop processing anything that's timed out.

        @param unused: unused (unused)
        @type unused: string
        """
        reactor.callLater(1, self.timeoutClients)
        self._timeoutClients()
        d = drive(self.fillCollectionSlots)
        d.addCallback(self.checkStop)
        d.addErrback(self.fillError)

    def getDeviceList(self):
        """
        Get the list of devices for which we are collecting:
        * if -d devicename was used, use the devicename
        * if a class path flag was supplied, gather the devices
          along that organizer
        * otherwise get all of the devices associated with our collector

        @return: list of devices
        @rtype: list
        """
        if self.options.device:
            self.log.info("Collecting for device %s", self.options.device)
            return succeed([self.options.device])

        self.log.info("Collecting for path %s", self.options.path)

        d = self.config().callRemote(
            "getDeviceListByOrganizer",
            self.options.path,
            self.options.monitor,
            self.options.__dict__,
        )

        def handle(results):
            if hasattr(results, "type") and results.type is HubDown:
                self.log.warning(
                    "Collection aborted: %s", results.getErrorMessage()
                )
                return
            self.log.critical(
                "%s is not a valid organizer.", self.options.path
            )
            reactor.running = False
            sys.exit(1)

        d.addErrback(handle)
        return d

    def mainLoop(self, driver):
        """
        Main collection loop, a Python iterable

        @param driver: driver object
        @type driver: driver object
        @return: Twisted deferred object
        @rtype: Twisted deferred object
        """
        if self.options.cycle:
            self.isMainScheduled = True
            driveLater(self.cycleTime(), self.mainLoop)

        if self.clients:
            self.log.error("Modeling cycle taking too long")
            return

        # ZEN-26637 - did we collect during collector loop?
        self.didCollect = False
        self.mainLoopGotDeviceList = False
        self.start = time.time()
        self.collectorLoopIteration = self.collectorLoopIteration + 1

        self.log.info(
            "Starting collector loop #{:03d}...".format(
                self.collectorLoopIteration
            )
        )
        yield self.getDeviceList()
        deviceList = driver.next()
        self.log.debug("getDeviceList returned %s devices", len(deviceList))
        self.log.debug("getDeviceList returned %s devices", deviceList)
        self.devicegen = iter(deviceList)
        self.iterationDeviceCount = len(deviceList)
        self.processedDevicesCount = 0
        self.log.info(
            "Got %d devices to be scanned during collector loop #%03d",
            self.iterationDeviceCount,
            self.collectorLoopIteration,
        )
        d = drive(self.fillCollectionSlots)
        d.addErrback(self.fillError)
        self.mainLoopGotDeviceList = True
        yield d
        driver.next()
        self.log.debug("Collection slots filled")

    def main(self, unused=None):
        """
        Wrapper around the mainLoop

        @param unused: unused (unused)
        @type unused: string
        @return: Twisted deferred object
        @rtype: Twisted deferred object
        """
        self.finished = []
        d = drive(self.mainLoop)
        d.addCallback(self.timeoutClients)
        return d

    def remote_deleteDevice(self, device):
        """
        Stub function

        @param device: device name (unused)
        @type device: string
        @todo: implement
        """
        # we fetch the device list before every scan
        self.log.debug("Asynch deleteDevice %s" % device)

    def remote_deleteDevices(self, devices):
        """
        Stub function

        @param devices: device ids (unused)
        @type device: set
        @todo: implement
        """
        # we fetch the device list before every scan
        self.log.debug("Asynch deleteDevices {0}".format(len(devices)))


class _UsePlugin(object):
    """Use a predicate to determine whether a plugin should be used."""

    def __init__(self, log, inclusion, exclusion):
        """Initialize a _UsePlugin instance.

        Either inclusion or exclusion may can be not None.

        :param Logger log: a logger object
        :param inclusion: Regex for plugin inclusion or None
        :type inclusion: Union[str,None]
        :param exclusion: Regex for plugin exclusion or None
        :type exclusion: Union[str,None]
        """
        if inclusion is not None and exclusion is not None:
            raise TypeError(
                "Either inclusion or exclusion may be not None, "
                "but not both"
            )
        self._log = log
        if inclusion is not None:
            self._matcher = re.compile(inclusion)
            self.__call__ = self._plugin_included
        elif exclusion is not None:
            self._matcher = re.compile(exclusion)
            self.__call__ = self._plugin_not_excluded
        else:
            self.__call__ = self._use_plugin

    def _plugin_not_excluded(self, name, deviceId):
        if self._matcher.search(name) is None:
            return True
        self._log.debug(
            "Ignoring plugin %s because of --ignore flag", name, deviceId,
        )
        return False

    def _plugin_included(self, name, deviceId):
        if self._matcher.search(name) is None:
            return False
        self._log.debug(
            "Using plugin %s on %s because of --collect flag", name, deviceId,
        )
        return True

    def _use_plugin(self, name, deviceId):
        self._log.debug("Using plugin %s on %s", name, deviceId)
        return True


if __name__ == "__main__":
    dc = ZenModeler()
    dc.processOptions()
    dc.run()
