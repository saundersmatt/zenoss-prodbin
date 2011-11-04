###########################################################################
#
# This program is part of Zenoss Core, an open source monitoring platform.
# Copyright (C) 2011, Zenoss Inc.
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License version 2 or (at your
# option) any later version as published by the Free Software Foundation.
#
# For complete information please visit: http://www.zenoss.com/oss/
#
###########################################################################

__doc__ = """Provides a batch notifier to break up the expensive, blocking IO
involved with calls to DeviceOrganizer.getSubDevices which can call getObject
on the brains of every device in the system. Processes batches of 10 devices
giving way to the event loop between each batch. See ticket #26626. zenhub
calls update_all_services as the entry point into this module, everything else
in this module is private."""

import logging
import collections
from twisted.internet import reactor, defer
from Products.ZenModel.DeviceClass import DeviceClass
from Products.ZenModel.Device import Device
from Products.ZenModel.DataRoot import DataRoot

LOG = logging.getLogger("zen.hub.notify")

class NotifyItem(object):
    """These items are held in the BatchNotifier's queue. They contain all the
    context needed to process the subdevices of a specific device class. This
    context includes...
    
        device class UID: e.g. /zport/dmd/Devices/Server/Linux)
        subdevices: an iterator over the device classes subdevices)
        notify_functions: a dictionary mapping Service UID to notifyAll
                          function. An example Service UID is 
                                     ('CommandPerformanceConfig', 'localhost')
        d: the deferred for this item. Always has the following callback 
           chain:
             Slot          Callback                    Errback
               1    BatchNotifier._callback             None
               2             None                 BatchNotifier._errback
    """

    def __init__(self, device_class_uid, subdevices):
        self.device_class_uid = device_class_uid
        self.subdevices = subdevices
        # keys are service_uids eg ('CommandPerformanceConfig', 'localhost')
        self.notify_functions = {}
        self.d = None

    def __repr__(self):
        args = (self.device_class_uid, self.notify_functions.keys())
        return "<NotifyItem(device_class_uid=%s, notify_functions=%s)>" % args

class BatchNotifier(object):
    """Processes the expensive getSubDevices call in batches. A singleton
    instance is registered as a utility in zcml. The queue contains NotifyItem
    instances. If notify_subdevices is called and an item exists in the queue
    for the same device class, then the new service UID and notify function
    are appended to the existing item. Once an item is moved from the queue to
    _current_item member, it is being processed and further notify_subdevices
    calls for the same device class will append a new item to the queue.
    """

    _BATCH_SIZE = 10
    _DELAY = 0.05

    def __init__(self):
        self._current_item = None
        self._queue = collections.deque()
        self._stopping = False

    def notify_subdevices(self, device_class, service_uid, notify_function):
        if not self._stopping:
            LOG.debug("BatchNotifier.notify_subdevices: %r, %s" % (device_class, service_uid))
            item = self._find_or_create_item(device_class)
            item.notify_functions[service_uid] = notify_function
        else:
            LOG.debug("notify_subdevices received a call while "
                      "stopping: %r, %s" % (device_class, service_uid))

    def _find_or_create_item(self, device_class):
        device_class_uid = device_class.getPrimaryId()
        for item in self._queue:
            if item.device_class_uid == device_class_uid:
                retval = item
                break
        else:
            subdevices = device_class.getSubDevicesGen()
            retval = NotifyItem(device_class_uid, subdevices)
            retval.d = self._create_deferred()
            if not self._queue:
                self._call_later(retval.d)
            self._queue.appendleft(retval)
        return retval

    def _create_deferred(self):
        d = defer.Deferred()
        d.addCallback(self._callback)
        d.addErrback(self._errback)
        return d

    def _call_later(self, d):
        reactor.callLater(BATCH_NOTIFIER._DELAY, d.callback, None)

    def _switch_to_next_item(self):
        self._current_item = self._queue.pop() if self._queue else None

    def _call_notify_functions(self, device):
        for service_uid, notify_function in self._current_item.notify_functions.items():
            try:
                notify_function(device)
            except Exception, e:
                args = (service_uid, device.getPrimaryId(), type(e).__name__, e)
                LOG.error("%s failed to notify %s: %s: %s" % args)

    def _callback(self, result):
        if self._current_item is None:
            self._current_item = self._queue.pop()
        batch_count = 0
        try:
            for device in self._current_item.subdevices:
                self._call_notify_functions(device)
                batch_count += 1
                if batch_count == BatchNotifier._BATCH_SIZE:
                    self._current_item.d = self._create_deferred()
                    break
            else:
                LOG.debug("BatchNotifier._callback: no more devices, %s in queue", len(self._queue))
                self._switch_to_next_item()
        except Exception, e:
            args = (self._current_item.device_class_uid, type(e).__name__, e)
            LOG.warn("Failed to get subdevice of %s: %s: %s" % args)
            self._switch_to_next_item()
        if self._current_item is not None:
            self._call_later(self._current_item.d)

    def _errback(self, failure):
        LOG.error("Failure in batch notifier: %s: %s" % (failure.type.__name__, failure.value))
        LOG.debug("BatchNotifier._errback: failure=%s" % failure)

    def stop(self):
        self._stopping = True
        return defer.DeferredList([item.d for item in self._queue])


BATCH_NOTIFIER = BatchNotifier()
