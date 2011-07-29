###########################################################################
#
# This program is part of Zenoss Core, an open source monitoring platform.
# Copyright (C) 2008, Zenoss Inc.
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License version 2 or (at your
# option) any later version as published by the Free Software Foundation.
#
# For complete information please visit: http://www.zenoss.com/oss/
#
###########################################################################

__doc__ = """MySqlSendEvent
Populate the events database with incoming events
"""

import logging
log = logging.getLogger("zen.Events")

from zope.component import getUtility

import Products.ZenUtils.guid as guid
from Event import buildEventFromDict
from ZenEventClasses import Heartbeat, Unknown
from Products.ZenMessaging.queuemessaging.interfaces import IEventPublisher, IQueuePublisher
from zenoss.protocols.protobufs.zep_pb2 import DaemonHeartbeat

class MySqlSendEventMixin:
    """
    Mix-in class that takes a MySQL db connection and builds inserts that
    sends the event to the backend.
    """
    def sendEvents(self, events):
        """
        Sends multiple events using a single publisher. This prevents
        using a new connection for each event.
        """
        publisher = None
        try:
            publisher = getUtility(IEventPublisher, 'batch')
            count = 0
            for event in events:
                try:
                    self._publishEvent(event, publisher)
                    count += 1
                except Exception, ex:
                    log.exception(ex)
            return count
        finally:
            if publisher:
                publisher.close()

    def sendEvent(self, event):
        """
        Send an event to the backend.

        @param event: an event
        @type event: Event class
        @return: event id or None
        @rtype: string
        """
        event = self._publishEvent(event)
        return event.evid if event else None

    def _publishEvent(self, event, publisher=None):
        """
        Sends this event to the event fan out queue
        """
        if publisher is None:
            publisher = getUtility(IEventPublisher)
        if log.isEnabledFor(logging.DEBUG):
            log.debug('%s%s%s' % ('=' * 15, '  incoming event  ', '=' * 15))
        if isinstance(event, dict):
            event = buildEventFromDict(event)

        if getattr(event, 'eventClass', Unknown) == Heartbeat:
            log.debug("Got a %s %s heartbeat event (timeout %s sec).",
                      getattr(event, 'device', 'Unknown'),
                      getattr(event, 'component', 'Unknown'),
                      getattr(event, 'timeout', 'Unknown'))
            return self._sendHeartbeat(event)

        event.evid = guid.generate(1)
        publisher.publish(event)
        return event

    def _sendHeartbeat(self, event):
        """
        Publishes a heartbeat message to the queue.

        @param event: event
        @type event: Event class
        """
        try:
            heartbeat = DaemonHeartbeat(monitor = event.device,
                                        daemon = getattr(event, "component", ""),
                                        timeout_seconds = int(event.timeout))
            publisher = getUtility(IQueuePublisher)
            publisher.publish('$Heartbeats', 'zenoss.heartbeat.%s' % heartbeat.monitor, heartbeat)
        except (ValueError, AttributeError) as e:
            log.error("Unable to send heartbeat: %s", event)
            log.exception(e)
