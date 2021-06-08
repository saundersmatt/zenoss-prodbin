##############################################################################
#
# Copyright (C) Zenoss, Inc. 2018, all rights reserved.
#
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
#
##############################################################################

import logging
import transaction

from itertools import chain

from transaction.interfaces import ISavepointDataManager, IDataManagerSavepoint
from zope.component import createObject, getUtility
from zope.component.interfaces import ComponentLookupError
from zope.interface import implementer

from .interfaces import IZingConnectorProxy, IImpactRelationshipsFactProvider

log = logging.getLogger("zen.zing.transaction")


class ZingTxState(object):
    """
    All relevant object updates within a transaction are buffered in this data
    structure in the transaction object. Once the tx successfully commits,
    facts will be generated and sent to zing-connector.
    """

    def __init__(self):
        # updated by zenhub during apply datamaps (ZingDatamapHandler)
        #
        self.datamaps = []
        self.datamaps_contexts = {}

        # updated by model_catalog IndexingEvent machinery
        # (ZingObjectUpdateHandler)

        # contextUUIDs:Fact that need an organizers fact
        self.need_organizers_fact = {}
        # contextUUIDs:Fact that need a device info fact
        self.need_device_info_fact = {}
        self.need_device_organizer_info_fact = {}
        self.need_component_group_info_fact = {}
        # contextUUIDs:Fact that need a deletion fact
        self.need_deletion_fact = {}

        # Sets containing the uuids for which we have already sent a type of
        # fact to avoid sending the same fact more than once.
        self.already_generated_organizer_facts = set()
        self.already_generated_device_info_facts = set()
        self.already_generated_device_organizer_info_facts = set()
        self.already_generated_component_group_info_facts = set()
        self.already_generated_impact_facts = set()

        self.impact_installed = False
        try:
            getUtility(IImpactRelationshipsFactProvider)
            self.impact_installed = True
        except ComponentLookupError:
            pass

    def is_there_datamap_updates(self):
        return len(self.datamaps) > 0

    def is_there_object_updates(self):
        return any(
            len(facts) > 0 for facts in (
                self.need_organizers_fact,
                self.need_device_info_fact,
                self.need_device_organizer_info_fact,
                self.need_component_group_info_fact,
                self.need_deletion_fact,
            )
        )


def get_zing_tx_state():
    current_tx = transaction.get()
    return getattr(current_tx, ZingTxStateManager.TX_DATA_FIELD_NAME, None)


class ZingTxStateManager(object):

    TX_DATA_FIELD_NAME = "zing_tx_state"

    def get_zing_tx_state(self, context):
        """
        Get the ZingTxState object for the current transaction.
        If it doesnt exists, create one.
        """
        zing_tx_state = None
        current_tx = transaction.get()
        zing_tx_state = getattr(current_tx, self.TX_DATA_FIELD_NAME, None)
        if not zing_tx_state:
            zing_tx_state = ZingTxState()
            setattr(current_tx, self.TX_DATA_FIELD_NAME, zing_tx_state)
            current_tx.join(ZingDataManager(context))
        return zing_tx_state


def _generate_facts(context, zing_connector, zing_tx_state):
    fact_generators = []
    if zing_tx_state.is_there_datamap_updates():
        # process datamaps
        datamap_handler = createObject("ZingDatamapHandler", context)
        fact_generators.append(
            datamap_handler.generate_facts(zing_tx_state)
        )
    if zing_tx_state.is_there_object_updates():
        # process object updates
        object_updates_handler = createObject(
            "ZingObjectUpdateHandler", context
        )
        fact_generators.append(
            object_updates_handler.generate_facts(zing_tx_state)
        )
    return fact_generators


@implementer(IDataManagerSavepoint)
class ZingSavepoint(object):
    """Do-nothing savepoint implementation."""

    def rollback(self):
        pass


@implementer(ISavepointDataManager)
class ZingDataManager(object):
    """Simple wrapper."""

    def __init__(self, ctx):
        self.__ctx = ctx
        self.__key = "zing_dm_%x" % id(self)

    def abort(self, tx):
        pass

    def tpc_begin(self, tx):
        pass

    def commit(self, tx):
        pass

    def tpc_vote(self, tx):
        pass

    def tpc_finish(self, tx):
        try:
            zing_tx_state = getattr(
                tx, ZingTxStateManager.TX_DATA_FIELD_NAME, None,
            )
            if zing_tx_state is None:
                log.warning("No transaction state found")
                return
            zing_connector = IZingConnectorProxy(self.__ctx)
            if not zing_connector.ping():
                log.error(
                    "Error processing facts: zing-connector did not respond"
                )
                return
            fact_generators = _generate_facts(
                self.__ctx, zing_connector, zing_tx_state
            )
            if fact_generators:
                zing_connector.send_fact_generator_in_batches(
                    fact_gen=chain(*fact_generators)
                )
        except Exception:
            log.exception("Exception processing facts for zing-connector")

    def tpc_abort(self, tx):
        pass

    def sortKey(self):
        return self.__key

    def savepoint(self):
        return ZingSavepoint()
