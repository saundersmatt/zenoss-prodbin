##############################################################################
#
# Copyright (C) Zenoss, Inc. 2007, all rights reserved.
#
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
#
##############################################################################

import inspect
import logging

from ZODB.transact import transact
from zope.event import notify
from metrology.registry import registry

import Globals  # noqa. required to import zenoss Products
from Products.ZenUtils.MetricReporter import QueueGauge
from Products.ZenUtils.Utils import importClass
from Products.DataCollector.Exceptions import ObjectCreationError
from Products.DataCollector.plugins.DataMaps import RelationshipMap, ObjectMap
from Products.ZenModel.ZenModelRM import ZenModelRM
from Products.ZenRelations.Exceptions import ObjectNotFound

from .incrementalupdate import (
    IncrementalDataMap,
    InvalidIncrementalDataMapError,
)
from .datamaputils import (
    _check_the_locks,
    _locked_from_updates,
    _locked_from_deletion,
    directive_map,
    _evaluate_legacy_directive,
    _objectmap_to_device_diff,
    _update_object,
)

from .reporter import ADMReporter
from .events import DatamapAddEvent, DatamapUpdateEvent


log = logging.getLogger("zen.ApplyDataMap")
log.setLevel(logging.DEBUG)

CLASSIFIER_CLASS = '/Classifier'


def isSameData(x, y):
    """
    A more comprehensive check to see if existing model data is the same as
    newly modeled data. The primary focus is comparing unsorted lists of
    dictionaries.
    """
    if isinstance(x, (tuple, list)) and isinstance(y, (tuple, list)):
        if (
            x and y
            and all(isinstance(i, dict) for i in x)
            and all(isinstance(i, dict) for i in y)
        ):
            x = set(tuple(sorted(d.items())) for d in x)
            y = set(tuple(sorted(d.items())) for d in y)
        else:
            return sorted(x) == sorted(y)

    return x == y


class ApplyDataMap(object):

    def __init__(self, datacollector=None):
        self.datacollector = datacollector
        self.num_obj_changed = 0
        self._dmd = None
        if datacollector:
            self._dmd = getattr(datacollector, 'dmd', None)
        metricName = 'applyDataMap.updateRelationship'  # pragma: no mutate
        if metricName not in {x[0] for x in registry}:
            registry.add(
                metricName,
                QueueGauge('zenoss_deviceId', 'zenoss_compname', 'internal')
            )
        self._urGauge = registry.get(metricName) # remove?

        self._build_directive_map()

        self._reporter = ADMReporter()

    def _build_directive_map(self):
        self._directive_map = {
            'update_locked': self._update_locked,
            'delete_locked': self._delete_locked,
            'nochange': self._nochange,
            'remove': self._remove,
            'update': self._update,
            'add': self._add,
            'rebuild': self._rebuild,
        }

    def setDeviceClass(self, device, deviceClass=None):
        """
        If a device class has been passed and the current class is not
        /Classifier then move the device to the newly clssified device class.
        """
        if (
            deviceClass
            and device.getDeviceClassPath().startswith(CLASSIFIER_CLASS)
        ):
            device.changeDeviceClass(deviceClass)

    def applyDataMap(
        self,
        device,
        datamap,
        relname="",
        compname="",
        modname="",
        parentId="",
        commit=True
    ):
        """Apply a datamap passed as a list of dicts through XML-RPC,
        A RelatinshipMap, or an ObjectMap

        Apply datamap to device. Return True if datamap changed device.

        The default value for commit is True for backwards-compatibility
        reasons. If you're a new caller to ApplyDataMap._applyData you should
        probably set commit to False and handle your own transactions.

        @type device: Device
        @param device: Device to be updated by a RelationshipMap,
            or parent device of an ObjectMap.
        @type datamap: RelationshipMap, ObjectMap
        @param datamap: map used to update the device, and its components
        @return: True if updated, False if not
        """
        log.debug('requested applyDataMap for device=%s', device)  # pragma: no mutate

        if not device or _locked_from_updates(device):
            log.warn('device is locked from updates: device=%s', device)  # pragma: no mutate
            return False

        datamap = _validate_datamap(
            datamap,
            relname=relname,
            compname=compname,
            modname=modname,
            parentId=parentId
        )

        notify(DatamapAddEvent(self._dmd, datamap, device))

        # Preprocess datamap, setting directive and diff
        if isinstance(datamap, RelationshipMap):
            datamap = _process_relationshipmap(datamap, device)
            if not datamap:
                return False
            adm_method = self._apply_relationshipmap
        elif not isinstance(datamap, IncrementalDataMap):
            datamap = _process_objectmap(datamap, device)
            adm_method = self._apply_objectmap

        if isinstance(datamap, IncrementalDataMap):
            adm_method = self._apply_incrementalmap

        # apply the changes
        if commit:
            result = transact(adm_method)(datamap, device)
        else:
            result = adm_method(datamap, device)

        # report the changes made
        result = self._report_changes(datamap, device)
        log.debug('applyDataMap result=%s', result)  # pragma: no mutate
        return result

    _applyDataMap = applyDataMap  # stop abusing _functions

    def _apply_relationshipmap(self, relmap, device):
        relname = relmap.relname
        log.debug('_apply_relationshipmap to %s.%s', device, relmap.relname)  # pragma: no mutate
        # remove any objects no longer included in the relationshipmap
        # to be deleted (device, relationship_name, object/id)
        for obj in relmap._diff['removed']:
            _remove_relationship(relmap._parent, relname, obj)

        # update relationships for each object in the relationship map
        for object_map in relmap:
            if isinstance(object_map, IncrementalDataMap):
                object_map.apply()

            elif isinstance(object_map, ObjectMap):
                self._apply_objectmap(object_map, device)

            elif isinstance(object_map, ZenModelRM):
                # add the relationship to the device
                device.addRelation(relname, object_map)
            else:
                raise RuntimeError(
                    'expected ObjectMap, found %s' % object_map.__class__  # pragma: no mutate
                )

    def _apply_objectmap(self, object_map, device):
        '''Add/Update/Remove objects to the target relationship.

        Return True if a change was made or false if no change was made.
        '''
        log.debug('_apply_objectmap: _directive=%s', object_map._directive)  # pragma: no mutate

        relname = getattr(object_map, '_relname', None)

        return self._directive_map[object_map._directive](
            device=device, relname=relname, object_map=object_map
        )

    def _apply_incrementalmap(self, incremental_map, device):
        log.debug('_apply_incrementalmap: incremental_map=%s', incremental_map)  # pragma: no mutate
        return incremental_map.apply()

    def _update_locked(self, device, object_map, **kwargs):
        return False

    def _delete_locked(self, device, **kwargs):
        return False

    def _nochange(self, **kwargs):
        return False

    def _remove(self, device, relname, object_map, **kwargs):
        log.debug(
            '_remove: parent=%s, relname=%s, target=%s',  # pragma: no mutate
            device, relname, object_map._target
        )
        try:
            return _remove_relationship(device, relname, object_map._target)
        except ObjectNotFound:
            log.exception(
                'attempted to remove non-existent relation'  # pragma: no mutate
                ' parent=%s, relname=%s, obj=%s',  # pragma: no mutate
                device, relname, object_map._target,
            )

            return False

    def _update(self, device, object_map, **kwargs):
        obj = _get_objmap_target(device, object_map)
        log.debug('_update: object=%s', obj)  # pragma: no mutate
        notify(DatamapUpdateEvent(self._dmd, object_map, device))
        return _update_object(obj, object_map._diff)

    def _add(self, device, relname, object_map, **kwargs):
        log.debug(
            '_add: device=%s, relationship=%s, object=%s',  # pragma: no mutate
            device, relname, object_map
        )
        self._add_related_object(device, relname, object_map)
        return True

    def _rebuild(self, device, relname, object_map, **kwargs):
        log.debug(
            '_rebuild: device=%s, relationship=%s, object=%s',  # pragma: no mutate
            device, relname, object_map
        )
        _remove_relationship(device, relname, object_map)
        self._add_related_object(device, relname, object_map)
        return True

    def _add_related_object(self, device, relname, object_map):
        new_object = _create_object(object_map, object_map._parent)
        _add_object_to_relationship(object_map._parent, relname, new_object)
        relationship = getattr(object_map._parent, relname)
        obj = relationship._getOb(new_object.id)
        _update_object(obj, object_map._diff)

    def stop(self):
        pass

    def _report_changes(self, datamap, device):
        if isinstance(datamap, RelationshipMap):
            self._report_relationshipmap_changes(datamap, device)

            counts = {directive: 0 for directive in self._directive_map.keys()}
            for object_map in datamap:
                counts[object_map._directive] += 1
                self._reporter.report_directive(device, object_map)
            log.info(
                'applied RelationshipMap changes:'  # pragma: no mutate
                ' target=%s.%s, change_counts=%s',  # pragma: no mutate
                device.id, datamap.relname, counts
            )
            changecount = sum(
                v for k, v in counts.iteritems() if k is not 'nochange'
            )
            changed = bool(changecount or datamap._diff['removed'])

        elif isinstance(datamap, ObjectMap):
            self._report_objectmap_changes(datamap, device)
            changed = (
                True if datamap._directive in ['add', 'update'] else False
            )

        elif isinstance(datamap, IncrementalDataMap):
            self._report_objectmap_changes(datamap, device)
            changed = datamap.changed

        else:
            log.warn('_report_changes for unknown datamap type %s', datamap)
            changed = False

        return changed

    def _report_relationshipmap_changes(self, relmap, device):

        for deleted in relmap._diff['removed']:
            self._reporter.report_removed(
                device, relname=relmap.relname, target=deleted
            )

        for locked in relmap._diff['locked']:
            self._reporter.report_delete_locked(
                device, target=locked, relname=relmap.relname
            )

    def _report_objectmap_changes(self, objectmap, obj):
        self._reporter.report_directive(obj, objectmap)

    def _updateRelationship(self, device, relmap):
        '''This stub is left to satisfy backwards compatability requirements
        for the monkeypatch in ZenPacks.zenoss.PythonCollector
        ZenPacks/zenoss/PythonCollector/patches/platform.py
        '''
        #print('WARNING: _updateRelationship is Deprecated')
        #print('called with %s, %s' % (device, relmap))
        self.applyDataMap(device=device, datamap=relmap)
        pass

    def _removeRelObject(self, device, objmap, relname):
        '''This stub is left to satisfy backwards compatability requirements
        for the monkeypatch in ZenPacks.zenoss.PythonCollector
        ZenPacks/zenoss/PythonCollector/patches/platform.py
        '''
        print('WARNING: _removeRelObject is Deprecated')
        #print('called with %s, %s, %s' %( device, objmap, relname))
        pass

    def _createRelObject(self, device, objmap, relname):
        '''This stub is left to satisfy backwards compatability
        '''
        #print('WARNING: _createRelObject is Deprecated')
        objmap.relname = relname
        idm = IncrementalDataMap(device, objmap)
        changed = self.applyDataMap(device=device, datamap=idm)
        return (changed, idm.target)



##############################################################################
# Preproce, diff and set directives
##############################################################################

def _get_relmap_target(device, relmap):
    '''get the device object associated with this map
    returns the object specified in the datamap
    '''
    device = _validate_device_class(device)
    if not device:
        log.debug('_get_relmap_target: no device found')  # pragma: no mutate
        return None

    pid = getattr(relmap, "parentId", None)
    if pid:
        if device.id == pid:
            return device
        else:
            return _get_object_by_pid(device, pid)

    path = getattr(relmap, 'compname', None)
    if path:
        return device.getObjByPath(relmap.compname)

    return device


def _get_objmap_target(device, objmap):
    objmap._target = device

    target_path = getattr(objmap, 'compname', None)
    if target_path:
        objmap._target = device.getObjByPath(target_path)

    try:
        relationship = getattr(objmap._target, objmap._relname)
        objmap._target = relationship._getOb(objmap.id)
    except Exception:
        log.warn('_get_objmap_target: Unable to find target object')  # pragma: no mutate

    return objmap._target


def _get_objmap_parent(device, objmap):
    parent_id = getattr(objmap, "parentId", None)
    if parent_id:
        if device.id == parent_id:
            return device
        else:
            return device.componentSearch(id=parent_id)
    return device


def _validate_device_class(device):
    '''There's the potential for a device to change device class during
    modeling. Due to this method being run within a retrying @transact,
    this will result in device losing its deviceClass relationship.
    '''
    try:
        if device.deviceClass():
            return device
    except AttributeError:
        pass

    new_device = device.dmd.Devices.findDeviceByIdExact(device.id)
    if new_device:
        log.debug(
            "changed device class during modeling: device=%s, class=%s",  # pragma: no mutate
            new_device.titleOrId(), new_device.getDeviceClassName()
        )
        return new_device

    log.warning(
        "lost its device class during modeling: device=%s", device.titleOrId()  # pragma: no mutate
    )
    return None


def _get_object_by_pid(device, parent_id):
    objects = device.componentSearch(id=parent_id)
    if len(objects) == 1:
        return objects[0].getObject()
    elif len(objects) < 1:
        log.warn('Unable to find a matching parentId: parentID=%s', parent_id)  # pragma: no mutate
    else:
        # all components must have a unique ID
        log.warn('too many matches for parentId: parentId=%s', parent_id)  # pragma: no mutate
    return None


def _validate_datamap(datamap, relname, compname, modname, parentId):
    if isinstance(datamap, RelationshipMap):
        log.debug('_validate_datamap: got valid RelationshipMap')  # pragma: no mutate
    elif relname:
        log.debug('_validate_datamap: build relationship_map using relname')  # pragma: no mutate
        datamap = RelationshipMap(
            relname=relname,
            compname=compname,
            modname=modname,
            objmaps=datamap,
            parentId=parentId
        )
    elif isinstance(datamap, ObjectMap):
        log.debug('_validate_datamap: got valid ObjectMap')  # pragma: no mutate
    elif isinstance(datamap, IncrementalDataMap):
        log.debug('_validate_datamap: got valid IncrementalDataMap')  # pragma: no mutate
    else:
        log.debug('_validate_datamap: build object_map')  # pragma: no mutate
        datamap = ObjectMap(datamap, compname=compname, modname=modname)

    return datamap


def _process_relationshipmap(relmap, base_device):
    relname = relmap.relname
    parent = _get_relmap_target(base_device, relmap)
    if parent:
        relmap._parent = parent
    else:
        log.warn('relationship map parent device not found. relmap=%s', relmap)  # pragma: no mutate
        return False

    if not hasattr(relmap._parent, relname):
        log.warn(
            'relationship not found: parent=%s, relationship=%s',  # pragma: no mutate
            relmap._parent.id, relname,
        )
        return False

    relmap._relname = relmap.relname
    # remove any objects no longer included in the relationshipmap
    # to be deleted (device, relationship_name, object/id)
    relmap._diff = _get_relationshipmap_diff(relmap._parent, relmap)

    # Try replacing the old object maps with incremental maps

    for object_map in relmap:
        #object_map._parent = relmap._parent
        object_map.relname = relmap.relname
        #if not hasattr(object_map, '_relname'):
        #    object_map._relname = relname
        #_set_related_object_directive(relmap._parent, relname, object_map)

    new_maps = [
        IncrementalDataMap(parent, object_map)
        for object_map in relmap.maps
    ]
    relmap.maps = new_maps

    return relmap


def _get_relationshipmap_diff(device, relmap):
    '''Return a list of objects on the device, that are not in the relmap
    '''
    relationship = getattr(device, relmap.relname)
    relids = _get_relationship_ids(device, relmap.relname)
    removed = set(relids) - set([o.id for o in relmap])
    missing_objects = (relationship._getOb(id) for id in removed)

    diff = {'removed': [], 'locked': []}
    for obj in missing_objects:
        if _locked_from_deletion(obj):
            diff['locked'].append(obj)
        else:
            diff['removed'].append(obj)

    return diff


def _get_relationship_ids(device, relationship_name):
    relationship = getattr(device, relationship_name)
    return set(relationship.objectIdsAll())


def _set_related_object_directive(device, relname, object_map):
    '''given an object map from a relationship map
    set the directive for the related object
    '''

    # Can I just...



    object_map = _evaluate_legacy_directive(object_map)

    obj = _get_objmap_target(device, object_map)
    object_map._parent = _get_objmap_parent(device, object_map)

    if hasattr(object_map, '_directive'):
        log.debug('_set_related_object_directive: already has directive')  # pragma: no mutate
        _check_the_locks(object_map, obj)
        return object_map

    _set_objectmap_directive(object_map, obj)

    relationship_ids = _get_relationship_ids(device, relname)

    if obj and object_map.id in relationship_ids:
        if _om_class_changed(object_map, obj):
            object_map._directive = directive_map['_rebuild']
    else:
        object_map._directive = directive_map['_add']

    _check_the_locks(object_map, obj)
    return object_map


def _om_class_changed(object_map, obj):
    '''Handle the possibility of objects changing class by
    recreating them. Ticket #5598.
    a classname of null-string indicates no change
    '''
    if object_map.classname == '':
        return False

    existing_modname, existing_classname = '', ''
    try:
        existing_modname = inspect.getmodule(obj).__name__
        existing_classname = obj.__class__.__name__
    except Exception:
        pass

    if (  # object class has not changed
        object_map.modname == existing_modname
        and object_map.classname == existing_classname
    ):
        log.debug('_om_class_changed: object map matches')  # pragma: no mutate
        return False

    log.debug('_om_class_changed: object_map class changed')  # pragma: no mutate
    return True


def _process_objectmap(object_map, device):
    try:
        return IncrementalDataMap(device, object_map)
    except InvalidIncrementalDataMapError:
        log.info('_evaluate_incremental_update: not an incremental update')  # pragma: no mutate

    object_map._directive = getattr(object_map, '_directive', None)
    object_map._target = _get_objmap_target(device, object_map)
    object_map._parent = _get_objmap_parent(device, object_map)

    if not object_map._directive:
        object_map = _evaluate_legacy_directive(object_map)

    if not object_map._directive:
        object_map = _set_objectmap_directive(object_map, object_map._target)

    if object_map._directive == 'update' and not hasattr(object_map, '_diff'):
        object_map._diff = _objectmap_to_device_diff(
            object_map, object_map._target
        )

    _check_the_locks(object_map, device)

    relname = getattr(object_map, 'relname', None)
    if relname:
        object_map._relname = relname

    return object_map


def _set_objectmap_directive(object_map, device):
    # Do not modify Locked devices
    if _locked_from_updates(device):
        object_map._directive = 'update_locked'
        return object_map

    diff = _objectmap_to_device_diff(object_map, device)
    if diff:
        object_map._directive = directive_map['_update']
        object_map._diff = diff
    else:
        object_map._directive = directive_map['_nochange']

    return object_map


##############################################################################
# Apply Changes
##############################################################################

def _remove_relationship(parent, relname, obj):
    '''Remove a related object from a parent's relationship

    parent: parent device
    relname: name of the relatinship on device
    object_map: object map for the object to be removed from the relationship
    '''
    try:
        parent.removeRelation(relname, obj)
    except AttributeError:
        return False

    return True


def _create_object(object_map, parent_device=None):
    '''Create a new zodb object from an ObjectMap
    '''
    parent = getattr(object_map, '_parent', None)
    constructor = importClass(object_map.modname, object_map.classname)

    if hasattr(object_map, 'id'):
        new_object = constructor(object_map.id)
    elif parent:
        new_object = constructor(parent, object_map)
    elif parent_device:
        new_object = constructor(parent_device, object_map)
    else:
        log.error('_create_object requires object_map.id or parent_device')  # pragma: no mutate
        new_object = None

    return new_object


def _add_object_to_relationship(device, relname, obj):
    relationship = getattr(device, relname, None)
    if not relationship:
        raise ObjectCreationError(
            "relationship not found: device=%s, class=%s relationship=%s"  # pragma: no mutate
            % (device.id, device.__class__, relname,)
        )
    if relationship.hasobject(obj):
        return True

    log.debug(
        'add related object: object=%s, relationship=%s, related_obj=%s',  # pragma: no mutate
        device.id, relname, obj
    )
    # either use device.addRelation(relname, object_map)
    # or create the object, then relationship._setObject(obj.id, obj)
    relationship._setObject(obj.id, obj)
    return True
