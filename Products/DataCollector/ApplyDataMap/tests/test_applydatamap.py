from unittest import TestCase
from mock import Mock, create_autospec, patch, sentinel, MagicMock, call

from Products.ZenModel.Device import Device
from ..applydatamap import (
    log,
    RelationshipMap,
    ObjectMap,
    IncrementalDataMap, InvalidIncrementalDataMapError,
    ZenModelRM,
    ObjectCreationError,
    ObjectNotFound,

    CLASSIFIER_CLASS,
    isSameData,
    ApplyDataMap,

    _get_relationship_ids,
    _validate_device_class,
    _set_objectmap_directive,
    _set_related_object_directive,
    _get_objmap_target,
    _get_objmap_parent,
    _get_relmap_target,
    _get_object_by_pid,
    _om_class_changed,
    _validate_datamap,
    _get_relationshipmap_diff,
    _process_relationshipmap,
    _process_objectmap,

    _remove_relationship,
    _create_object,
    _add_object_to_relationship,
)

log.setLevel('DEBUG')

PATH = {'src': 'Products.DataCollector.ApplyDataMap.applydatamap'}


class TestisSameData(TestCase):

    def test_isSameData(t):
        ret = isSameData('a', 'a')
        t.assertTrue(ret)

    # compare unsorted lists of dictionaries
    def test_unsorted_lists_of_dicts_match(t):
        a = [{'a': 1, 'b': 2}, {'c': 3}, {'d': 4}]
        b = [{'d': 4}, {'c': 3}, {'b': 2, 'a': 1}]
        t.assertTrue(isSameData(a, b))

    def test_unsorted_lists_of_dicts_differ(t):
        a = [{'a': 1, 'b': 2}, {'c': 3}, {'d': 4}]
        c = [{'d': 4}, ]
        t.assertFalse(isSameData(a, c))

    def test_unsorted_tuple_of_dicts_match(t):
        a = ({'a': 1, 'b': 2}, {'c': 3}, {'d': 4})
        b = ({'d': 4}, {'c': 3}, {'b': 2, 'a': 1})
        t.assertTrue(isSameData(a, b))

    def test_unsorted_tuple_of_dicts_differ(t):
        a = ({'a': 1, 'b': 2}, {'c': 3}, {'d': 4})
        c = ({'d': 4},)
        t.assertFalse(isSameData(a, c))

    def test_tuples_match(t):
        a = (('a', 1, 'b', 2), ('c', 3), ('d', 4))
        b = (('d', 4), ('c', 3), ('a', 1, 'b', 2))
        t.assertTrue(isSameData(a, b))

    def test_tuples_differ(t):
        a = (('a', 1, 'b', 2), ('c', 3), ('d', 4))
        b = (('d', 4), ('c', 3), ('x', 10, 'y', 20))
        t.assertFalse(isSameData(a, b))

    def test_type_mismatch(t):
        a, c = 555, ('x', 'y')
        t.assertFalse(isSameData(a, c))


class ApplyDataMapTests(TestCase):

    def setUp(t):
        patches = [
            'notify', '_get_relmap_target', 'ADMReporter', 'DatamapAddEvent'
        ]

        for target in patches:
            patcher = patch('{src}.{}'.format(target, **PATH), autospec=True)
            setattr(t, target, patcher.start())
            t.addCleanup(patcher.stop)

        t.datacollector = Mock(name='datacollector', dmd=Mock(name='dmd'))
        t.adm = ApplyDataMap(t.datacollector)

    def _device_and_objmap(self, map_spec=[], dev_spec=[]):
        relname = 'relationship'
        relationship = Mock(name=relname, spec_set=['_getOb'])
        map_spec += ['_relname', 'id']
        object_map = Mock(
            ObjectMap, name='object_map',
            _relname=relname, id='device_id'
        )
        dev_spec += ['id', 'meta_type', object_map._relname]
        device = Mock(
            name='device', spec_set=dev_spec, id=object_map.id,
            meta_type='Device'
        )
        setattr(device, object_map._relname, relationship)

        self._get_relmap_target.return_value = device
        return device, object_map

    def _device_and_relmap(self):
        device = Mock(Device, name='device')
        device.isLockedFromUpdates.return_value = False
        self._get_relmap_target.return_value = device
        relmap = MagicMock(RelationshipMap)

        self._get_relmap_target.return_value = device
        return device, relmap

    def test___init__(t):
        t.assertEqual(t.adm.datacollector, t.datacollector)
        t.assertEqual(t.adm.num_obj_changed, 0)
        t.assertEqual(t.adm._dmd, t.datacollector.dmd)

    def test___init__without_datacollector(t):
        adm = ApplyDataMap()
        t.assertEqual(adm._dmd, None)
        t.assertEqual(adm.datacollector, None)

    def test_setDeviceClass(t):
        device = Mock(
            name='device',
            spec_set=['getDeviceClassPath', 'changeDeviceClass'],
        )
        device.getDeviceClassPath.return_value = CLASSIFIER_CLASS
        newclass = 'NewClass'

        t.adm.setDeviceClass(device, deviceClass=newclass)

        device.changeDeviceClass.assert_called_with(newclass)

    def test_setDeviceClass_mismatch(t):
        device = Mock(
            name='device',
            spec_set=['getDeviceClassPath', 'changeDeviceClass'],
        )
        device.getDeviceClassPath.return_value = 'NOT_CLASSIFIER_CLASS'

        t.adm.setDeviceClass(device, deviceClass='NewClass')

        device.changeDeviceClass.assert_not_called()

    def test_applyDataMap_false_if_target_not_found(t):
        datamap = Mock(
            RelationshipMap, relname='not_found'
        )
        t._get_relmap_target.return_value = None

        ret = t.adm.applyDataMap(sentinel.device, datamap)
        t.assertEqual(ret, False)

    @patch('{src}._locked_from_updates'.format(**PATH), autospec=True)
    def test_applyDataMap_false_if_target_locked(t, _locked_from_updates):
        t._get_relmap_target.return_value = sentinel.device
        _locked_from_updates.return_value = True
        ret = t.adm.applyDataMap(sentinel.device, sentinel.datamap)
        t.assertEqual(ret, False)

    @patch('{src}._process_relationshipmap'.format(**PATH), autospec=True)
    @patch('{src}.transact'.format(**PATH), autospec=True)
    def test_applyDataMap_RelationshipMap(
        t, transact, _process_relationshipmap,
    ):
        device, relmap = t._device_and_relmap()
        _process_relationshipmap.return_value = relmap
        _get_relmap_target.return_value = device
        # make transact return the argument passed to it
        transact.side_effect = lambda x: x
        t.adm._apply_relationshipmap = create_autospec(
            t.adm._apply_relationshipmap
        )
        t.adm._report_changes = create_autospec(t.adm._report_changes)

        ret = t.adm.applyDataMap(device, relmap)

        t.DatamapAddEvent.assert_called_with(t.adm._dmd, relmap, device)
        t.notify.assert_called_with(t.DatamapAddEvent.return_value)
        transact.assert_called_with(t.adm._apply_relationshipmap)
        _process_relationshipmap.assert_called_with(relmap, device)
        t.adm._apply_relationshipmap.assert_called_with(relmap, device)
        t.adm._report_changes.assert_called_with(relmap, device)
        t.assertEqual(ret, t.adm._report_changes.return_value)

    @patch('{src}._process_objectmap'.format(**PATH), autospec=True)
    @patch('{src}.transact'.format(**PATH), autospec=True)
    def test_applyDataMap_ObjectMap(t, transact, _process_objectmap):
        device, objmap = t._device_and_objmap(dev_spec=['getObjByPath'])
        device.getObjByPath.return_value = device
        _process_objectmap.return_value = objmap
        # make transact return the argument passed to it
        transact.side_effect = lambda x: x
        t.adm._apply_objectmap = create_autospec(t.adm._apply_objectmap)
        t.adm._report_changes = create_autospec(t.adm._report_changes)

        ret = t.adm.applyDataMap(device, objmap)

        t.DatamapAddEvent.assert_called_with(t.adm._dmd, objmap, device)
        t.notify.assert_called_with(t.DatamapAddEvent.return_value)
        _process_objectmap.assert_called_with(objmap, device)
        transact.assert_called_with(t.adm._apply_objectmap)
        t.adm._apply_objectmap.assert_called_with(objmap, device)
        t.adm._report_changes.assert_called_with(objmap, device)
        t.assertEqual(ret, t.adm._report_changes.return_value)

    @patch('{src}.transact'.format(**PATH), autospec=True)
    def test_applyDataMap_IncrementalDataMap(t, transact):
        device, objmap = t._device_and_objmap()
        incremental_dm = Mock(IncrementalDataMap)
        # make transact return the argument passed to it
        transact.side_effect = lambda x: x
        _apply_incrementalmap = create_autospec(t.adm._apply_incrementalmap)
        t.adm._apply_incrementalmap = _apply_incrementalmap
        t.adm._report_changes = create_autospec(t.adm._report_changes)

        ret = t.adm.applyDataMap(device, incremental_dm)

        transact.assert_called_with(_apply_incrementalmap)
        t.adm._apply_incrementalmap.assert_called_with(incremental_dm, device)
        t.adm._report_changes.assert_called_with(incremental_dm, device)
        t.assertEqual(ret, t.adm._report_changes.return_value)

    @patch('{src}._process_relationshipmap'.format(**PATH), autospec=True)
    def test_applyDataMap_RelationshipMap_commit_false(
        t, _process_relationshipmap
    ):
        device, relmap = t._device_and_relmap()
        _process_relationshipmap.return_value = relmap
        t.adm._apply_relationshipmap = create_autospec(
            t.adm._apply_relationshipmap
        )
        t.adm._report_changes = create_autospec(t.adm._report_changes)

        ret = t.adm.applyDataMap(device, relmap, commit=False)

        t.DatamapAddEvent.assert_called_with(t.adm._dmd, relmap, device)
        t.notify.assert_called_with(t.DatamapAddEvent.return_value)
        _process_relationshipmap.assert_called_with(relmap, device)
        t.adm._apply_relationshipmap.assert_called_with(relmap, device)
        t.adm._report_changes.assert_called_with(relmap, device)
        t.assertEqual(ret, t.adm._report_changes.return_value)

    @patch('{src}._process_objectmap'.format(**PATH), autospec=True)
    def test_applyDataMap_ObjectMap_commit_false(t, _process_objectmap):
        device, objmap = t._device_and_objmap()
        _process_objectmap.return_value = objmap
        t.adm._apply_objectmap = create_autospec(t.adm._apply_objectmap)
        t.adm._report_changes = create_autospec(t.adm._report_changes)

        ret = t.adm.applyDataMap(device, objmap, commit=False)

        t.DatamapAddEvent.assert_called_with(t.adm._dmd, objmap, device)
        t.notify.assert_called_with(t.DatamapAddEvent.return_value)
        _process_objectmap.assert_called_with(objmap, device)
        t.adm._apply_objectmap.assert_called_with(objmap, device)
        t.adm._report_changes.assert_called_with(objmap, device)
        t.assertEqual(ret, t.adm._report_changes.return_value)

    def test__applyDataMap(t):
        '''_applyDataMap is an alias for the public API
        because most legacy code calls it directly
        '''
        t.assertEqual(t.adm._applyDataMap, t.adm.applyDataMap)

    @patch('{src}._remove_relationship'.format(**PATH), autospec=True)
    def test__apply_relationshipmap(t, _remove_relationship):
        t.adm._apply_objectmap = create_autospec(
            t.adm._apply_objectmap, return_value=True
        )
        device, relmap = t._device_and_relmap()
        object_map_1 = ObjectMap({'id': 'om1'})
        object_map_2 = ObjectMap({'id': 'om2'})
        relmap._parent = sentinel.parent
        relmap.maps = [object_map_1, object_map_2]
        relmap.__iter__.return_value = relmap.maps
        relmap._diff = {'removed': [sentinel.remove], 'locked': []}

        t.adm._apply_relationshipmap(relmap, device)

        _remove_relationship.assert_called_with(
            relmap._parent, relmap.relname, sentinel.remove
        )
        t.adm._apply_objectmap.assert_has_calls([
            call(object_map_1, device),
            call(object_map_2, device)
        ])

    @patch('{src}._get_relationshipmap_diff'.format(**PATH), autospec=True)
    @patch('{src}._get_relationship_ids'.format(**PATH), autospec=True)
    def test__apply_relationshipmap_invalid_map_exception(
        t, _get_relationship_ids, _get_relationshipmap_diff
    ):
        '''raises an exception if iterating over the relationship map returns
        an invalid objectmap type
        '''
        relmap = MagicMock(name='RelationshipMap', relname='relationship_name')
        relmap.__iter__.return_value = iter([sentinel.invalid])
        device = Mock(name='device')
        device.isLockedFromUpdates.return_value = False

        with t.assertRaises(RuntimeError):
            t.adm._apply_relationshipmap(relmap, device)

    def test__apply_relationshipmap_ZenModelRM(t):
        device, relmap = t._device_and_relmap()
        relmap._diff = {'removed': [], 'locked': []}
        object_map = Mock(ZenModelRM, name='object_map_1', id='om1')
        relmap.__iter__.return_value = [object_map]

        t.adm._apply_relationshipmap(relmap, device)

        device.addRelation.assert_called_with(relmap.relname, object_map)

    def test__apply_objectmap_nochange(t):
        device, objmap = t._device_and_objmap()
        objmap._directive = 'nochange'

        ret = t.adm._apply_objectmap(objmap, device)

        t.assertFalse(ret)

    def test__apply_objectmap_add(t):
        t.adm._add_related_object = create_autospec(t.adm._add_related_object)
        device = Device(id='device_id')
        objmap = ObjectMap({'_relname': sentinel.rel, '_directive': 'add'})

        ret = t.adm._apply_objectmap(objmap, device)

        t.adm._add_related_object.assert_called_with(
            device, objmap._relname, objmap
        )
        t.assertIs(ret, True)

    def test__apply_objectmap_update(t):
        t.adm._update = create_autospec(t.adm._update)
        t.adm._build_directive_map()
        device = Device(id='device_id')
        objmap = ObjectMap(
            {'id': device.id, '_relname': 'relate', '_directive': 'update'}
        )

        ret = t.adm._apply_objectmap(objmap, device)

        t.adm._update.assert_called_with(
            device=device, relname=objmap._relname, object_map=objmap
        )
        t.assertEqual(ret, t.adm._update.return_value)

    @patch('{src}._remove_relationship'.format(**PATH), autospec=True)
    def test__apply_objectmap_remove(t, _remove_relationship):
        device, objmap = t._device_and_objmap()
        objmap._target = device
        objmap._directive = 'remove'

        ret = t.adm._apply_objectmap(objmap, device)

        _remove_relationship.assert_called_with(
            device, objmap._relname, objmap._target
        )
        t.assertEqual(ret, _remove_relationship.return_value)

    @patch('{src}._remove_relationship'.format(**PATH), autospec=True)
    def test__apply_objectmap_rebuild(t, _remove_relationship):
        t.adm._add_related_object = create_autospec(t.adm._add_related_object)
        device, objmap = t._device_and_objmap()
        objmap._directive = 'rebuild'

        ret = t.adm._apply_objectmap(objmap, device)

        _remove_relationship.assert_called_with(
            device, objmap._relname, objmap
        )
        t.adm._add_related_object.assert_called_with(
            device, objmap._relname, objmap
        )
        t.assertIs(ret, True)

    def test__apply_objectmap_directive_update_locked(t):
        device, objmap = t._device_and_objmap()
        objmap._directive = 'update_locked'
        ret = t.adm._apply_objectmap(objmap, device)
        t.assertEqual(ret, False)

    def test__apply_objectmap_directive_delete_locked(t):
        device, objmap = t._device_and_objmap()
        objmap._directive = 'delete_locked'
        ret = t.adm._apply_objectmap(objmap, device)
        t.assertEqual(ret, False)

    def test__apply_incrementalmap(t):
        device = sentinel.device
        imap = Mock(IncrementalDataMap)

        t.adm._apply_incrementalmap(imap, device)

        imap.apply.assert_called_with()

    @patch('{src}.DatamapUpdateEvent'.format(**PATH), autospec=True)
    def test__update(t, DatamapUpdateEvent):
        obj = Device('deviceid')
        obj.attr = sentinel.a
        object_map = ObjectMap({'attr': sentinel.b})
        object_map._diff = {'attr': 'sanitized sentinel.b'}

        ret = t.adm._update(obj, object_map)

        DatamapUpdateEvent.assert_called_with(t.adm._dmd, object_map, obj)
        t.notify.assert_called_with(DatamapUpdateEvent.return_value)
        t.assertEqual(obj.attr, 'sanitized sentinel.b')
        t.assertTrue(ret)

    @patch('{src}.DatamapUpdateEvent'.format(**PATH), autospec=True)
    def test__update_ignore_undefined(t, DatamapUpdateEvent):
        '''do not set new attributes on a device where they are not defined
        '''
        obj = Device('deviceid')
        undefined = 'undefined_attr'
        t.assertFalse(hasattr(obj, undefined))
        obj.attr = sentinel.a
        object_map = ObjectMap({'attr': sentinel.b})
        object_map._diff = {'attr': 'sanitized sentinel.b', undefined: 'NO'}

        ret = t.adm._update(obj, object_map)

        DatamapUpdateEvent.assert_called_with(t.adm._dmd, object_map, obj)
        t.notify.assert_called_with(DatamapUpdateEvent.return_value)
        t.assertEqual(obj.attr, 'sanitized sentinel.b')
        t.assertFalse(hasattr(obj, undefined))
        t.assertTrue(ret)

    @patch('{src}._remove_relationship'.format(**PATH), autospec=True)
    def test__remove_missing_obj(t, _remove_relationship):
        # Why is this exception iterable?!
        _remove_relationship.side_effect = [ObjectNotFound()]
        device = sentinel.device
        objmap = Mock(_target=device, )

        ret = t.adm._remove(device, 'relname', objmap)

        _remove_relationship.assert_called_with(
            device, 'relname', objmap._target
        )
        t.assertEqual(ret, False)

    @patch('{src}._update_object'.format(**PATH), autospec=True)
    @patch('{src}._add_object_to_relationship'.format(**PATH), autospec=True)
    @patch('{src}._create_object'.format(**PATH), autospec=True)
    def test__add_related_object(
        t, _create_object, _add_object_to_relationship, _update_object
    ):
        device, objmap = t._device_and_objmap()
        objmap._parent = device
        objmap._diff = sentinel.diff

        t.adm._add_related_object(device, objmap._relname, objmap)

        _create_object.assert_called_with(objmap, device)
        _add_object_to_relationship.assert_called_with(
            device, objmap._relname, _create_object.return_value
        )
        _update_object.assert_called_with(
            device.relationship._getOb.return_value, objmap._diff
        )

    def test_stop(t):
        '''legacy method, noop
        '''
        t.adm.stop()

    @patch('{src}.log'.format(**PATH), autospec=True)
    def test__report_changes(t, log):
        t.adm._report_relationshipmap_changes = create_autospec(
            t.adm._report_relationshipmap_changes
        )
        device, relmap = t._device_and_relmap()
        objmaps = [Mock(_directive=dir) for dir in ['add', 'update']]
        relmap.__iter__.return_value = objmaps
        relmap._diff = {}

        ret = t.adm._report_changes(relmap, device)

        log.info.assert_called_with(
            'applied RelationshipMap changes: target=%s.%s, change_counts=%s',
            device.id, relmap.relname,
            {
                'nochange': 0, 'rebuild': 0, 'delete_locked': 0, 'update': 1,
                'remove': 0, 'add': 1, 'update_locked': 0
            }
        )
        t.adm._report_relationshipmap_changes.assert_called_with(
            relmap, device
        )
        t.assertEqual(ret, True)

    def test__report_changes_for_relationshipmap_nochange(t):
        t.adm._report_relationshipmap_changes = create_autospec(
            t.adm._report_relationshipmap_changes
        )
        device, relmap = t._device_and_relmap()
        objmaps = [Mock(_directive=dir) for dir in ['nochange', 'nochange']]
        relmap.__iter__.return_value = objmaps
        relmap._diff = {'removed': []}

        ret = t.adm._report_changes(relmap, device)

        t.adm._report_relationshipmap_changes.assert_called_with(
            relmap, device
        )
        t.assertEqual(ret, False)

    def test__report_changes_for_objectmap(t):
        t.adm._report_objectmap_changes = create_autospec(
            t.adm._report_objectmap_changes
        )
        device, objmap = t._device_and_objmap()
        objmap._directive = 'update'

        ret = t.adm._report_changes(objmap, device)

        t.adm._report_objectmap_changes.assert_called_with(
            objmap, device
        )
        t.assertEqual(ret, True)

    def test__report_changes_for_objectmap_add(t):
        t.adm._report_objectmap_changes = create_autospec(
            t.adm._report_objectmap_changes
        )
        device, objmap = t._device_and_objmap()
        objmap._directive = 'add'

        ret = t.adm._report_changes(objmap, device)

        t.adm._report_objectmap_changes.assert_called_with(
            objmap, device
        )
        t.assertEqual(ret, True)

    def test__report_changes_no_change(t):
        t.adm._report_objectmap_changes = create_autospec(
            t.adm._report_objectmap_changes
        )
        device, objmap = t._device_and_objmap()
        objmap._directive = 'nochange'

        ret = t.adm._report_changes(objmap, device)

        t.adm._report_objectmap_changes.assert_called_with(
            objmap, device
        )
        t.assertEqual(ret, False)

    def test__report_changes_for_incrementaldatamap(t):
        t.adm._report_objectmap_changes = create_autospec(
            t.adm._report_objectmap_changes
        )
        incrementaldatamap = Mock(IncrementalDataMap)
        device = sentinel.device

        ret = t.adm._report_changes(incrementaldatamap, device)

        t.adm._report_objectmap_changes.assert_called_with(
            incrementaldatamap, device
        )
        t.assertEqual(ret, incrementaldatamap.changed)

    def test__report_changes_for_unknown(t):
        ret = t.adm._report_changes('unknown type', 'device')
        t.assertEqual(ret, False)

    def test__report_relationshipmap_changes(t):
        device, relmap = t._device_and_relmap()
        relmap._diff = {
            'removed': [sentinel.remove], 'locked': [sentinel.lock]
        }
        relmap.__iter__.return_value = [sentinel.objectmap, ]

        t.adm._report_relationshipmap_changes(relmap, device)

        t.adm._reporter.report_removed.assert_called_with(
            device, relname=relmap.relname, target=sentinel.remove
        )
        t.adm._reporter.report_delete_locked.assert_called_with(
            device, relname=relmap.relname, target=sentinel.lock
        )

    def test_report_objectmap_changes(t):
        obj, objmap = t._device_and_objmap()
        t.adm._report_objectmap_changes(objmap, obj)
        t.adm._reporter.report_directive.assert_called_with(obj, objmap)

    def test__updateRelationship(t):
        '''legacy method, exists so zenpack monkey patches won't fail
        '''
        t.adm._updateRelationship(MagicMock(), MagicMock())

    def test__removeRelObject(t):
        '''legacy method, exists so zenpack monkey patches won't fail
        '''
        t.adm._removeRelObject('device', 'objmap', 'relname')

##############################################################################
# Preproce, diff and set directives
##############################################################################


class Test_get_relmap_target(TestCase):

    def test__get_relmap_target(t):
        device = Mock(name='device', id=sentinel.pid)
        datamap = Mock(name='datamap', parentId=None, compname=None)
        ret = _get_relmap_target(device, datamap)
        t.assertEqual(ret, device)

    def test__get_relmap_target_pid(t):
        device = Mock(name='device', id=sentinel.pid)
        datamap = Mock(name='datamap', parentId=sentinel.pid)
        ret = _get_relmap_target(device, datamap)
        t.assertEqual(ret, device)

    @patch('{src}._get_object_by_pid'.format(**PATH), autospec=True)
    def test__get_relmap_target_pid_mismatch(t, _get_object_by_pid):
        device = Mock(name='device', id=sentinel.id)
        datamap = Mock(name='datamap', parentId=sentinel.pid)

        ret = _get_relmap_target(device, datamap)
        _get_object_by_pid.assert_called_with(device, datamap.parentId)
        t.assertEqual(ret, _get_object_by_pid.return_value)

    def test__get_relmap_target_component(t):
        device = Mock(
            name='device', id=sentinel.id, getObjByPath=lambda x: sentinel.obj
        )
        datamap = Mock(
            name='datamap', parentId=None, compname=sentinel.compname
        )
        device.componentSearch.return_value = []

        ret = _get_relmap_target(device, datamap)
        t.assertEqual(ret, sentinel.obj)

    @patch('{src}._validate_device_class'.format(**PATH), autospec=True)
    def test__get_relmap_target_invalid_device(t, _validate_device_class):
        _validate_device_class.return_value = None
        ret = _get_relmap_target(sentinel.device, sentinel.datamap)
        t.assertIs(None, ret)


class Test_validate_device_class(TestCase):

    def test__validate_device_class(t):
        device = Mock(name='device', deviceClass=lambda: 'some class')
        ret = _validate_device_class(device)
        t.assertEqual(ret, device)

    def test__validate_device_class_dmd_lookup(t):
        device = Mock(name='device', deviceClass=lambda: None)
        ret = _validate_device_class(device)

        device.dmd.Devices.findDeviceByIdExact.assert_called_with(device.id)
        t.assertEqual(ret, device.dmd.Devices.findDeviceByIdExact.return_value)

    def test__validate_device_class_lost(t):
        device = Mock(name='device', deviceClass=lambda: None)
        device.dmd.Devices.findDeviceByIdExact.return_value = None

        ret = _validate_device_class(device)
        t.assertEqual(ret, None)

    def test__validate_device_class_missing(t):
        device = Mock(
            name='device', deviceClass=Mock(side_effect=AttributeError())
        )
        device.dmd.Devices.findDeviceByIdExact.return_value = None

        ret = _validate_device_class(device)
        t.assertEqual(ret, None)


class Test_get_object_by_pid(TestCase):

    def test_finds_object(t):
        device = Mock(name='device')
        device.componentSearch.return_value = [
            Mock(name='object ref', getObject=lambda: sentinel.obj)
        ]

        ret = _get_object_by_pid(device, sentinel.parent_id)
        device.componentSearch.assert_called_with(id=sentinel.parent_id)
        t.assertEqual(ret, sentinel.obj)

    @patch('{src}.log'.format(**PATH), autospec=True)
    def test_too_many_matches(t, log):
        device = Mock(name='device')
        device.componentSearch.return_value = ['a', 'b']

        ret = _get_object_by_pid(device, sentinel.parent_id)
        device.componentSearch.assert_called_with(id=sentinel.parent_id)
        log.warn.assert_called_with(
            'too many matches for parentId: parentId=%s', sentinel.parent_id
        )
        t.assertEqual(ret, None)

    @patch('{src}.log'.format(**PATH), autospec=True)
    def test_no_matches(t, log):
        device = Mock(name='device', id=sentinel.id)
        device.componentSearch.return_value = []

        ret = _get_object_by_pid(device, sentinel.parent_id)

        device.componentSearch.assert_called_with(id=sentinel.parent_id)
        log.warn.assert_called_with(
            'Unable to find a matching parentId: parentID=%s',
            sentinel.parent_id
        )
        t.assertEqual(ret, None)


class Test__validate_datamap(TestCase):

    def test_relationshipmap(t):
        datamap = RelationshipMap()
        ret = _validate_datamap(
            datamap, 'relname', 'compname', 'modname', 'parentId'
        )
        t.assertEqual(ret, datamap)

    def test_objectmap(t):
        datamap = ObjectMap({'id': sentinel.deviceid})
        ret = _validate_datamap(
            datamap, None, 'compname', 'modname', 'parentId'
        )
        t.assertEqual(ret, datamap)

    def test_relname_means_relationshipmap(t):
        '''Legacy API Asumption:
        given a ObjectMap, and including a relname
        return a relationshipMap
        '''
        object_map = ObjectMap({'id': sentinel.deviceid})
        datamap = [object_map]
        ret = _validate_datamap(
            datamap, 'relname', 'compname', sentinel.modname, 'parentId'
        )

        t.assertIsInstance(ret, RelationshipMap)
        for objmap in ret:
            t.assertEqual(objmap.modname, sentinel.modname)
            t.assertEqual(objmap.id, sentinel.deviceid)

    def test_build_objectmap_from_dict(t):
        datamap = {'id': sentinel.deviceid}
        ret = _validate_datamap(
            datamap, None, sentinel.compname, sentinel.modname, 'parentId'
        )
        t.assertIsInstance(ret, ObjectMap)
        t.assertEqual(ret.id, sentinel.deviceid)
        t.assertEqual(ret.modname, sentinel.modname)
        t.assertEqual(ret.compname, sentinel.compname)


class Test_process_relationshipmap(TestCase):

    @patch('{src}._get_relmap_target'.format(**PATH), autospec=True)
    def test_missing_relname(t, _get_relmap_target):
        '''returns false if the parent device does not have the specified
        relationship
        '''
        parent_device = Mock(
            name='parent_device', spec_set=['id', 'relname', ],
            relname='relationshipname', id='pid',
        )
        _get_relmap_target.return_value = parent_device
        relmap = Mock(name='relmap')
        relmap.relname = 'relationshipname'

        ret = _process_relationshipmap(relmap, parent_device)

        t.assertEqual(ret, False)

    @patch('{src}._get_relationshipmap_diff'.format(**PATH), autospec=True)
    def test_process_relationshipmap(t, _get_relationshipmap_diff):
        device = Mock(name='device')
        relmap = RelationshipMap(
            relname="interfaces",
            modname="Products.ZenModel.IpInterface",
        )
        om1 = ObjectMap({'id': 'eth0'})
        om2 = ObjectMap({'id': 'eth1'})
        relmap.maps = [om1, om2, ]

        _process_relationshipmap(relmap, device)

        t.assertEqual(len(relmap.maps), 2)
        for omap in relmap.maps:
            t.assertEqual(omap.relname, relmap.relname)
            t.assertEqual(omap.parent, relmap._parent)
        t.assertEqual(relmap._diff, _get_relationshipmap_diff.return_value)


class Test__get_relationshipmap_diff(TestCase):

    def setUp(t):
        current_ids = ['id1', 'id2', 'id3']
        relname = 'relationship'
        t.object_1 = Mock(id='id1')
        t.object_2 = Mock(id='id2')
        t.relationship = Mock(
            name=relname, spec_set=['objectIdsAll', '_getOb']
        )
        t.relationship.objectIdsAll.return_value = current_ids
        t.relationship._getOb.side_effect = [t.object_1, t.object_2]
        t.relmap = MagicMock(
            RelationshipMap, name='object_map', relname=relname, id='device_id'
        )
        t.relmap.__iter__.return_value = [Mock(id='id3'), Mock(id='id4')]
        t.device = Mock(
            name='device', id=t.relmap.id,
            spec_set=['id', t.relmap.relname, 'removeRelation'],
        )
        setattr(t.device, t.relmap.relname, t.relationship)

    def test_remove_from_relationship(t):
        t.object_1.isLockedFromDeletion.return_value = False
        t.object_2.isLockedFromDeletion.return_value = False
        ret = _get_relationshipmap_diff(t.device, t.relmap)

        t.relationship._getOb.assert_called_with('id1')
        t.assertEqual(
            ret, {'removed': [t.object_1, t.object_2], 'locked': []}
        )

    def test_does_not_remove_locked_devices(t):
        t.object_1.isLockedFromDeletion.return_value = False
        t.object_2.isLockedFromDeletion.return_value = True
        ret = _get_relationshipmap_diff(t.device, t.relmap)

        t.relationship._getOb.assert_called_with('id1')
        t.assertEqual(
            ret, {'removed': [t.object_1], 'locked': [t.object_2]}
        )


class Test_get_relationship_ids(TestCase):

    def test__get_relationship_ids(t):
        relname = 'relationship_name'
        relationship = Mock(name='relatinoship')
        relationship.objectIdsAll.return_value = ['r1', 'r2', 'r2', 'r3']
        device = Mock(name='Device')
        setattr(device, relname, relationship)

        ret = _get_relationship_ids(device, relname)

        t.assertEqual(ret, set(['r1', 'r2', 'r3']))


class Test_set_related_object_directive(TestCase):

    def setUp(t):
        t.relname = 'relationship_name'
        t.object_id = sentinel.object_id
        t.object_map = ObjectMap({'modname': 'module', 'id': t.object_id})
        t.device = Mock(name='Device')
        t.related_object = Mock(name='related_object')
        t.related_object.isLockedFromUpdates.return_value = False
        t.related_object.isLockedFromDeletion.return_value = False

        patches = ['_get_relationship_ids', '_get_objmap_target']

        for target in patches:
            patcher = patch('{src}.{}'.format(target, **PATH), autospec=True)
            setattr(t, target, patcher.start())
            t.addCleanup(patcher.stop)

        t._get_objmap_target.return_value = t.related_object

    def test_given_remove(t):
        t.object_map.remove = True
        ret = _set_related_object_directive(t.device, t.relname, t.object_map)

        t.assertEqual(t.object_map._parent, t.device)
        t.assertEqual(ret._directive, 'remove')
        t.assertEqual(t.object_map._directive, 'remove')

    def test_given__remove(t):
        t.object_map._remove = True
        ret = _set_related_object_directive(t.device, t.relname, t.object_map)
        t.assertEqual(ret._directive, 'remove')

    def test_device_locked_from_deletion(t):
        t.object_map._remove = True
        t.related_object.isLockedFromDeletion.return_value = True
        ret = _set_related_object_directive(t.device, t.relname, t.object_map)
        t.assertEqual(ret._directive, 'delete_locked')

    def test_set_add(t):
        t._get_relationship_ids.return_value = {}
        ret = _set_related_object_directive(t.device, t.relname, t.object_map)
        t.assertEqual(ret._directive, 'add')

    @patch('{src}._om_class_changed'.format(**PATH), autospec=True)
    def test_set_rebuild(t, _om_class_changed):
        _om_class_changed.return_value = True
        t._get_relationship_ids.return_value = {t.object_id}
        ret = _set_related_object_directive(t.device, t.relname, t.object_map)
        t.assertEqual(ret._directive, 'rebuild')

    @patch('{src}._objectmap_to_device_diff'.format(**PATH), autospec=True)
    @patch('{src}._om_class_changed'.format(**PATH), autospec=True)
    def test_set_update(
        t, _om_class_changed, _objectmap_to_device_diff,
    ):
        t._get_relationship_ids.return_value = {t.object_id}
        _om_class_changed.return_value = False
        _objectmap_to_device_diff.return_value = {'attr': 'updated'}

        ret = _set_related_object_directive(t.device, t.relname, t.object_map)

        t._get_relationship_ids.assert_called_with(t.device, t.relname)
        t.assertEqual(ret._directive, 'update')

    @patch('{src}._objectmap_to_device_diff'.format(**PATH), autospec=True)
    @patch('{src}._om_class_changed'.format(**PATH), autospec=True)
    def test_object_locked_from_updates(
        t, _om_class_changed, _objectmap_to_device_diff,
    ):
        t._get_relationship_ids.return_value = {t.object_id}
        _om_class_changed.return_value = False
        _objectmap_to_device_diff.return_value = {'attr': 'updated'}
        t.related_object.isLockedFromUpdates.return_value = True

        ret = _set_related_object_directive(t.device, t.relname, t.object_map)

        t.assertEqual(ret._directive, 'update_locked')


class Test__get_objmap_target(TestCase):

    def test__get_objmap_target(t):
        device = Mock(name='device')
        relname = 'relationship_name'
        relationship = device.relationship_name
        relationship._getOb.return_value = sentinel.object
        object_map = ObjectMap({'id': 'object_id', '_relname': relname})

        ret = _get_objmap_target(device, object_map)

        relationship._getOb.assert_called_with(object_map.id)
        t.assertEqual(ret, sentinel.object)

    def test__get_objmap_target_component(t):
        device = Mock(
            name='device',
            spec_set=['component_name', 'id', 'getObjByPath']
        )
        device.getObjByPath.return_value = sentinel.component
        object_map = Mock(
            name='object_map', spec_set=['compname', '_relname', '_target'],
            compname='component_name', _relname='relationship_name'
        )

        ret = _get_objmap_target(device, object_map)

        device.getObjByPath.assert_called_with('component_name')
        t.assertEqual(ret, sentinel.component)


class Test__get_objmap_parent(TestCase):

    def test__getobjmap_parent(t):
        parent_id = 'pid'
        device = Mock(name='device', spec_set=['id', ], id=parent_id, )
        object_map = ObjectMap({'id': 'object_id', 'parentId': parent_id})

        ret = _get_objmap_parent(device, object_map)

        t.assertEqual(ret, device)

    def test__getobjmap_parent_is_component(t):
        parent_id = 'pid'
        device = Mock(
            name='device', spec_set=['id', 'componentSearch'], id='not pid',
        )
        object_map = ObjectMap({'id': 'object_id', 'parentId': parent_id})

        ret = _get_objmap_parent(device, object_map)

        device.componentSearch.assert_called_with(id=parent_id)
        t.assertEqual(ret, device.componentSearch.return_value)

    def test__getobjmap_parent_not_found(t):
        device = Mock(name='device', spec_set=['id', ], id='not pid', )
        object_map = ObjectMap({'id': 'object_id', })

        ret = _get_objmap_parent(device, object_map)

        t.assertEqual(ret, device)


class Test_om_class_changed(TestCase):

    def setUp(t):
        t.obj = Device('oid')
        t.object_map = ObjectMap(data={
            'id': 'objectid',
            'modname': 'Products.ZenModel.Device',
            'classname': 'Device'
        })

    def test_unchanged(t):
        ret = _om_class_changed(t.object_map, t.obj)
        t.assertEqual(ret, False)

    def test_class_changed(t):
        t.object_map.classname = 'NewDevice'
        ret = _om_class_changed(t.object_map, t.obj)
        t.assertEqual(ret, True)

    def test_class_is_nullstr(t):
        t.object_map.classname = ''
        ret = _om_class_changed(t.object_map, t.obj)
        t.assertEqual(ret, False)

    @patch('{src}.inspect'.format(**PATH), autospec=True)
    def test_handles_exception_getting_obj_class(t, inspect):
        inspect.getmodule.side_effect = Exception()
        ret = _om_class_changed(t.object_map, t.obj)
        t.assertEqual(ret, True)

    def test_module_changed(t):
        t.object_map.modname = 'Products.ZenModel.NewDevice'
        ret = _om_class_changed(t.object_map, t.obj)
        t.assertEqual(ret, True)


class Test__process_objectmap(TestCase):

    def test_given_directive(t):
        object_map = ObjectMap({'_directive': 'update'})
        ret = _process_objectmap(object_map, sentinel.device)
        t.assertEqual(ret, object_map)
        t.assertEqual(object_map._directive, 'update')
        t.assertEqual(object_map._parent, sentinel.device)

    def test_legacy_directive(t):
        object_map = ObjectMap({'_remove': True})
        ret = _process_objectmap(object_map, sentinel.device)
        t.assertEqual(ret, object_map)
        t.assertEqual(object_map._directive, 'remove')

    @patch('{src}._set_objectmap_directive'.format(**PATH), autospec=True)
    def test_unknown_directive(t, _set_objectmap_directive):
        '''_process_objectmap calls _set_objectmap_directive
        if a directive is not given
        '''
        device = Mock(name='device', spec_set=['getObjByPath', ])
        object_map = ObjectMap({})
        ret = _process_objectmap(object_map, device)
        t.assertEqual(ret, _set_objectmap_directive.return_value)

    def test_handles_relname(t):
        object_map = ObjectMap({'_directive': 'update', 'relname': 'rel'})
        ret = _process_objectmap(object_map, sentinel.device)
        t.assertEqual(ret, object_map)
        t.assertEqual(object_map._directive, 'update')
        t.assertEqual(object_map._parent, sentinel.device)
        t.assertEqual(object_map._relname, 'rel')

    def test_handles_incremental_updates(t):
        device = Mock(Device)
        object_map = ObjectMap({
            "id": "eth0",
            "compname": "os",
            "relname": "interfaces",
            'remove': True,
        })

        ret = _process_objectmap(object_map, device)

        t.assertIsInstance(ret, IncrementalDataMap)

    @patch('{src}._objectmap_to_device_diff'.format(**PATH), autospec=True)
    def test_legacy_update_includes_diff(t, _objectmap_to_device_diff):
        '''Update directives must include a diff
        '''
        object_map = ObjectMap({'_update': True})
        device = Mock(name='device', spec_set=['getObjByPath', ])

        ret = _process_objectmap(object_map, device)

        _objectmap_to_device_diff.assert_called_with(object_map, device)
        t.assertEqual(ret, object_map)
        t.assertEqual(object_map._diff, _objectmap_to_device_diff.return_value)

    @patch('{src}.IncrementalDataMap'.format(**PATH), autospec=True)
    def test_checks_update_locks(t, IncrementalDataMap):
        IncrementalDataMap.side_effect = InvalidIncrementalDataMapError()
        object_map = ObjectMap({'_directive': 'update'})
        device = Mock(
            name='device', spec_set=['getObjByPath', 'isLockedFromUpdates'],
            isLockedFromUpdates=Mock(return_value=True),
        )

        ret = _process_objectmap(object_map, device)

        t.assertEqual(ret, object_map)
        t.assertEqual(object_map._directive, 'update_locked')

    @patch('{src}.IncrementalDataMap'.format(**PATH), autospec=True)
    def test_checks_delete_locks(t, IncrementalDataMap):
        IncrementalDataMap.side_effect = InvalidIncrementalDataMapError()
        object_map = ObjectMap({'_directive': 'remove'})
        device = Mock(
            name='device', spec_set=['getObjByPath', 'isLockedFromDeletion'],
            isLockedFromDeletion=Mock(return_value=True),
        )

        ret = _process_objectmap(object_map, device)

        t.assertEqual(ret, object_map)
        t.assertEqual(object_map._directive, 'delete_locked')

    def test_do_not_change_device_id(t):
        device = Mock(name='device', spec_set=['id', 'getObjByPath', ], id='x')
        object_map = ObjectMap({'id': 'new_id'})

        ret = _process_objectmap(object_map, device)

        t.assertNotIn('id', ret._diff)


class Test_set_objectmap_directive(TestCase):

    def setUp(t):
        t.object_id = sentinel.object_id
        t.object_map = Mock(
            ObjectMap, name='object_map', modname='module', id=t.object_id,
            _relname='relationship_name', _directive=None
        )
        t.device = Mock(name='Device')
        t.device.isLockedFromUpdates.return_value = False

        _get_relationship_ids_patcher = patch(
            '{src}._get_relationship_ids'.format(**PATH), autospec=True
        )
        t._get_relationship_ids = _get_relationship_ids_patcher.start()
        t.addCleanup(_get_relationship_ids_patcher.stop)

    @patch('{src}._objectmap_to_device_diff'.format(**PATH), autospec=True)
    def test_set_update(t, _objectmap_to_device_diff):
        '''set _directive = 'add' if object_map.id not in relathinship ids
        '''
        _objectmap_to_device_diff.return_value = {'attr': 'has changed'}
        _set_objectmap_directive(t.object_map, t.device)
        t.assertEqual(t.object_map._directive, 'update')

    @patch('{src}._objectmap_to_device_diff'.format(**PATH), autospec=True)
    def test_set_nochange(t, _objectmap_to_device_diff):
        _objectmap_to_device_diff.return_value = {}
        _set_objectmap_directive(t.object_map, t.device)
        t.assertEqual(t.object_map._directive, 'nochange')

    def test_device_locked_from_updates(t):
        t.device.isLockedFromUpdates.return_value = True
        ret = _set_objectmap_directive(t.object_map, t.device)
        t.assertEqual(ret._directive, 'update_locked')


##############################################################################
# Apply Changes
##############################################################################

class Test__remove_relationship(TestCase):

    def test_remove_object_from_devices_relationship(t):
        device = Mock(name='device', spec_set=['removeRelation'])
        ret = _remove_relationship(device, sentinel.relname, sentinel.obj)
        device.removeRelation.assert_called_with(
            sentinel.relname, sentinel.obj
        )
        t.assertEqual(ret, True)

    def test_remove_missing_object(t):
        device = Mock(
            name='device', spec_set=['removeRelation'],
            removeRelation=Mock(side_effect=AttributeError()),
        )

        ret = _remove_relationship(device, sentinel.relname, sentinel.obj)

        device.removeRelation.assert_called_with(
            sentinel.relname, sentinel.obj
        )
        t.assertEqual(ret, False)


class Test__create_object(TestCase):

    def setUp(t):
        patches = ['importClass', ]

        for target in patches:
            patcher = patch('{src}.{}'.format(target, **PATH), autospec=True)
            setattr(t, target, patcher.start())
            t.addCleanup(patcher.stop)

        t.new_object = sentinel.new_object
        t.constructor = Mock(name='constructor', return_value=t.new_object)
        t.importClass.return_value = t.constructor

    def test__create_object(t):
        objmap = ObjectMap(
            {'id': 'deviceid'}, modname='py.module.name', classname='ClassName'
        )

        ret = _create_object(objmap)

        t.importClass.assert_called_with(objmap.modname, objmap.classname)
        t.constructor.assert_called_with(objmap.id)
        t.assertEqual(ret, t.new_object)

    def test_without_id(t):
        objmap = ObjectMap({}, modname='py.module.name', classname='ClassName')

        ret = _create_object(objmap, sentinel.parent_device)

        t.importClass.assert_called_with(objmap.modname, objmap.classname)
        t.constructor.assert_called_with(sentinel.parent_device, objmap)
        t.assertEqual(ret, t.new_object)

    def test_from_parent(t):
        objmap = ObjectMap({}, modname='py.module.name', classname='ClassName')
        objmap._parent = sentinel.parent

        ret = _create_object(objmap)

        t.importClass.assert_called_with(objmap.modname, objmap.classname)
        t.constructor.assert_called_with(sentinel.parent, objmap)
        t.assertEqual(ret, t.new_object)

    def test_failure(t):
        '''requires object_map.id or a parent device
        '''
        objmap = ObjectMap({}, modname='py.module.name', classname='ClassName')
        ret = _create_object(objmap)
        t.assertIsNone(ret)


class Test_add_object_to_relationship(TestCase):

    def setUp(t):
        t.obj = Mock(name='obj', spec_set=['id'], id=sentinel.obj_id)
        t.device = MagicMock(name='device')
        t.relname = 'relationship'

    def test_add(t):
        t.device.relationship.hasobject.return_value = False
        ret = _add_object_to_relationship(t.device, t.relname, t.obj)
        t.device.relationship._setObject.assert_called_with(t.obj.id, t.obj)
        t.assertEqual(ret, True)

    def test_missing_relationship_exception(t):
        setattr(t.device, t.relname, None)
        with t.assertRaises(ObjectCreationError):
            _add_object_to_relationship(t.device, t.relname, t.obj)

    def test_already_has_object(t):
        t.device.relationship.hasobject.return_value = True
        ret = _add_object_to_relationship(t.device, t.relname, t.obj)
        t.device.addRelation.assert_not_called()
        t.assertEqual(ret, True)
