/*
###########################################################################
#
# This program is part of Zenoss Core, an open source monitoring platform.
# Copyright (C) 2009, Zenoss Inc.
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License version 2 or (at your
# option) any later version as published by the Free Software Foundation.
#
# For complete information please visit: http://www.zenoss.com/oss/
#
###########################################################################
*/

Ext.ns('Zenoss.ui.Triggers');

Ext.onReady(function () {

    var router = Zenoss.remote.TriggersRouter,
        ZFR = Zenoss.form.rule,
        STRINGCMPS = ZFR.STRINGCOMPARISONS,
        NUMCMPS = ZFR.NUMBERCOMPARISONS,
        AddDialogue,
        addNotificationDialogue,
        addNotificationDialogueConfig,
        addScheduleDialogue,
        addScheduleDialogueConfig,
        addTriggerDialogue,
        colModel,
        colModelConfig,
        detailPanelConfig,
        displayEditTriggerDialogue,
        displayNotificationEditDialogue,
        displayScheduleEditDialogue,
        EditNotificationDialogue,
        editNotificationDialogue,
        editNotificationDialogueConfig,
        editScheduleDialogue,
        editScheduleDialogueConfig,
        editTriggerDialogue,
        EditTriggerDialogue,
        EditScheduleDialogue,
        masterPanelConfig,
        navSelectionModel,
        NotificationPageLayout,
        notificationPanelConfig,
        notificationsPanelConfig,
        NotificationSubscriptions,
        notification_panel,
        PageLayout,
        reloadNotificationGrid,
        reloadScheduleGrid,
        reloadTriggersGrid,
        SchedulesPanel,
        schedulesPanelConfig,
        schedules_panel,
        TriggersGridPanel,
        triggersPanelConfig,
        disableTabContents;

    // visual items
    var bigWindowHeight = 450;
    var bigWindowWidth = 600;
    var panelPadding = 10;

    AddDialogue = Ext.extend(Ext.Window, {
        constructor: function(config) {
            config = config || {};
            Ext.applyIf(config, {
                height: 120,
                width: 300,
                boxMaxWidth: 300, // for chrome, safari
                modal: true,
                plain: true,
                closeAction: 'hide',
                items:{
                    xtype:'form',
                    ref: 'addForm',
                    border: false,
                    monitorValid: true,
                    buttonAlign: 'center',
                    items:[{
                        xtype: 'textfield',
                        name: 'newId',
                        ref: 'newId',
                        allowBlank: false,
                        vtype: 'alphanum',
                        fieldLabel: _t('Id')
                    }],
                    buttons:[{
                        xtype: 'button',
                        text: _t('Submit'),
                        ref: '../../submitButton',
                        formBind: true,
                        /*
                         * This dialogue is used to generically add objects for
                         * triggers, notifications and schedule windows. For
                         * triggers and notifications, do the normal thing, but
                         * for schedules, let the config pass in a handler
                         * since creating a window requires slightly different
                         * context.
                         */
                        handler: function(button) {
                            if (config.submitHandler) {
                                config.submitHandler(button);
                            } else {
                                var params = {
                                    newId: button.refOwner.addForm.newId.getValue()
                                };
                                config.directFn(params, function(){
                                    button.refOwner.addForm.newId.setValue('');
                                    config.reloadFn();
                                    button.refOwner.hide();
                                });
                            }
                        }
                    },{
                        xtype: 'button',
                        ref: '../../cancelButton',
                        text: _t('Cancel'),
                        handler: function(button) {
                            button.refOwner.hide();
                        }
                    }]}
            });
            AddDialogue.superclass.constructor.apply(this, arguments);
        }
    });
    Ext.reg('triggersadddialogue', AddDialogue);


    /**
     * NOTIFICATIONS
     **/

    notificationPanelConfig = {
        id: 'notification_panel',
        xtype: 'notificationsubscriptions'
    };

    schedulesPanelConfig = {
        id: 'schedules_panel',
        xtype: 'schedulespanel'
    };


    disableTabContents = function(tab) {
        // disable everything in this tab, but then re-enable the tab itself so
        // that we can still view it's contents.
        tab.cascade(function(){
            this.disable();
        });
        tab.setDisabled(false);
    };

    var enableTabContents = function(tab) {
        tab.cascade(function() {
            this.enable();
        });
        tab.setDisabled(false);
    }

    reloadNotificationGrid = function() {
        Ext.getCmp(notificationPanelConfig.id).getStore().reload();
    };


    reloadScheduleGrid = function() {
        var panel = Ext.getCmp(notificationPanelConfig.id),
            row = panel.getSelectionModel().getSelected();
        if (row) {
            Ext.getCmp(schedulesPanelConfig.id).getStore().reload({uid:row.data.uid});
        }
    };

    displayScheduleEditDialogue = function(data) {
        var dialogue = Ext.getCmp(editScheduleDialogueConfig.id);
        dialogue.setTitle(String.format('{0} - {1}', editScheduleDialogueConfig.title, data['newId']));
        dialogue.loadData(data);
        dialogue.show();
    };

    editScheduleDialogueConfig = {
        id: 'edit_schedule_dialogue',
        xtype: 'editscheduledialogue',
        title: _t('Edit Notification Schedule'),
        directFn: router.updateWindow,
        reloadFn: reloadScheduleGrid
    };

    addScheduleDialogueConfig = {
        id: 'add_schedule_dialogue',
        xtype: 'addialogue',
        title: _t('Add Schedule Window'),
        directFn: router.addWindow,
        submitHandler: function(button) {
            var panel = Ext.getCmp(notificationPanelConfig.id),
                row = panel.getSelectionModel().getSelected(),
                params = {
                    newId: button.refOwner.addForm.newId.getValue(),
                    contextUid: row.data.uid
                };

            router.addWindow(params, function(){
                button.refOwner.addForm.newId.setValue('');
                button.refOwner.hide();
                reloadScheduleGrid();
            });
        }
    };


    var writeColumn = new Ext.grid.CheckColumn({
        header: _t('Write'),
        dataIndex: 'write'
    });

    var manageColumn = new Ext.grid.CheckColumn({
        header: _t('Manage'),
        dataIndex: 'manage'
    });

    var UsersPermissionGrid = Ext.extend(Ext.grid.EditorGridPanel, {
        constructor: function(config) {
            var me = this;

            config = config || {};
            this.allowManualEntry = config.allowManualEntry || false;
            Ext.applyIf(config, {
                ref: 'users_grid',
                border: false,
                viewConfig: {forceFit: true},
                title: config.title,
                autoExpandColumn: 'value',
                loadMask: {msg:_t('Loading...')},
                autoHeight: true,
                plugins: [writeColumn, manageColumn],
                keys: [
                    {
                        key: [Ext.EventObject.ENTER],
                        handler: function() {
                            me.addValueFromCombo();
                        }
                    }
                ],
                tbar: {
                    items: [
                        {
                            xtype: 'combo',
                            ref: 'users_combo',
                            typeAhead: true,
                            triggerAction: 'all',
                            lazyRender:true,
                            mode: 'local',
                            store: {
                                xtype: 'directstore',
                                directFn: router.getRecipientOptions,
                                root: 'data',
                                autoLoad: true,
                                idProperty: 'value',
                                fields: [
                                    'type',
                                    'label',
                                    'value'
                                ]
                            },
                            valueField: 'value',
                            displayField: 'label'
                        },{
                            xtype: 'button',
                            text: 'Add',
                            ref: 'add_button',
                            handler: function(btn, event) {
                                me.addValueFromCombo()
                            }
                        },{
                            xtype: 'button',
                            ref: 'delete_button',
                            iconCls: 'delete',
                            handler: function(btn, event) {
                                var row = btn.refOwner.ownerCt.getSelectionModel().getSelected();
                                btn.refOwner.ownerCt.getStore().remove(row);
                                btn.refOwner.ownerCt.getView().refresh();
                            }
                        }
                    ]
                },
                store: new Ext.data.JsonStore({
                    autoDestroy: true,
                    storeId: 'users_combo_store',
                    autoLoad: false,
                    idProperty: 'value',
                    fields: [
                        'type',
                        'label',
                        'value',
                        {name: 'write', type: 'bool'},
                        {name: 'manage', type: 'bool'}
                    ],
                    data: []
                }),
                colModel: new Ext.grid.ColumnModel({
                    defaults: {
                        width: 120,
                        sortable: true
                    },
                    columns: [
                        {
                            header: _t('Type'),
                            dataIndex: 'type'
                        },{
                            header: config.title,
                            dataIndex: 'label'
                        },
                        writeColumn,
                        manageColumn
                    ]
                }),
                sm: new Ext.grid.RowSelectionModel({singleSelect:true})
            });
            UsersPermissionGrid.superclass.constructor.apply(this, arguments);
        },
        addValueFromCombo: function() {
            var val = this.getTopToolbar().users_combo.getValue(),
                row = this.getTopToolbar().users_combo.getStore().getById(val),
                type = 'manual',
                label;

            if (row) {
                type = row.data.type;
                label = row.data.label;
            }
            else {
                val = this.getTopToolbar().users_combo.getRawValue();
                label = val;
            }


            if (!this.allowManualEntry && type == 'manual') {
                Zenoss.message.error(_t('Manual entry not permitted here.'));
            }
            else {
                var existingIndex = this.getStore().findExact('value', val);

                if (!Ext.isEmpty(val) && existingIndex == -1) {
                    var record = new Ext.data.Record({
                        type:type,
                        value:val,
                        label:label,
                        write:false,
                        manage: false
                    });
                    this.getStore().add(record);
                    this.getView().refresh();
                    this.getTopToolbar().users_combo.clearValue();
                }
                else if (existingIndex != -1) {
                    Zenoss.message.error(_t('Duplicate items not permitted here.'));
                }
            }

        },
        loadData: function(data) {
            this.getStore().loadData(data.users);
        }
    });



    var NotificationTabContent = Ext.extend(Ext.Panel, {
        constructor: function(config) {
            config = config || {};
            Ext.applyIf(config, {
                forceLayout: true,
                autoScroll: true,
                resizable: false,
                height: bigWindowHeight-110,
                maxHeight: bigWindowHeight-110,
                width: bigWindowWidth,
                minWidth: bigWindowWidth
            });
            NotificationTabContent.superclass.constructor.apply(this, arguments);
        }
    });

    var NotificationTabPanel = Ext.extend(Ext.TabPanel, {
        constructor: function(config) {
            config = config || {};
            Ext.applyIf(config, {
                ref: '../tabPanel',
                activeTab: 0,
                activeIndex: 0,
                forceLayout: true,
                frame: true,
                loadData: function(data) {
                    Ext.each(this.items.items, function(item, index, allitems) {
                        item.loadData(data);
                    });
                }
            });
            NotificationTabPanel.superclass.constructor.apply(this, arguments);
        }
    });

    displayNotificationEditDialogue = function(data) {
        var tab_notification, tab_content, tab_subscriptions;
        var _width, _height;

        tab_content = new NotificationTabContent({
            layout: 'form',
            padding: panelPadding,
            title: 'Content',
            defaults: {
                padding: 0
            },
            listeners: {
                render: function() {
                    if (!this.userWrite) {
                        disableTabContents(this);
                    }
                }
            },
            loadData: function(data) {
                this.userWrite = data['userWrite'] || false;
                var panel = {xtype:'panel'};
                panel = Ext.applyIf(panel, data.content);
                var comp = Ext.create(panel);
                this.add(comp)
            }
        });
        
        tab_notification = new NotificationTabContent({
            title: 'Notification',
            ref: 'notification_tab',
            items: [
                {
                    xtype: 'panel',
                    layout: 'column',
                    border: false,
                    padding: panelPadding,
                    defaults: {
                        layout: 'form',
                        padding: 0,
                        columnWidth: 0.5
                    },
                    items: [
                        {
                            xtype: 'fieldset',
                            header: false,
                            items: [
                                {
                                    xtype: 'hidden',
                                    name: 'uid',
                                    ref: '../../uid'
                                },{
                                    xtype: 'checkbox',
                                    name: 'enabled',
                                    ref: '../../enabled',
                                    fieldLabel: _t('Enabled')
                                },{
                                    xtype: 'checkbox',
                                    name: 'send_clear',
                                    ref: '../../send_clear',
                                    fieldLabel: _t('Send Clear')
                                },{
                                    xtype: 'checkbox',
                                    name: 'send_initial_occurrence',
                                    ref: '../../send_initial_occurrence',
                                    fieldLabel: _t('Send only on Initial Occurrence?')
                                }
                            ]
                        },
                        {
                            xtype: 'fieldset',
                            header: false,
                            items: [
                                {
                                    xtype: 'numberfield',
                                    name: 'delay_seconds',
                                    allowNegative: false,
                                    allowBlank: false,
                                    ref: '../../delay_seconds',
                                    fieldLabel: _t('Delay (seconds)')
                                },{
                                    xtype: 'numberfield',
                                    allowNegative: false,
                                    allowBlank: false,
                                    name: 'repeat_seconds',
                                    ref: '../../repeat_seconds',
                                    fieldLabel: _t('Repeat (seconds)')
                                }
                            ]
                        }
                    ]
                },
                {
                    xtype: 'triggersSubscriptions',
                    ref: 'subscriptions'
                }
            ],
            loadData: function(data) {
                this.uid.setValue(data.uid);
                this.enabled.setValue(data.enabled);
                this.delay_seconds.setValue(data.delay_seconds);
                this.send_clear.setValue(data.send_clear);
                this.repeat_seconds.setValue(data.repeat_seconds);
                this.subscriptions.loadData(data.subscriptions);
                this.send_initial_occurrence.setValue(data.send_initial_occurrence);
            }
        });

        if (!data['userWrite']) {
            disableTabContents(tab_notification);
        }

        var recipients_grid = new UsersPermissionGrid({
            title: _t('Subscribers'),
            allowManualEntry: true,
            width: Ext.IsIE ? bigWindowWidth-50 : 'auto',
            ref: 'recipients_grid'
        });

        var tab_recipients = new NotificationTabContent({
            title: 'Subscribers',
            ref: 'recipients_tab',
            items: [{
                xtype: 'panel',
                border: false,
                layout: 'form',
                padding: panelPadding,
                title: _t('Local Notification Permissions'),
                items: [
                    {
                        xtype:'checkbox',
                        name: 'notification_globalRead',
                        ref: '../globalRead',
                        boxLabel: _t('Everyone can view'),
                        hideLabel: true
                    },
                    {
                        xtype:'checkbox',
                        name: 'notification_globalWrite',
                        ref: '../globalWrite',
                        boxLabel: _t('Everyone can edit content'),
                        hideLabel: true
                    },
                    {
                        xtype:'checkbox',
                        name: 'notification_globalManage',
                        ref: '../globalManage',
                        boxLabel: _t('Everyone can manage subscriptions'),
                        hideLabel: true
                    }
                ]
            },
            recipients_grid
            ],
            loadData: function(data) {
                this.recipients_grid.getStore().loadData(data.recipients);
                this.globalRead.setValue(data.globalRead);
                this.globalWrite.setValue(data.globalWrite);
                this.globalManage.setValue(data.globalManage);
            }
        });
        
        if (!data['userManage']) {
            disableTabContents(tab_recipients);
        }

        var tab_panel = new NotificationTabPanel({
            // the following width dance is to make IE, FF and Chrome behave.
            // Each browser was responding differently to the following params.
            width: bigWindowWidth-10,
            minWidth: bigWindowWidth-15,
            maxWidth: bigWindowWidth,
            items: [
                // NOTIFICATION INFO
                tab_notification,

                // CONTENT TAB
                tab_content,

                // RECIPIENTS
                tab_recipients
            ]
        });

        var dialogue = new EditNotificationDialogue({
            id: 'edit_notification_dialogue',
            title: _t('Edit Notification'),
            directFn: router.updateNotification,
            reloadFn: reloadNotificationGrid,
            tabPanel: tab_panel
        });
        
        dialogue.title = String.format("{0} - {1} ({2})", dialogue.title, data['newId'], data['action']);
        dialogue.loadData(data);
        dialogue.show();
    };

    var displayNotificationAddDialogue = function() {
        var typesCombo = new Ext.form.ComboBox({
            store: {
                xtype: 'directstore',
                directFn: router.getNotificationTypes,
                root: 'data',
                autoLoad: true,
                idProperty: 'id',
                fields: [
                    'id', 'name',
                ]
            },
            name:'action',
            ref: 'action_combo',
            allowBlank:false,
            required:true,
            editable:false,
            displayField:'name',
            valueField:'id',
            fieldLabel: _t('Action'),
            mode:'local',
            triggerAction: 'all'
        });
        typesCombo.store.on('load', function(){
            typesCombo.setValue('email');
        });
        var dialogue = new Ext.Window({
            title: 'Add Notification',
            height: 140,
            width: 300,
            modal: true,
            plain: true,
            items: [{
                xtype:'form',
                ref: '../addForm',
                border: false,
                monitorValid: true,
                buttonAlign: 'center',
                items:[
                    {
                        xtype: 'textfield',
                        name: 'newId',
                        ref: '../newId',
                        allowBlank: false,
                        vtype: 'alphanum',
                        fieldLabel: _t('Id')
                    },
                    typesCombo
                ],
                buttons:[
                    {
                        xtype: 'button',
                        ref: 'submitButton',
                        formBind: true,
                        text: _t('Submit'),
                        handler: function(button) {
                            var params = button.refOwner.ownerCt.getForm().getFieldValues();
                            router.addNotification(params, function(){
                                reloadNotificationGrid();
                                button.refOwner.ownerCt.ownerCt.close();
                            });
                        }
                    },{
                        xtype: 'button',
                        ref: 'cancelButton',
                        text: _t('Cancel'),
                        handler: function(button) {
                            button.refOwner.ownerCt.ownerCt.close();
                        }
                    }
                ]
            }]
        });
        dialogue.show();
    };

    EditNotificationDialogue = Ext.extend(Ext.Window, {
        constructor: function(config) {
            config = config || {};
            Ext.applyIf(config, {
                plain: true,
                cls: 'white-background-panel',
                border: false,
                autoScroll: true,
                constrain: true,
                modal: true,
                resizable: false,
                width: bigWindowWidth,
                height: bigWindowHeight,
                minWidth: bigWindowWidth,
                minHeight: bigWindowHeight,
                stateful: false,
                layout: 'fit',
                forceLayout: true,
                items: [{
                    xtype:'form',
                    ref: 'editForm',
                    border: false,
                    buttonAlign: 'center',
                    monitorValid: true,
                    items:[config.tabPanel],
                    buttons:[{
                        xtype: 'button',
                        text: _t('Submit'),
                        ref: '../../submitButton',
                        formBind: true,
                        handler: function(button) {
                            var params = button.refOwner.editForm.getForm().getFieldValues();
                            params.recipients = [];
                            params.subscriptions = [];
                            Ext.each(
                                button.refOwner.tabPanel.notification_tab.subscriptions.getStore().getRange(),
                                function(item, index, allItems) {
                                    params.subscriptions.push(item.data.uuid);
                                }
                            );
                            Ext.each(
                                button.refOwner.tabPanel.recipients_tab.recipients_grid.getStore().getRange(),
                                function(item, index, allItems){
                                    params.recipients.push(item.data);
                                }
                            );
                            config.directFn(params, function(){
                                button.refOwner.close();
                                config.reloadFn();
                            });
                        }
                    },{
                        xtype: 'button',
                        ref: '../../cancelButton',
                        text: _t('Cancel'),
                        handler: function(button) {
                            button.refOwner.close();
                        }
                    }]
                }]
            });
            EditNotificationDialogue.superclass.constructor.apply(this, arguments);
        },
        loadData: function(data) {
            this.tabPanel.loadData(data);
        }
    });
    Ext.reg('editnotificationdialogue', EditNotificationDialogue);


    EditScheduleDialogue = Ext.extend(Ext.Window, {
        constructor: function(config) {
            config = config || {};
            Ext.applyIf(config, {
                modal: true,
                plain: true,
                width: 350,
                height: 250,
                maxWidth: 350,
                maxHeight: 250,
                border: false,
                closeAction: 'hide',
                items:{
                    xtype:'form',
                    ref: 'editForm',
                    border: false,
                    buttonAlign: 'center',
                    monitorValid: true,
                    autoWidth: true,
                    items:[
                        {
                            xtype: 'hidden',
                            name: 'uid',
                            ref: 'uid'
                        },{
                            xtype: 'checkbox',
                            name: 'enabled',
                            ref: 'enabled',
                            fieldLabel: _t('Enabled')
                        },{
                            xtype: 'datefield',
                            name: 'start',
                            ref: 'start',
                            allowBlank: false,
                            fieldLabel: _t('Start Date')
                        }, {
                            xtype: 'timefield',
                            name: 'starttime',
                            ref: 'starttime',
                            allowBlank: false,
                            format: 'H:i',
                            fieldLabel: _t('Start Time')
                        },
                        new Ext.form.ComboBox({
                            store: new Ext.data.ArrayStore({
                                autoDestroy: true,
                                fields:['value'],
                                id: 0,
                                data: [
                                    ['Never'],
                                    ['Daily'],
                                    ['Every Weekday'],
                                    ['Weekly'],
                                    ['Monthly'],
                                    ['First Sunday of the Month']
                                ]
                            }),
                            mode: 'local',
                            name: 'repeat',
                            allowBlank: false,
                            required: true,
                            editable: false,
                            displayField: 'value',
                            valueField: 'value',
                            triggerAction: 'all',
                            fieldLabel: _t('Repeat')
                        }),{
                            xtype: 'numberfield',
                            allowNegative: false,
                            allowDecimals: false,
                            name: 'duration',
                            ref: 'duration',
                            fieldLabel: _t('Duration (minutes)')
                        }
                    ],
                    buttons:[
                        {
                            xtype: 'button',
                            text: _t('Submit'),
                            ref: '../../submitButton',
                            formBind: true,
                            handler: function(button) {
                                var params = button.refOwner.editForm.getForm().getFieldValues();
                                config.directFn(params, function(){
                                    button.refOwner.hide();
                                    config.reloadFn();
                                });
                            }
                        },{
                            xtype: 'button',
                            ref: '../../cancelButton',
                            text: _t('Cancel'),
                            handler: function(button) {
                                button.refOwner.hide();
                            }
                        }]
                    }
            });
            EditScheduleDialogue.superclass.constructor.apply(this, arguments);
        },
        loadData: function(data) {
            Ext.each(this.editForm.items.items, function(item, index, allitems) {
                item.setValue(eval('data.'+item.name));
            });
        }
    });
    Ext.reg('editscheduledialogue', EditScheduleDialogue);

    editScheduleDialogue = new EditScheduleDialogue(editScheduleDialogueConfig);
    addScheduleDialogue = new AddDialogue(addScheduleDialogueConfig);


    NotificationSubscriptions = Ext.extend(Ext.grid.GridPanel, {
        constructor: function(config) {
            config = config || {};
            Ext.applyIf(config, {
                autoScroll: true,
                autoHeight: true,
                minHeight: 400, // force IE to show something
                border: false,
                viewConfig: {
                    forceFit: true
                },
                listeners: {
                    rowdblclick: function(grid, rowIndex, event){
                        var row = grid.getSelectionModel().getSelected();
                        if (row) {
                            displayNotificationEditDialogue(row.data);
                        }
                    }
                },
                selModel: new Ext.grid.RowSelectionModel({
                    singleSelect: true,
                    listeners: {
                        rowselect: function(sm, rowIndex, record) {
                            var row = sm.getSelected(),
                                panel = Ext.getCmp(schedulesPanelConfig.id);
                            panel.setContext(row.data.uid);
                            panel.disableButtons(false);
                            sm.grid.customizeButton.setDisabled(false);
                        },
                        rowdeselect: function(sm, rowIndex, record) {
                            Ext.getCmp(schedulesPanelConfig.id).disableButtons(true);
                            sm.grid.customizeButton.setDisabled(true);
                        }
                    },
                    scope: this
                }),
                store: {
                    xtype: 'directstore',
                    directFn: router.getNotifications,
                    root: 'data',
                    autoLoad: true,
                    fields: [
                        'uid',
                        'newId',
                        'enabled',
                        'action',
                        'delay_seconds',
                        'send_clear',
                        'send_initial_occurrence',
                        'repeat_seconds',
                        'content',
                        'recipients',
                        'subscriptions',
                        'globalRead',
                        'globalWrite',
                        'globalManage',
                        'userRead',
                        'userWrite',
                        'userManage'

                    ]
                },
                colModel: new Ext.grid.ColumnModel({
                    columns: [{
                        xtype: 'booleancolumn',
                        trueText: _t('Yes'),
                        falseText: _t('No'),
                        dataIndex: 'enabled',
                        header: _t('Enabled'),
                        sortable: true
                    },{
                        dataIndex: 'newId',
                        header: _t('Id'),
                        sortable: true
                    },{
                        dataIndex: 'subscriptions',
                        header: _t('Trigger'),
                        sortable: true,
                        // use a fancy renderer that get's it's display value
                        // from the store that already has the triggers.
                        renderer: function(value, metaData, record, rowIndex, colIndex, store) {
                            var triggerList = [];
                            Ext.each(
                                value,
                                function(item, index, allItems) {
                                    if (item) {
                                        triggerList.push(item.name);
                                    }
                                }
                            );
                            return triggerList.join(', ');
                       }
                    },{
                        dataIndex: 'action',
                        header: _t('Action'),
                        sortable: true
                    },{
                        dataIndex: 'recipients',
                        header: _t('Subscribers'),
                        sortable: true,
                        renderer: function(value, metaData, record, rowIndex, colIndex, store) {
                            return record.data.recipients.length || 0;
                        }
                    }]
                }),
                tbar:[{
                    xtype: 'button',
                    iconCls: 'add',
                    ref: '../addButton',
                    handler: function(button) {
                        displayNotificationAddDialogue();
                    }
                },{
                    xtype: 'button',
                    iconCls: 'delete',
                    ref: '../deleteButton',
                    handler: function(button) {
                        var row = button.refOwner.getSelectionModel().getSelected(),
                            uid,
                            params,
                            callback;
                        if (row){
                            uid = row.data.uid;
                            // show a confirmation
                            Ext.Msg.show({
                                title: _t('Delete Notification Subscription'),
                                msg: String.format(_t("Are you sure you wish to delete the notification, {0}?"), row.data.newId),
                                buttons: Ext.Msg.OKCANCEL,
                                fn: function(btn) {
                                    if (btn == "ok") {
                                        params = {
                                            uid:uid
                                        };
                                        callback = function(response){
                                            var panel = Ext.getCmp(schedulesPanelConfig.id);
                                            panel.getStore().removeAll();
                                            panel.disableButtons(true);
                                            Ext.getCmp(notificationPanelConfig.id).customizeButton.setDisabled(true);
                                            reloadNotificationGrid();
                                        };
                                        router.removeNotification(params, callback);

                                    } else {
                                        Ext.Msg.hide();
                                    }
                                }
                            });
                        }
                    }
                },{
                    xtype: 'button',
                    iconCls: 'customize',
                    disabled:true,
                    ref: '../customizeButton',
                    handler: function(button){
                        var row = button.refOwner.getSelectionModel().getSelected();
                        if (row) {
                            displayNotificationEditDialogue(row.data);
                        }
                    }
                }]
            });
            NotificationSubscriptions.superclass.constructor.apply(this, arguments);
        },
        setContext: function(uid) {
            this.uid = uid;
            this.getStore().load({
                params: {
                    uid: uid
                }
            });
        }
    });
    Ext.reg('notificationsubscriptions', NotificationSubscriptions);

    notification_panel = Ext.create(notificationPanelConfig);


    SchedulesPanel = Ext.extend(Ext.grid.GridPanel, {
        constructor: function(config) {
            config = config || {};
            Ext.applyIf(config, {
                autoScroll: true,
                border: false,
                autoHeight: true,
                viewConfig: {
                    forceFit: true
                },
                listeners: {
                    rowdblclick: function(grid, rowIndex, event){
                        var row = grid.getSelectionModel().getSelected();
                        if (row) {
                            displayScheduleEditDialogue(row.data);
                        }
                    }
                },
                selModel: new Ext.grid.RowSelectionModel({
                    singleSelect: true,
                    listeners: {
                        rowselect: function(sm, rowIndex, record) {
                            var row = sm.getSelected();
                            sm.grid.customizeButton.setDisabled(false);
                        },
                        rowdeselect: function(sm, rowIndex, record) {
                            sm.grid.customizeButton.setDisabled(true);
                        }
                    },
                    scope: this
                }),
                store: {
                    xtype: 'directstore',
                    directFn: router.getWindows,
                    root: 'data',
                    fields: [
                        'uid',
                        'newId',
                        'enabled',
                        'start',
                        'starttime',
                        'repeat',
                        'duration'
                    ]
                },
                colModel: new Ext.grid.ColumnModel({
                    columns: [{
                        xtype: 'booleancolumn',
                        trueText: _t('Yes'),
                        falseText: _t('No'),
                        dataIndex: 'enabled',
                        header: _t('Enabled'),
                        sortable: true
                    },{
                        dataIndex: 'newId',
                        header: _t('Id'),
                        width:200,
                        sortable: true
                    },{
                        dataIndex: 'start',
                        header: _t('Start'),
                        width:200,
                        sortable: true
                    }]
                }),
                tbar:[{
                    xtype: 'button',
                    iconCls: 'add',
                    ref: '../addButton',
                    disabled: true,
                    handler: function(button) {
                        addScheduleDialogue.show();
                    }
                },{
                    xtype: 'button',
                    iconCls: 'delete',
                    ref: '../deleteButton',
                    disabled: true,
                    handler: function(button) {
                        var row = button.refOwner.getSelectionModel().getSelected(),
                            uid,
                            params;
                        if (row){
                            uid = row.data.uid;
                            // show a confirmation
                            Ext.Msg.show({
                                title: _t('Delete Schedule'),
                                msg: String.format(_t("Are you sure you wish to delete the schedule, {0}?"), row.data.newId),
                                buttons: Ext.Msg.OKCANCEL,
                                fn: function(btn) {
                                    if (btn == "ok") {
                                        params = {
                                            uid:uid
                                        };
                                        router.removeWindow(params, reloadScheduleGrid);
                                    } else {
                                        Ext.Msg.hide();
                                    }
                                }
                            });
                        }
                    }
                },{
                    xtype: 'button',
                    iconCls: 'customize',
                    disabled:true,
                    ref: '../customizeButton',
                    handler: function(button){
                        var row = button.refOwner.getSelectionModel().getSelected();
                        if (row) {
                            displayScheduleEditDialogue(row.data);
                        }
                    }
                }]

            });
            SchedulesPanel.superclass.constructor.apply(this, arguments);
        },
        setContext: function(uid){
            this.uid = uid;
            this.getStore().load({
                params: {
                    uid: uid
                }
            });
            this.disableButtons(false);
            this.customizeButton.setDisabled(true);
        },
        disableButtons: function(bool){
            this.addButton.setDisabled(bool);
            this.deleteButton.setDisabled(bool);
        }
    });
    Ext.reg('schedulespanel', SchedulesPanel);

    schedules_panel = Ext.create(schedulesPanelConfig);


    NotificationPageLayout = Ext.extend(Ext.Panel, {
        constructor: function(config) {
            config = config || {};
            Ext.applyIf(config, {
                id: 'notification_subscription_panel',
                layout:'border',
                defaults: {
                    collapsible: false,
                    split: true,
                    border: false
                },
                items: [{
                    title: _t('Notification Schedules'),
                    region:'east',
                    width: 375,
                    minSize: 100,
                    maxSize: 375,
                    items: [config.schedulePanel]
                },{
                    title: _t('Notifications'),
                    region:'center',
                    items:[config.notificationPanel]
                }]

            });
            NotificationPageLayout.superclass.constructor.apply(this, arguments);
        }
    });
    Ext.reg('notificationsubscriptions', NotificationPageLayout);


    notificationsPanelConfig = {
        id: 'notifications_panel',
        xtype: 'notificationsubscriptions',
        schedulePanel: schedules_panel,
        notificationPanel: notification_panel
    };




    /***
     * TRIGGERS
     **/

    colModelConfig = {
        defaults: {
            menuDisabled: true
        },
        columns: [
            {
                id: 'enabled',
                dataIndex: 'enabled',
                header: _t('Enabled'),
                xtype: 'booleancolumn',
                trueText: _t('Yes'),
                falseText: _t('No'),
                width: 70,
                sortable: true
            }, {
                id: 'name',
                dataIndex: 'name',
                header: _t('Name'),
                width: 200,
                sortable: true
            }
        ]
    };

    triggersPanelConfig = {
        id: 'triggers_grid_panel',
        xtype: 'TriggersGridPanel'
    };

    detailPanelConfig = {
        id: 'triggers_detail_panel',
        xtype: 'contextcardpanel',
        split: true,
        region: 'center',
        layout: 'card',
        activeItem: 0,
        items: [triggersPanelConfig, notificationsPanelConfig]
    };

    navSelectionModel = new Ext.tree.DefaultSelectionModel({
        listeners: {
            selectionchange: function (sm, newnode) {
                var p = Ext.getCmp(detailPanelConfig.id);
                p.layout.setActiveItem(newnode.attributes.target);
                p.setContext(newnode.attributes.target);
            }
        }
    });

    masterPanelConfig = {
        id: 'master_panel',
        region: 'west',
        split: 'true',
        width: 275,
        autoScroll: false,
        items: [
            {
                id: 'master_panel_navigation',
                xtype: 'treepanel',
                region: 'west',
                split: 'true',
                width: 275,
                autoScroll: true,
                border: false,
                rootVisible: false,
                selModel: navSelectionModel,
                layout: 'fit',
                bodyStyle: { 'margin-top' : 10 },
                root: {
                    text: 'Trigger Navigation',
                    draggable: false,
                    id: 'trigger_root',
                    expanded: true,
                    children: [
                        {
                            target: triggersPanelConfig.id,
                            text: 'Triggers',
                            leaf: true,
                            iconCls: 'no-icon'
                        }, {
                            target: notificationsPanelConfig.id,
                            text: 'Notifications',
                            leaf: true,
                            iconCls: 'no-icon'
                        }
                    ]
                }
            }
        ]
    };

    var trigger_tab_content = {
        xtype:'panel',
        ref: '../../tab_content',
        height: bigWindowHeight-110,
        autoScroll: true,
        layout: 'form',
        title: 'Trigger',
        padding: 10,
        labelWidth: 75,
        items:[
            {
                xtype: 'hidden',
                name: 'uuid',
                ref: 'uuid'
            },{
                xtype: 'textfield',
                name: 'name',
                ref: 'name',
                allowBlank: false,
                fieldLabel: _t('Name')
            },{
                xtype: 'checkbox',
                name: 'enabled',
                ref: 'enabled',
                fieldLabel: _t('Enabled')
            },{
                xtype: 'rulebuilder',
                fieldLabel: _t('Rule'),
                name: 'criteria',
                ref: 'rule',
                subjects: [
                Ext.applyIf(
                    {
                    text: _t('Device Priority'),
                    value: 'dev.priority'
                    },
                    ZFR.DEVICEPRIORITY
                ),
                Ext.applyIf({
                    text: _t('Device Production State'),
                    value: 'dev.production_state'
                    },
                    ZFR.PRODUCTIONSTATE
                ),{
                    text: _t('Device (Element)'),
                    value: 'elem.name',
                    comparisons: STRINGCMPS
                },{
                    text: _t('Component (Sub-Element)'),
                    value: 'sub_elem.name',
                    comparisons: STRINGCMPS
                },{
                    text: _t('Element Type'),
                    value: 'elem.type',
                    comparisons: ZFR.IDENTITYCOMPARISONS,
                    field: {
                        xtype: 'combo',
                        mode: 'local',
                        valueField: 'name',
                        displayField: 'name',
                        typeAhead: false,
                        forceSelection: true,
                        triggerAction: 'all',
                        store: new Ext.data.ArrayStore({
                            fields: ['name'],
                            data: [[
                                'COMPONENT'
                            ],[
                                'DEVICE'
                            ],[
                                'SERVICE'
                            ],[
                                'ORGANIZER'
                            ]]
                        })
                    }
                },{
                    text: _t('Sub Element Type'),
                    value: 'sub_elem.type',
                    comparisons: ZFR.IDENTITYCOMPARISONS,
                    field: {
                        xtype: 'combo',
                        mode: 'local',
                        valueField: 'name',
                        displayField: 'name',
                        typeAhead: false,
                        forceSelection: true,
                        triggerAction: 'all',
                        store: new Ext.data.ArrayStore({
                            fields: ['name'],
                            data: [[
                                'COMPONENT'
                            ],[
                                'DEVICE'
                            ],[
                                'SERVICE'
                            ],[
                                'ORGANIZER'
                            ]]
                        })
                    }
                }, {
                    text: _t('Event Class'),
                    value: 'evt.event_class',
                    comparisons: STRINGCMPS,
                    field: {
                        xtype: 'eventclass'
                    }
                },{
                    text: _t('Event Key'),
                    value: 'evt.event_key',
                    comparisons: STRINGCMPS
                },{
                    text: _t('Summary'),
                    value: 'evt.summary',
                    comparisons: STRINGCMPS
                },{
                    text: _t('Message'),
                    value: 'evt.message',
                    comparisons: STRINGCMPS
                },{
                    text: _t('Severity'),
                    value: 'evt.severity',
                    comparisons: NUMCMPS,
                    field: {
                        xtype: 'combo',
                        mode: 'local',
                        valueField: 'value',
                        displayField: 'name',
                        typeAhead: false,
                        forceSelection: true,
                        triggerAction: 'all',
                        store: new Ext.data.ArrayStore({
                            fields: ['name', 'value'],
                            data: [[
                                _t('Critical'), 5
                            ],[
                                _t('Error'), 4
                            ],[
                                _t('Warning'), 3
                            ],[
                                _t('Info'), 2
                            ],[
                                _t('Debug'), 1
                            ],[
                                _t('Clear'), 0
                            ]]
                        })
                    }
                },{
                    text: _t('Fingerprint'),
                    value: 'evt.fingerprint',
                    comparisons: STRINGCMPS
                },{
                    text: _t('Agent'),
                    value: 'evt.agent',
                    comparisons: STRINGCMPS
                },{
                    text: _t('Monitor'),
                    value: 'evt.monitor',
                    comparisons: STRINGCMPS
                },{
                    text: _t('Count'),
                    value: 'evt.count',
                    comparisons: NUMCMPS,
                    field: {
                        xtype: 'numberfield'
                    }
                },{
                    text: _t('Status'),
                    value: 'evt.status',
                    comparisons: NUMCMPS,
                    field: {
                        xtype: 'combo',
                        mode: 'local',
                        valueField: 'value',
                        displayField: 'name',
                        typeAhead: false,
                        forceSelection: true,
                        triggerAction: 'all',
                        store: new Ext.data.ArrayStore({
                            fields: ['name', 'value'],
                            data: [[
                                _t('New'), 1
                            ],[
                                _t('Acknowledged'), 2
                            ],[
                                _t('Suppressed'), 3
                            ]]
                        })
                    }
                },{
                    text: _t('Event Class Key'),
                    value: 'evt.event_class_key',
                    comparisons: STRINGCMPS
                },{
                    text: _t('Syslog Priority'),
                    value: 'evt.syslog_priority',
                    comparisons: NUMCMPS,
                    field: {
                        xtype: 'combo',
                        mode: 'local',
                        valueField: 'value',
                        displayField: 'name',
                        typeAhead: false,
                        forceSelection: true,
                        triggerAction: 'all',
                        store: new Ext.data.ArrayStore({
                            fields: ['name', 'value'],
                            data: [[
                                _t('Emergency'), 0
                            ],[
                                _t('Alert'), 1
                            ],[
                                _t('Critical'), 2
                            ],[
                                _t('Error'), 3
                            ],[
                                _t('Warning'), 4
                            ],[
                                _t('Notice'), 5
                            ],[
                                _t('Info'), 6
                            ],[
                                _t('Debug'), 7
                            ]]
                        })
                    }
                },{
                    text: _t('Location'),
                    value: 'dev.location',
                    comparisons: STRINGCMPS
                },
                ZFR.DEVICECLASS,
                {
                    text: _t('Syslog Facility'),
                    value: 'evt.syslog_facility',
                    comparisons: NUMCMPS,
                    field: {
                        xtype: 'numberfield'
                    }
                },{
                    text: _t('NT Event Code'),
                    value: 'evt.nt_event_code',
                    comparisons: NUMCMPS,
                    field: {
                        xtype: 'numberfield'
                    }
                },{
                    text: _t('IP Address'),
                    value: 'dev.ip_address',
                    comparisons: STRINGCMPS
                },
                ZFR.SYSTEMS,
                ZFR.DEVICEGROUPS
                ]
            }
        ]
    };
    
    var users_grid = new UsersPermissionGrid({
        title: _t('Users'),
        allowManualEntry: false
    });

    var trigger_tab_users = {
        xtype: 'panel',
        ref: '../../tab_users',
        title: 'Users',
        autoScroll: true,
        height: bigWindowHeight-110,
        items: [
            {
                xtype: 'panel',
                border: false,
                layout: 'form',
                title: _t('Local Trigger Permissions'),
                padding: 10,
                items: [
                    {
                        xtype:'checkbox',
                        name: 'trigger_globalRead',
                        ref: '../globalRead',
                        boxLabel: _t('Everyone can view'),
                        hideLabel: true
                    },
                    {
                        xtype:'checkbox',
                        name: 'trigger_globalWrite',
                        ref: '../globalWrite',
                        boxLabel: _t('Everyone can edit content'),
                        hideLabel: true
                    },
                    {
                        xtype:'checkbox',
                        name: 'trigger_globalManage',
                        ref: '../globalManage',
                        boxLabel: _t('Everyone can manage users'),
                        hideLabel: true
                    }

                ]
            },
            users_grid
        ]
    };


    EditTriggerDialogue = Ext.extend(Ext.Window, {
        constructor: function(config) {
            config = config || {};
            Ext.applyIf(config, {
                plain: true,
                cls: 'white-background-panel',
                autoScroll: false,
                constrain: true,
                resizable: false,
                modal: true,
                height: bigWindowHeight,
                width: bigWindowWidth+235,
                boxMaxWidth: bigWindowWidth+235, // for chrome, safari
                border: false,
                closeAction: 'hide',
                layout: 'fit',
                items: [
                    {
                        xtype:'form',
                        ref: 'wrapping_form',
                        border: false,
                        buttonAlign: 'center',
                        monitorValid: true,
                        items: [
                            {
                                xtype: 'tabpanel',
                                ref: '../tabs',
                                activeTab: 0,
                                activeIndex: 0,
                                defaults: {
                                    height: bigWindowHeight,
                                    width: bigWindowWidth+225,
                                    autoScroll: true,
                                    frame: false,
                                    border: false
                                },
                                items: [
                                    trigger_tab_content,
                                    trigger_tab_users
                                ]
                            }
                        ],
                        buttons:[
                            {
                                xtype: 'button',
                                text: _t('Submit'),
                                ref: '../../submitButton',
                                formBind: true,
                                handler: function(button) {
                                    var tab_content = button.refOwner.tab_content,
                                        tab_users = button.refOwner.tab_users;

                                    var params = {
                                        uuid: tab_content.uuid.getValue(),
                                        enabled: tab_content.enabled.getValue(),
                                        name: tab_content.name.getValue(),
                                        rule: {
                                            source: tab_content.rule.getValue()
                                        },

                                        // tab_users
                                        globalRead: tab_users.globalRead.getValue(),
                                        globalWrite: tab_users.globalWrite.getValue(),
                                        globalManage: tab_users.globalManage.getValue(),

                                        users: []
                                    };

                                    Ext.each(
                                        tab_users.users_grid.getStore().getRange(),
                                        function(item, index, allItems){
                                            params.users.push(item.data);
                                        }
                                    );

                                    config.directFn(params, function(){
                                        reloadTriggersGrid();
                                        button.refOwner.hide();
                                    });
                                }
                            },{
                                xtype: 'button',
                                ref: '../../cancelButton',
                                text: _t('Cancel'),
                                handler: function(button) {
                                    button.refOwner.hide();
                                }
                            }
                        ]
                    }
                ]
            });
            EditTriggerDialogue.superclass.constructor.apply(this, arguments);
        },
        loadData: function(data) {
            // set content stuff.
            this.tab_content.uuid.setValue(data.uuid);
            this.tab_content.enabled.setValue(data.enabled);
            this.tab_content.name.setValue(data.name);
            this.tab_content.rule.setValue(data.rule.source);

            // set users information (permissions and such)
            this.tab_users.globalRead.setValue(data.globalRead);
            this.tab_users.globalWrite.setValue(data.globalWrite);
            this.tab_users.globalManage.setValue(data.globalManage);
            
            this.tab_users.users_grid.getStore().loadData(data.users);
            
        }
    });
    Ext.reg('edittriggerdialogue', EditTriggerDialogue);


    reloadTriggersGrid = function() {
        Ext.getCmp(triggersPanelConfig.id).getStore().reload();
    };

    displayEditTriggerDialogue = function(data) {

        editTriggerDialogue = new EditTriggerDialogue({
            title: String.format("{0} - {1}", _t('Edit Trigger'), data['name']),
            directFn: router.updateTrigger,
            reloadFn: reloadTriggersGrid,
            validateFn: router.parseFilter
        });

        editTriggerDialogue.loadData(data);
        
        if (!data['userWrite']) {
            disableTabContents(editTriggerDialogue.tab_content);
        } else {
            enableTabContents(editTriggerDialogue.tab_content);
        }

        if (!data['userManage']) {
            disableTabContents(editTriggerDialogue.tab_users);
        } else {
            enableTabContents(editTriggerDialogue.tab_users);
        }
        
        editTriggerDialogue.show();
    };

    addTriggerDialogue = new AddDialogue({
        title: _t('Add Trigger'),
        directFn: router.addTrigger,
        reloadFn: reloadTriggersGrid
    });


    colModel = new Ext.grid.ColumnModel(colModelConfig);

    TriggersGridPanel = Ext.extend(Ext.grid.GridPanel, {
        constructor: function(config) {
            Ext.applyIf(config, {
                autoExpandColumn: 'name',
                stripeRows: true,
                cm: colModel,
                title: _t('Triggers'),
                store: {
                    xtype: 'directstore',
                    directFn: router.getTriggers,
                    root: 'data',
                    autoLoad: true,
                    fields: ['uuid', 'enabled', 'name', 'rule', 'users',
                        'globalRead', 'globalWrite', 'globalManage',
                        'userRead', 'userWrite', 'userManage']
                },
                sm: new Ext.grid.RowSelectionModel({
                    singleSelect: true,
                    listeners: {
                        rowselect: function(sm, rowIndex, record) {
                            // enable/disabled the edit button
                            sm.grid.deleteButton.setDisabled(false);
                            sm.grid.customizeButton.setDisabled(false);
                        },
                        rowdeselect: function(sm, rowIndex, record) {
                            sm.grid.deleteButton.setDisabled(true);
                            sm.grid.customizeButton.setDisabled(true);
                        }
                    },
                    scope: this
                }),
                listeners: {
                    rowdblclick: function(grid, rowIndex, event) {
                        var row = grid.getSelectionModel().getSelected();
                        if (row) {
                            displayEditTriggerDialogue(row.data);
                        }
                    }
                },
                tbar:[
                    {
                        xtype: 'button',
                        iconCls: 'add',
                        ref: '../addButton',
                        handler: function(button) {
                            addTriggerDialogue.show();
                        }
                    },{
                        xtype: 'button',
                        iconCls: 'delete',
                        ref: '../deleteButton',
                        handler: function(button) {
                            var row = button.refOwner.getSelectionModel().getSelected(),
                                uuid, params, callback;
                            if (row){
                                uuid = row.data.uuid;
                                // show a confirmation
                                Ext.Msg.show({
                                    title: _t('Delete Trigger'),
                                    msg: String.format(_t("Are you sure you wish to delete the trigger, {0}?"), row.data.name),
                                    buttons: Ext.Msg.OKCANCEL,
                                    fn: function(btn) {
                                        if (btn == "ok") {
                                            params= {
                                                uuid:uuid
                                            };
                                            callback = function(response){
                                                // item removed, reload grid.
                                                button.refOwner.deleteButton.setDisabled(true);
                                                button.refOwner.customizeButton.setDisabled(true);
                                                reloadTriggersGrid();
                                                reloadNotificationGrid();
                                            };
                                            router.removeTrigger(params, callback);

                                        } else {
                                            Ext.Msg.hide();
                                        }
                                    }
                                });
                            }
                        }
                    },{
                        xtype: 'button',
                        iconCls: 'customize',
                        disabled:true,
                        ref: '../customizeButton',
                        handler: function(button){
                            var row = button.refOwner.getSelectionModel().getSelected();
                            if (row) {
                                displayEditTriggerDialogue(row.data);
                            }
                        }
                    }
                ]
            });
            TriggersGridPanel.superclass.constructor.call(this, config);
        },
        setContext: function(uid) {
            // triggers are not context aware.
            this.getStore().load();
        }
    });
    Ext.reg('TriggersGridPanel', TriggersGridPanel);

    Ext.getCmp('center_panel').add({
        id: 'center_panel_container',
        layout: 'border',
        defaults: {
            'border':false
        },
        items: [
            masterPanelConfig,  // navigation
            detailPanelConfig   // content panel
        ]
    });

});
