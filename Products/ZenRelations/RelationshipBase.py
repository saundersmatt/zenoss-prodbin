###########################################################################
#
# This program is part of Zenoss Core, an open source monitoring platform.
# Copyright (C) 2007, Zenoss Inc.
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License version 2 as published by
# the Free Software Foundation.
#
# For complete information please visit: http://www.zenoss.com/oss/
#
###########################################################################

__doc__="""RelationshipBase

RelationshipBase is the base class for RelationshipManager
and ToManyRelationship.

$Id: RelationshipBase.py,v 1.26 2003/10/03 16:16:01 edahl Exp $"""

__version__ = "$Revision: 1.26 $"[11:-2]

import logging
log = logging.getLogger("zen.Relations")
  
from Globals import InitializeClass
from Acquisition import aq_base
from zope import interface

from Products.ZenRelations.Exceptions import *
from Products.ZenRelations.utils import importClass

from PrimaryPathObjectManager import PrimaryPathManager

from zope.event import notify
from OFS.event import ObjectWillBeAddedEvent
from OFS.event import ObjectWillBeRemovedEvent
from zope.app.container.contained import dispatchToSublocations
from zope.app.container.contained import ObjectAddedEvent
from zope.app.container.contained import ObjectRemovedEvent
from zope.app.container.contained import ContainerModifiedEvent

class IRelationship(interface.Interface):
    """
    Marker interface.
    """

class RelationshipBase(PrimaryPathManager):
    """
    Abstract base class for all relationship classes.
    """
    interface.implements(IRelationship)

    _operation = -1 # if a Relationship's are only deleted

    def __call__(self):
        """Return the contents of this relation."""
        raise NotImplementedError

    
    def getId(self):
        return self.id


    def hasobject(self, obj):
        """Does this relationship relate to obj."""
        raise NotImplementedError


    def _add(self, obj):
        """Add object to local side of relationship."""
        raise NotImplementedError 


    def _remove(self,obj=None):
        """
        Remove object from local side of relationship. 
        If obj=None remove all object in the relationship
        """
        raise NotImplementedError 


    def _remoteRemove(self, obj=None):
        """Remove obj form the remote side of this relationship."""
        raise NotImplementedError 


    def addRelation(self, obj):
        """Form a bi-directional relation between self and obj."""
        if obj is None: raise ZenRelationsError("Can not add None to relation")
        if not isinstance(obj, self.remoteClass()):
            raise ZenSchemaError("%s restricted to class %s. %s is class %s" %
                (self.id, self.remoteClass().__name__, 
                 obj.id, obj.__class__.__name__))
        try:
            self._add(obj)
            rname = self.remoteName()
            # make sure remote rel is on this obj
            getattr(aq_base(obj), rname) 
            remoteRel = getattr(obj, self.remoteName())
            remoteRel._add(self.__primary_parent__)
        except RelationshipExistsError:
            log.debug("obj %s already exists on %s", obj.getPrimaryId(),
                        self.getPrimaryId())


    def removeRelation(self, obj=None):
        """remove an object from a relationship"""
        self._remoteRemove(obj)
        self._remove(obj)
   

    def remoteType(self):
        """Return the type of the remote end of our relationship."""
        schema = self.__primary_parent__.lookupSchema(self.id)
        return schema.remoteType


    def remoteTypeName(self):
        """Return the type of the remote end of our relationship."""
        schema = self.__primary_parent__.lookupSchema(self.id)
        return schema.remoteType.__name__


    def remoteClass(self):
        """Return the class at the remote end of our relationship."""
        classdef = getattr(aq_base(self), "_v_remoteClass", None)
        if not classdef:
            schema = self.__primary_parent__.lookupSchema(self.id)
            classdef = importClass(schema.remoteClass)
            self._v_remoteClass = classdef
        return classdef


    def remoteName(self):
        """Return the name at the remote end of our relationship."""
        schema = self.__primary_parent__.lookupSchema(self.id)
        return schema.remoteName


    def getPrimaryParent(self):
        """Return our parent object by our primary path"""
        return self.__primary_parent__.primaryAq()


    def getRelationshipManagerClass(self):
        """
        Return the local class of this relationship. For all relationshps
        this is the class of our __primary_parent___.
        """
        return self.__primary_parent__.__class__


    def cb_isCopyable(self):
        """Don't let relationships move off their managers"""        
        return 0
        
    
    def cb_isMoveable(self):
        """Don't let relationships move off their managers"""        
        return 0
   

    def checkRelation(self, repair=False):
        """Check to make sure that relationship bidirectionality is ok.
        """
        return

        
InitializeClass(RelationshipBase)
