/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

*/
/*
 * Created on Jun 30, 2008
 */

package com.bigdata.relation;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A mapping between {@link IRelationName}s and {@link IRelationLocator}s.
 * This can be used to locate local, temporary or virtual relations.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RelationLocatorMap<R> implements IRelationLocator<R> {

    /**
     * 
     */
    private final Map<IRelationName<R>, IRelationLocator<R>> relationLocators = new ConcurrentHashMap<IRelationName<R>, IRelationLocator<R>>();

    /**
     * De-serialization ctor.
     */
    public RelationLocatorMap() {
        
    }
        
    /**
     * Add a mapping from an {@link IRelationName} to the
     * {@link IRelationLocator} for the identified {@link IRelation}.
     * 
     * @param relationName
     *            The relation name.
     * @param relationLocator
     *            The locator.
     */
    public void addRelation(IRelationName<R> relationName, IRelationLocator<R> relationLocator) {
        
        if (relationName == null)
            throw new IllegalArgumentException();

        if (relationLocator == null)
            throw new IllegalArgumentException();
        
        relationLocators.put(relationName, relationLocator);
        
    }

    public IRelation<R> getRelation(IRelationName<R> relationName, long timestamp) {

        if (relationName == null)
            throw new IllegalArgumentException();
        
        IRelationLocator<R> relationLocator = relationLocators.get(relationName);
        
        if (relationLocator == null) {

//            relationLocator = defaultRelationLocator;
            throw new IllegalArgumentException("Unknown relation: "+relationName);

        }

        return relationLocator.getRelation(relationName, timestamp);

    }

}
