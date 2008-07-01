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
 * Created on Jun 25, 2008
 */

package com.bigdata.relation;

import java.io.Serializable;
import java.util.concurrent.ExecutorService;

import com.bigdata.journal.Journal;
import com.bigdata.service.LocalDataServiceFederation;

/**
 * Knows how to locate an {@link IRelation} on a local {@link Journal} using
 * indices WITHOUT concurrency control.
 * <p>
 * Note: If you want to use concurrency control and an embedded database then
 * you should probably be using a {@link LocalDataServiceFederation} and the
 * {@link FederationRelationLocator}.
 * <p>
 * Note: This class is NOT {@link Serializable}. You CAN NOT access a local
 * store from a remote location.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class LocalRelationLocator<R> extends AbstractCachingRelationLocator<R> {

    private final ExecutorService service;

    private final Journal journal;

    public LocalRelationLocator(ExecutorService service, Journal journal,
            IRelationFactory<R> relationFactory) {

        super(relationFactory);

        if (service == null)
            throw new IllegalArgumentException();

        if (journal == null)
            throw new IllegalArgumentException();

        this.service = service;
        
        this.journal = journal;
        
    }

    synchronized public IRelation<R> getRelation(IRelationName<R> relationName,
            long timestamp) {

        if (relationName == null)
            throw new IllegalArgumentException();
        
        IRelation<R> relation = get(relationName, timestamp);

        if (relation == null) {

            final String namespace = relationName.toString();
            
            relation = getRelationFactory().newRelation(service, journal, namespace, timestamp);

            put(relation);
            
        }

        return relation;

    }

}
