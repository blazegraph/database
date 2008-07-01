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

import com.bigdata.journal.ITx;
import com.bigdata.journal.TemporaryStore;

/**
 * Knows how to locate an {@link IRelation} on a local {@link TemporaryStore}.
 * <p>
 * Note: This class is NOT {@link Serializable}. You CAN NOT access a local
 * {@link TemporaryStore} from a remote location.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TemporaryRelationLocator<R> extends AbstractCachingRelationLocator<R> {

    private final ExecutorService service;
    private final TemporaryStore tempStore;
    
    public TemporaryRelationLocator(ExecutorService service,
            TemporaryStore tempStore, IRelationFactory<R> relationFactory) {

        super(relationFactory);
        
        if (service == null)
            throw new IllegalArgumentException();

        if (tempStore == null)
            throw new IllegalArgumentException();
        
        this.service = service;
        
        this.tempStore = tempStore;
        
    }

    /**
     * 
     * @param relationName
     * @param timestampIsIgnored
     *            The {@link TemporaryStore} does not support timestamps for
     *            index views. All indices views are "unisolated" and the
     *            timestamp parameter is ignored.
     * @return
     */
    synchronized public IRelation<R> getRelation(IRelationName<R> relationName,
            long timestampIsIgnored) {

        if (relationName == null)
            throw new IllegalArgumentException();
        
        final long timestamp = ITx.UNISOLATED;
        
        IRelation<R> relation = get(relationName, timestamp);

        if (relation == null) {

            final String namespace = relationName.toString();
            
            relation = getRelationFactory().newRelation(service, tempStore, namespace);

            put(relation);
            
        }

        return relation;

    }

}
