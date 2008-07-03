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
import java.util.Properties;
import java.util.concurrent.ExecutorService;

import com.bigdata.journal.ITx;
import com.bigdata.journal.TemporaryStore;

/**
 * Knows how to locate a specific {@link IRelation} on a local {@link TemporaryStore}.
 * <p>
 * Note: This class is NOT {@link Serializable}. You CAN NOT access a local
 * {@link TemporaryStore} from a remote location.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TemporaryStoreRelationLocator<R> extends DefaultRelationLocator<R> {

    private final Class<? extends IRelation<R>> cls;
    private final Properties properties;
    
    public TemporaryStoreRelationLocator(ExecutorService service,
            TemporaryStore tempStore, Class<? extends IRelation<R>> cls,
            Properties properties) {

        super(service, tempStore);

        if (cls == null)
            throw new IllegalArgumentException();

        if (properties == null)
            throw new IllegalArgumentException();

        this.cls = cls;
        
        this.properties = properties;
        
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
        
        final String namespace = relationName.toString();
        
        IRelation<R> relation = get(namespace, timestamp);

        if (relation == null) {
            
            relation = newInstance(cls, service, indexManager, namespace,
                    timestamp, properties);

            put(relation);
            
        }

        return relation;

    }

}
