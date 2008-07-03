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

import com.bigdata.journal.Journal;

/**
 * Knows how to locate an {@link IRelation} on a local {@link Journal} using
 * indices WITHOUT concurrency control.
 * <p>
 * Note: If you want to use concurrency control and an embedded database then
 * you should probably be using the {@link DefaultRelationLocator}.
 * <p>
 * Note: This class is NOT {@link Serializable}. You CAN NOT access a local
 * store from a remote location.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class JournalRelationLocator<R> extends DefaultRelationLocator<R> {

    private final Class<? extends IRelation<R>> cls;

    private final Properties properties;

    public JournalRelationLocator(ExecutorService service, Journal journal,
            Class<? extends IRelation<R>> cls, Properties properties) {

        super(service, journal);

        if (cls == null)
            throw new IllegalArgumentException();

        if (properties == null)
            throw new IllegalArgumentException();

        this.cls = cls;

        this.properties = properties;
        
    }

    synchronized public IRelation<R> getRelation(IRelationName<R> relationName,
            long timestamp) {

        if (relationName == null)
            throw new IllegalArgumentException();

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
