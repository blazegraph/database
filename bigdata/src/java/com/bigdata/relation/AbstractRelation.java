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

import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;

import com.bigdata.btree.IIndex;
import com.bigdata.journal.IIndexManager;
import com.bigdata.relation.accesspath.IKeyOrder;
import com.bigdata.relation.rdf.SPORelationName;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractRelation<R> implements IMutableRelation<R> {

    protected static Logger log = Logger.getLogger(AbstractRelation.class);
    
    private final IIndexManager indexManager;
    
    private final ExecutorService service;
    
    private final String namespace;

    private final long timestamp;
    
    private final SPORelationName relationName;

    public IIndexManager getIndexManager() {
        
        return indexManager;
        
    }
    
    public ExecutorService getExecutorService() {
        
        return service;
        
    }
    
    public String getNamespace() {
        
        return namespace;
        
    }
    
    public long getTimestamp() {
        
        return timestamp;
        
    }

    public SPORelationName getRelationName() {
        
        return relationName;
        
    }
    
    /**
     * 
     */
    protected AbstractRelation(ExecutorService service,
            IIndexManager indexManager, String namespace, long timestamp) {

        if (service == null)
            throw new IllegalArgumentException();

        if (indexManager == null)
            throw new IllegalArgumentException();

        if (namespace == null)
            throw new IllegalArgumentException();

        this.service = service;
        
        this.indexManager = indexManager;
        
        this.namespace = namespace;
        
        this.timestamp = timestamp;

        this.relationName = new SPORelationName(namespace);

    }

    /**
     * The fully qualified name of the index.
     * 
     * @param keyOrder
     *            The natural index order.
     *            
     * @return The index name.
     */
    abstract public String getFQN(IKeyOrder<? extends R> keyOrder);
    
    /**
     * The index.
     * 
     * @param keyOrder
     *            The natural index order.
     *            
     * @return The index.
     */
    public IIndex getIndex(IKeyOrder<? extends R> keyOrder) {

        return indexManager.getIndex(getFQN(keyOrder), timestamp);
        
    }

}
