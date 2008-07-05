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

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;

import com.bigdata.btree.IIndex;
import com.bigdata.journal.IIndexManager;
import com.bigdata.relation.accesspath.IKeyOrder;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <E>
 *            The generic type of the [E]lements of the relation.
 */
abstract public class AbstractRelation<E> implements IMutableRelation<E> {

    protected static Logger log = Logger.getLogger(AbstractRelation.class);
    
    private final IIndexManager indexManager;
    
    private final ExecutorService service;
    
    private final String namespace;

    private final IRelationName<E> relationName;

    private final long timestamp;
    
    private final Properties properties;

    public IIndexManager getIndexManager() {
        
        return indexManager;
        
    }
    
    public ExecutorService getExecutorService() {
        
        return service;
        
    }
    
    public String getNamespace() {
        
        return namespace;
        
    }
    
    public IRelationName getRelationName() {

        return relationName;
        
    }

    public long getTimestamp() {
        
        return timestamp;
        
    }

    /**
     * Return an object wrapping the properties specified to the ctor.
     */
    public Properties getProperties() {
        
        return new Properties(properties);
        
    }
    
    /**
     * The class name, timestamp and namespace for the relation view.
     */
    public String toString(){
        
        return getClass().getSimpleName() + "{timestamp=" + timestamp
                + ", namespace=" + namespace + "}";

    }

    /**
     * 
     */
    protected AbstractRelation(ExecutorService service,
            IIndexManager indexManager, String namespace, long timestamp,
            Properties properties) {

        if (service == null)
            throw new IllegalArgumentException();

        if (indexManager == null)
            throw new IllegalArgumentException();

        if (namespace == null)
            throw new IllegalArgumentException();

        if (properties == null)
            throw new IllegalArgumentException();

        this.service = service;

        this.indexManager = indexManager;

        this.namespace = namespace;

        this.relationName = new RelationName<E>(namespace);

        this.timestamp = timestamp;

        this.properties = properties;
        
        properties.setProperty(RelationSchema.NAMESPACE, namespace);

        properties.setProperty(RelationSchema.CLASS, getClass().getName());
        
    }

    /**
     * The fully qualified name of the index.
     * 
     * @param keyOrder
     *            The natural index order.
     * 
     * @return The index name.
     */
    abstract public String getFQN(IKeyOrder<? extends E> keyOrder);
    
    /**
     * The index.
     * 
     * @param keyOrder
     *            The natural index order.
     *            
     * @return The index.
     */
    public IIndex getIndex(IKeyOrder<? extends E> keyOrder) {

        return indexManager.getIndex(getFQN(keyOrder), timestamp);
        
    }

    /**
     * 
     * @todo race conditions in index registration could be handled by having a
     *       property that indicated whether or not the indices were in the
     *       process of being registered. The value of the property could be a
     *       UUID identifying the client. The property would be cleared once the
     *       indices were registered. Other clients observing the property would
     *       wait until it had been cleared. If not cleared within a timeout
     *       (say 10 seconds) then the waiting client should assume that the
     *       client attempting to register the indices had died. In that case
     *       the client should itself assert the property and then attempt to
     *       register the indices itself - dropping existing indices is Ok IFF
     *       they are empty, otherwise we need a thrown exception and a flag to
     *       allow override.
     *       <p>
     *       To avoid a race condition when the client attemps to take a
     *       compensating action we need a means to update a property iff some
     *       pre-condition is satisified.
     *       <p>
     *       A similar approach could be used when taking a triple store offline
     *       and dropping its indices.
     *       <p>
     *       Add ctor flag to create iff not found?
     */
    public AbstractRelation<E> create() {
        
        log.info(toString());

        /*
         * Convert the Properties to a Map.
         */
        final Map<String,Object> map = new HashMap<String, Object>();
        
        Enumeration<? extends Object> e = properties.propertyNames();

        while (e.hasMoreElements()) {

            final Object key = e.nextElement();

            if (!(key instanceof String)) {

                log.warn("Will not store non-String key: "+key);
                
                continue;
                
            }

            final String name = (String) key;

            map.put(name, properties.getProperty(name));

        }

        // Write the map on the row store.
        final Map afterMap = indexManager.getGlobalRowStore().write(RelationSchema.INSTANCE, map);
        
        if(log.isDebugEnabled()) {
            
            log.debug("Properties after write: "+afterMap);
            
        }
        
        return this;
        
    }
    
    public void destroy() {

        log.info(toString());

        // Delete the entry for this relation from the row store.
        indexManager.getGlobalRowStore().delete(RelationSchema.INSTANCE, namespace);
        
    }
    
//    /**
//     * @todo javadoc, move to database class?
//     *
//     * @todo problem is access to the relationLocator.  we really want to
//     * reuse the same instance whenever possible to improve caching, but
//     * we also need to be able to run on any of the various platforms.
//     * There is no issue for IBigdataFederation, just TemporaryStore and
//     * Journal.  When used with a federation they SHOULD use the same
//     * locator.  The main issue is when they are used by themselves.  This
//     * may require either raising the locator into a parameter to the ctor
//     * for this class or passing it in explicitly to a method such as this
//     * one.
//     * 
//     * @param solutionFlags
//     * 
//     * @return
//     */
//    abstract public IJoinNexus newJoinNexus(int solutionFlags);
    
}
