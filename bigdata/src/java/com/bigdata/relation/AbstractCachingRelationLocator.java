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

import com.bigdata.cache.LRUCache;
import com.bigdata.cache.WeakValueCache;
import com.bigdata.util.NT;

/**
 * Abstract base class for {@link IRelationLocator}s with caching.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractCachingRelationLocator<R> implements IRelationLocator<R> {

    private transient WeakValueCache<NT,IRelation<R>> cache;

    private int capacity;
    
    private IRelationFactory<R> relationFactory;
    
    protected static transient final int DEFAULT_CACHE_CAPACITY = 10;

    /**
     * The cache capacity.
     */
    public int capacity() {
        
        return capacity;
        
    }

    public IRelationFactory<R> getRelationFactory() {
        
        return relationFactory;
        
    }
    
    /**
     * De-serialization ctor.
     */
    protected AbstractCachingRelationLocator() {
        
    }
    
    public AbstractCachingRelationLocator(IRelationFactory<R> relationFactory) {

        this(relationFactory, DEFAULT_CACHE_CAPACITY);

    }

    public AbstractCachingRelationLocator(IRelationFactory<R> relationFactory, int capacity) {

        if (relationFactory == null)
            throw new IllegalArgumentException();

        this.relationFactory = relationFactory;

        this.capacity = capacity;
        
        this.cache = new WeakValueCache<NT, IRelation<R>>(
                new LRUCache<NT, IRelation<R>>(capacity));

    }

    /**
     * Looks up the relation in the cache.
     * <p>
     * Note: The caller MUST be synchronized across their
     * {@link IRelationLocator#getRelation(IRelationName, long)} implementation.
     * 
     * @param relationName
     * 
     * @param timestamp
     * 
     * @return The relation -or- <code>null</code> iff it is not in the cache.
     */
    protected IRelation<R> get(IRelationName<R> relationName, long timestamp) {

        if (relationName == null)
            throw new IllegalArgumentException();

        final String namespace = relationName.toString();

        final IRelation<R> r = cache.get(new NT(namespace, timestamp));
        
        return r;

    }

    /**
     * Places the relation in the cache.
     * <p>
     * Note: The caller MUST be synchronized across their
     * {@link IRelationLocator#getRelation(IRelationName, long)} implementation.
     * 
     * @param relation
     *            The relation.
     */
    protected void put(IRelation<R> relation) {
        
        if (relation == null)
            throw new IllegalArgumentException();
        
        // @todo IRelation#getNamespace()
        final String namespace = relation.getRelationName().toString();

        final long timestamp = relation.getTimestamp();
        
        cache.put(new NT(namespace, timestamp), relation, false/* dirty */);
        
    }

//    @SuppressWarnings("unchecked")
//    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
//
//        this.relationFactory = (IRelationFactory<R>)in.readObject();
//
//        this.capacity = in.readInt();
//        
//        this.cache = new WeakValueCache<NT, IRelation<R>>(
//                new LRUCache<NT, IRelation<R>>(capacity));
//        
//    }
//
//    public void writeExternal(ObjectOutput out) throws IOException {
//
//        out.writeObject(relationFactory);
//        
//        out.writeInt(capacity);
//        
//    }
    
}
