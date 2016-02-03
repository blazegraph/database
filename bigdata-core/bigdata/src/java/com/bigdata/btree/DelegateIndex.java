/*

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on Feb 20, 2008
 */

package com.bigdata.btree;

import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedureConstructor;
import com.bigdata.btree.proc.IKeyRangeIndexProcedure;
import com.bigdata.btree.proc.IResultHandler;
import com.bigdata.btree.proc.ISimpleIndexProcedure;
import com.bigdata.counters.CounterSet;
import com.bigdata.mdi.IResourceMetadata;

import cutthecrap.utils.striterators.IFilter;

/**
 * An object that delegates its {@link IIndex} interface.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class DelegateIndex implements IIndex {

    private final IIndex delegate;
    
    /**
     * @param delegate
     *            The delegate.
     */
    public DelegateIndex(IIndex delegate) {

        if (delegate == null) {

            throw new IllegalArgumentException();
            
        }
        
        this.delegate = delegate;
        
    }

    @Override
    public String toString() {
     
        final StringBuilder sb = new StringBuilder();
        
        sb.append(getClass().getSimpleName());
        
        sb.append("{ ");

        sb.append(delegate.toString());
        
        sb.append("}");
        
        return sb.toString();

    }
    
    @Override
    public boolean contains(byte[] key) {
        return delegate.contains(key);
    }

    @Override
    public ICounter getCounter() {
        return delegate.getCounter();
    }

    @Override
    public IndexMetadata getIndexMetadata() {
        return delegate.getIndexMetadata();
    }

    @Override
    public IResourceMetadata[] getResourceMetadata() {
        return delegate.getResourceMetadata();
    }

    @Override
    public CounterSet getCounters() {
        return delegate.getCounters();
    }

    @Override
    public byte[] insert(byte[] key, byte[] value) {
        return delegate.insert(key, value);
    }

    @Override
    public byte[] putIfAbsent(byte[] key, byte[] value) {
        return delegate.putIfAbsent(key, value);
    }

    @Override
    public byte[] lookup(byte[] key) {
        return delegate.lookup(key);
    }

    @Override
    public long rangeCount() {
        return delegate.rangeCount();
    }
    
    @Override
    public long rangeCount(byte[] fromKey, byte[] toKey) {
        return delegate.rangeCount(fromKey, toKey);
    }

    @Override
    public long rangeCountExact(byte[] fromKey, byte[] toKey) {
        return delegate.rangeCountExact(fromKey, toKey);
    }
    
    @Override
    public long rangeCountExactWithDeleted(byte[] fromKey, byte[] toKey) {
        return delegate.rangeCountExactWithDeleted(fromKey, toKey);
    }
    
    @Override
    public ITupleIterator rangeIterator() {
        return rangeIterator(null,null);
    }

    @Override
    public ITupleIterator rangeIterator(byte[] fromKey, byte[] toKey, int capacity, int flags, IFilter filter) {
        return delegate.rangeIterator(fromKey, toKey, capacity, flags, filter);
    }

    @Override
    public ITupleIterator rangeIterator(byte[] fromKey, byte[] toKey) {
        return delegate.rangeIterator(fromKey, toKey);
    }

    @Override
    public byte[] remove(byte[] key) {
        return delegate.remove(key);
    }

    @Override
    public <T> T submit(final byte[] key, final ISimpleIndexProcedure<T> proc) {
        return delegate.submit(key, proc);
    }

    @Override
    public void submit(byte[] fromKey, byte[] toKey, IKeyRangeIndexProcedure proc, IResultHandler handler) {
        delegate.submit(fromKey, toKey, proc, handler);
    }

    @Override
    public void submit(int fromIndex, int toIndex, byte[][] keys, byte[][] vals, AbstractKeyArrayIndexProcedureConstructor ctor, IResultHandler handler) {
        delegate.submit(fromIndex, toIndex, keys, vals, ctor, handler);
    }

    @Override
    public boolean contains(Object key) {
        return delegate.contains(key);
    }

    @Override
    public Object insert(Object key, Object value) {
        return delegate.insert(key, value);
    }

    @Override
    public Object lookup(Object key) {
        return delegate.lookup(key);
    }

    @Override
    public Object remove(Object key) {
        return delegate.remove(key);
    }

}
