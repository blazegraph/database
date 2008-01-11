/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Feb 16, 2007
 */

package com.bigdata.btree;

import java.util.UUID;

import com.bigdata.service.Split;


/**
 * A fly-weight wrapper that does not permit write operations and reads
 * through onto an underlying {@link IIndex}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ReadOnlyIndex implements IIndexWithCounter, IRangeQuery {

    private final IIndex src;
    
    public ReadOnlyIndex(IIndex src) {
        
        if (src == null)
            throw new IllegalArgumentException();
        
        this.src = src;
        
    }

    public UUID getIndexUUID() {
        
        return src.getIndexUUID();
        
    }
    
    public String getStatistics() {
        
        return getClass().getSimpleName() + " : "+ src.getStatistics();
        
    }

    public ICounter getCounter() {
        
        if(src instanceof IIndexWithCounter) {
        
            IIndexWithCounter ndx = (IIndexWithCounter) src;
            
            return new ReadOnlyCounter(ndx.getCounter());
            
        } else {
            
            throw new UnsupportedOperationException();
            
        }
        
    }
    
    public boolean contains(byte[] key) {
        return src.contains(key);
    }

    public Object insert(Object key, Object value) {
        throw new UnsupportedOperationException();
    }

    public Object lookup(Object key) {
        return src.lookup(key);
    }

    public Object remove(Object key) {
        throw new UnsupportedOperationException();
    }

    public int rangeCount(byte[] fromKey, byte[] toKey) {
        return src.rangeCount(fromKey, toKey);
    }

    public IEntryIterator rangeIterator(byte[] fromKey, byte[] toKey) {
        return src.rangeIterator(fromKey, toKey);
    }

    public void contains(BatchContains op) {
        src.contains(op);
    }

    public void insert(BatchInsert op) {
        throw new UnsupportedOperationException();
    }

    public void lookup(BatchLookup op) {
        src.lookup(op);
    }

    public void remove(BatchRemove op) {
        throw new UnsupportedOperationException();
    }

    public void submit(int n, byte[][] keys, byte[][] vals,
            IIndexProcedureConstructor ctor, IResultAggregator aggregator) {

        Object result = ctor.newInstance(n, 0/* offset */, keys, vals).apply(this);
        
        aggregator.aggregate(result, new Split(null,0,n));
        
    }
    
    public boolean isIsolatable() {
        
        return src.isIsolatable();
        
    }
    
}
