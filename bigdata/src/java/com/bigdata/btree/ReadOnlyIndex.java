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

import com.bigdata.btree.IIndexProcedure.IIndexProcedureConstructor;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.service.Split;

/**
 * A fly-weight wrapper that does not permit write operations and reads
 * through onto an underlying {@link IIndex}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ReadOnlyIndex implements IIndex {

    private final IIndex src;
    
    public ReadOnlyIndex(IIndex src) {
        
        if (src == null)
            throw new IllegalArgumentException();
        
        this.src = src;
        
    }

    public UUID getIndexUUID() {
        
        return src.getIndexMetadata().getIndexUUID();
        
    }
    
    public IndexMetadata getIndexMetadata() {
        
        return src.getIndexMetadata();
        
    }

    final public String getStatistics() {
        
        return getClass().getSimpleName() + " : "+ src.getStatistics();
        
    }

    final public ICounter getCounter() {
        return new ReadOnlyCounter(src.getCounter());
    }
    
    final public boolean contains(byte[] key) {
        return src.contains(key);
    }

    final public byte[] insert(byte[] key, byte[] value) {
        throw new UnsupportedOperationException();
    }

    final public byte[] lookup(byte[] key) {
        return src.lookup(key);
    }

    final public byte[] remove(byte[] key) {
        throw new UnsupportedOperationException();
    }

    final public long rangeCount(byte[] fromKey, byte[] toKey) {
        return src.rangeCount(fromKey, toKey);
    }

    final public IEntryIterator rangeIterator(byte[] fromKey, byte[] toKey) {
        return src.rangeIterator(fromKey, toKey);
    }

    final public IEntryIterator rangeIterator(byte[] fromKey, byte[] toKey,
            int capacity, int flags, IEntryFilter filter) {

        if ((flags & REMOVEALL) != 0) {

            /*
             * Note: Must be explicitly disabled!
             */
            
            throw new UnsupportedOperationException();
            
        }
        
        return new ReadOnlyEntryIterator(src.rangeIterator(fromKey, toKey,
                capacity, flags, filter));
        
    }
    
    public Object submit(byte[] key, IIndexProcedure proc) {
        
        return proc.apply(this);
        
    }

    @SuppressWarnings("unchecked")
    public void submit(byte[] fromKey, byte[] toKey,
            final IIndexProcedure proc, final IResultHandler handler) {

        Object result = proc.apply(this);
        
        if(handler!=null) {
            
            handler.aggregate(result, new Split(null,0,0));
            
        }
        
    }
    
    @SuppressWarnings("unchecked")
    final public void submit(int n, byte[][] keys, byte[][] vals,
            IIndexProcedureConstructor ctor, IResultHandler aggregator) {

        Object result = ctor.newInstance(n, 0/* offset */, keys, vals).apply(this);
        
        aggregator.aggregate(result, new Split(null,0,n));
        
    }

    public IResourceMetadata[] getResourceMetadata() {

        return src.getResourceMetadata();
        
    }
    
}
