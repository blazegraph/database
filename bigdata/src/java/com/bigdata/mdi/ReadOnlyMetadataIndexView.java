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
 * Created on Feb 20, 2008
 */

package com.bigdata.mdi;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.DelegateIndex;
import com.bigdata.btree.ICounter;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.ReadOnlyCounter;
import com.bigdata.btree.filter.IFilterConstructor;
import com.bigdata.btree.proc.AbstractIndexProcedureConstructor;
import com.bigdata.btree.proc.IIndexProcedure;
import com.bigdata.btree.proc.IResultHandler;
import com.bigdata.service.EmbeddedFederation;
import com.bigdata.service.Split;

/**
 * Implementation disallows writes on the index.
 * <p>
 * Note: This is used by the {@link EmbeddedFederation} to return a
 * read-only view.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ReadOnlyMetadataIndexView extends MetadataIndexView {

    /**
     * @param delegate
     */
    public ReadOnlyMetadataIndexView(AbstractBTree delegate) {

        super(delegate);
        
    }

    /** {@link IndexMetadata} is cloned to disallow modification. */
    final public IndexMetadata getIndexMetadata() {

        return super.getIndexMetadata().clone();
        
    }

    /**
     * {@link IResourceMetadata}[] is cloned to disallow modification (the
     * {@link IResourceMetadata} instances in the array are all dynamically
     * created so changes to them do not propagate back to the index).
     */
    final public IResourceMetadata[] getResourceMetadata() {

        return super.getResourceMetadata().clone();
        
    }

    /**
     * Counter is read-only.
     */
    final public ICounter getCounter() {

        return new ReadOnlyCounter(super.getCounter());
        
    }
    
    /**
     * Disabled.
     */
    final public byte[] insert(byte[] key, byte[] value) {

        throw new UnsupportedOperationException();
        
    }

    /**
     * Disabled.
     */
    final public byte[] remove(byte[] key) {

        throw new UnsupportedOperationException();
        
    }

    final public ITupleIterator rangeIterator(byte[] fromKey, byte[] toKey,
            int capacity, int flags, IFilterConstructor filter) {

        return super.rangeIterator(fromKey, toKey, capacity, flags
                | IRangeQuery.READONLY, filter);
        
    }
    
    /**
     * Overriden to ensure that procedure is applied against read-only view and
     * not the {@link DelegateIndex}.
     */
    final public Object submit(byte[] key, IIndexProcedure proc) {
    
        return proc.apply(this);
        
    }

    /**
     * Overriden to ensure that procedure is applied against read-only view and
     * not the {@link DelegateIndex}.
     */
    @SuppressWarnings("unchecked")
    final public void submit(byte[] fromKey, byte[] toKey,
            final IIndexProcedure proc, final IResultHandler handler) {

        Object result = proc.apply(this);
        
        if (handler != null) {
            
            handler.aggregate(result, new Split(null,0,0));
            
        }
        
    }
    
    /**
     * Overriden to ensure that procedure is applied against read-only view and
     * not the {@link DelegateIndex}.
     */
    @SuppressWarnings("unchecked")
    final public void submit(int fromIndex, int toIndex, byte[][] keys, byte[][] vals,
            AbstractIndexProcedureConstructor ctor, IResultHandler aggregator) {

        Object result = ctor.newInstance(this,fromIndex,toIndex, keys, vals).apply(this);
        
        if(aggregator != null) {

            aggregator.aggregate(result, new Split(null,fromIndex,toIndex));
            
        }
        
    }

}
