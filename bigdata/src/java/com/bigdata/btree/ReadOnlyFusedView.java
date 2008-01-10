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
 * Created on Feb 1, 2007
 */

package com.bigdata.btree;

import java.util.UUID;

/**
 * <p>
 * A fused view providing read-only operations on multiple B+-Trees mapping
 * variable length unsigned byte[] keys to arbitrary values. This class does NOT
 * handle version counters or deletion markers.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo support N sources for a {@link ReadOnlyFusedView} by chaining together
 *       multiple {@link ReadOnlyFusedView} instances if not in a more efficient
 *       manner.
 */
public class ReadOnlyFusedView implements IIndex, IFusedView {

    /**
     * Holds the various btrees that are the sources for the view.
     */
    public final AbstractBTree[] srcs;
    
    /**
     * True iff all sources support isolation.
     */
    private final boolean isolatable;
    
    public AbstractBTree[] getSources() {
        
        return srcs;
        
    }
    
    public ReadOnlyFusedView(AbstractBTree src1, AbstractBTree src2) {
        
        this(new AbstractBTree[] { src1, src2 });
        
    }
    
    /**
     * 
     * @param srcs
     *            The ordered sources for the fused view. The order of the
     *            elements in this array determines which value will be selected
     *            for a given key by lookup() and which value is retained by
     *            rangeQuery().
     * 
     * @exception IllegalArgumentException
     *                if a source is used more than once.
     * @exception IllegalArgumentException
     *                unless all sources have the same
     *                {@link IIndex#getIndexUUID()}
     */
    public ReadOnlyFusedView(final AbstractBTree[] srcs) {
        
        if (srcs == null)
            throw new IllegalArgumentException("sources is null");

        if (srcs.length < 2) {
            throw new IllegalArgumentException(
                    "at least two sources are required");
        }

        if(srcs.length>2) {
            // @todo generalize to N>2 sources.
            throw new UnsupportedOperationException(
                    "Only two sources are supported.");
        }
        
        boolean isolatable = false; // arbitrary initial value.
        
        for( int i=0; i<srcs.length; i++) {
            
            if (srcs[i] == null)
                throw new IllegalArgumentException("a source is null");

            if(i==0) {
                
                isolatable = srcs[0].isIsolatable();
                
            }
            
            for(int j=0; j<i; j++) {
                
                if (srcs[i] == srcs[j])
                    
                    throw new IllegalArgumentException(
                            "Source used more than once"
                            );
                

                if (! srcs[i].getIndexUUID().equals(srcs[j].getIndexUUID())) {
                    
                    throw new IllegalArgumentException(
                            "Sources have different index UUIDs"
                            );
                    
                }
             
                if( isolatable && ! srcs[i].isIsolatable() ) {
                    
                    throw new IllegalArgumentException(
                            "Sources are not all same: isIsolatable()"
                            );
                    
                }
                
            }
            
        }

        this.srcs = srcs.clone();
        
        this.isolatable = isolatable;
        
    }
    
    public UUID getIndexUUID() {
       
        return srcs[0].getIndexUUID();
        
    }

    public String getStatistics() {

        StringBuilder sb = new StringBuilder();

        sb.append(getClass().getSimpleName());
        
        for(AbstractBTree ndx : srcs ) {
        
            sb.append("\n"+ndx.getStatistics());
            
        }
        
        return sb.toString();
        
    }

    /**
     * Write operations are not supported on the view.
     */
    public void insert(BatchInsert op) {
        
        throw new UnsupportedOperationException();
        
    }

    /**
     * Write operations are not supported on the view.
     */
    public void remove(BatchRemove op) {

        throw new UnsupportedOperationException();
        
    }
    
    public Object insert(Object key, Object value) {

        throw new UnsupportedOperationException();

    }

    public Object remove(Object key) {

        throw new UnsupportedOperationException();

    }

    /**
     * Return the first value for the key in an ordered search of the trees in
     * the view.
     */
    public Object lookup(Object key) {
        
        for( int i=0; i<srcs.length; i++) {
            
            Object ret = srcs[i].lookup(key);
            
            if (ret != null)
                return ret;
            
        }

        return null;
        
    }

    /**
     * Returns true if any tree in the view has an entry for the key.
     */
    public boolean contains(byte[] key) {
        
        for( int i=0; i<srcs.length; i++) {
            
            if (srcs[i].contains(key))
                return true;
            
        }

        return false;
        
    }
    
    /**
     * @todo implement and write test of chained lookup operations. the
     *       challenge here is that we only want the first value for a
     *       given key.  this seems to require that we mark tuples to
     *       be ignored on subsequent indices, which in turn needs to
     *       get into the batch api.  the contains() method already has
     *       been modified to finesse this.
     */
    public void lookup(BatchLookup op) {
        
        throw new UnsupportedOperationException();
        
    }

    public void contains( BatchContains op ) {

        for( int i=0; i<srcs.length; i++) {

            AbstractBTree src = srcs[i];
            
            // reset the first tuple index for each pass.
            op.tupleIndex = 0;
            
            src.contains(op);
            
        }

    }

    /**
     * Returns the sum of the range count on each index in the view. This is the
     * maximum #of entries that could lie within that key range. However, the
     * actual number could be less if there are entries for the same key in more
     * than one source index.
     * 
     * @todo this could be done using concurrent threads.
     */
    public int rangeCount(byte[] fromKey, byte[] toKey) {
        
        int count = 0;
        
        for(int i=0; i<srcs.length; i++) {
            
            count += srcs[i].rangeCount(fromKey, toKey);
            
        }
        
        return count;
        
    }

    /**
     * Returns an iterator that visits the distinct entries. When an entry
     * appears in more than one index, the entry is choosen based on the order
     * in which the indices were declared to the constructor.
     */
    public IEntryIterator rangeIterator(byte[] fromKey, byte[] toKey) {
        
        return new FusedEntryIterator(srcs, fromKey, toKey);
        
    }
    
    /**
     * <code>true</code> iff all sources support isolation.
     */
    public boolean isIsolatable() {
        
        return isolatable;
        
    }
    
}
