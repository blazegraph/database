/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

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
        
        for( int i=0; i<srcs.length; i++) {
            
            if (srcs[i] == null)
                throw new IllegalArgumentException("a source is null");
            
            for(int j=0; j<i; j++) {
                
                if (srcs[i] == srcs[j])
                    throw new IllegalArgumentException(
                            "source used more than once");

                if (! srcs[i].getIndexUUID().equals(srcs[j].getIndexUUID())) {
                    throw new IllegalArgumentException(
                            "Sources have different index UUIDs");
                }
                
            }
            
        }

        this.srcs = srcs.clone();
        
    }
    
    public UUID getIndexUUID() {
        return srcs[0].getIndexUUID();
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
    
}
