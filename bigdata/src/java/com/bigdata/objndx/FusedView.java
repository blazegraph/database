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

package com.bigdata.objndx;

import java.util.Arrays;
import java.util.NoSuchElementException;

/**
 * <p>
 * A fused view providing read-only operations on multiple B+-Trees mapping
 * variable length unsigned byte[] keys to arbitrary values.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo support N sources for a {@link FusedView} by chaining together multiple
 *       {@link FusedView} instances if not in a more efficient manner.
 */
public class FusedView implements IIndex, IFusedView {

    /**
     * Holds the various btrees that are the sources for the view.
     */
    public final AbstractBTree[] srcs;
    
    public FusedView(AbstractBTree src1, AbstractBTree src2) {
        
        this(new AbstractBTree[] { src1, src2 });
        
    }
    
    /**
     * 
     * @param srcs
     *            The ordered sources for the fused view. The order of the
     *            elements in this array determines which value will be selected
     *            for a given key by lookup() and which value is retained by
     *            rangeQuery().
     */
    public FusedView(final AbstractBTree[] srcs) {
        
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
                
            }
            
        }

        this.srcs = srcs.clone();
        
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
     * Helper class merges entries from the sources in the view.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected static class FusedEntryIterator implements IEntryIterator {

        private final AbstractBTree[] srcs;
//        private final byte[] fromKey;
//        private final byte[] toKey;
        
        private final IEntryIterator[] itrs;
//        private final boolean[] available;
        private final boolean[] exhausted;

        /**
         * The current key from each source and null if we need to get the next
         * key from that source.
         */
        private final byte[][] keys;
        
        /**
         * Index of the iterator that returned the last value and -1 if no
         * iterator has returned a value yet.
         */
        private int current = -1;
        
        public FusedEntryIterator(AbstractBTree[] srcs,byte[] fromKey, byte[] toKey) {
        
            assert srcs != null;
            
            assert srcs.length > 0;
            
            this.srcs = srcs;
            
//            this.fromKey = fromKey;
//            
//            this.toKey = toKey;
            
            itrs = new IEntryIterator[srcs.length];

            for( int i=0; i<itrs.length; i++) {
                
                itrs[i] = srcs[i].rangeIterator(fromKey, toKey);
                
            }
            
//            available = new boolean[srcs.length];
//            
//            Arrays.fill(available,true);

            keys = new byte[itrs.length][];
            
            exhausted = new boolean[srcs.length];
            
            Arrays.fill(exhausted,false);
            
        }

        public byte[] getKey() {
            
            if(current == -1) throw new IllegalStateException();
            
            return itrs[current].getKey();
            
        }

        public Object getValue() {
            
            if(current == -1) throw new IllegalStateException();
            
            return itrs[current].getValue();
            
        }

        public boolean hasNext() {

            // @todo this could use advanceKeyStreams() instead.
            for( int i=0; i<itrs.length; i++ ) {
                
                if (!exhausted[i] && keys[i]!= null || itrs[i].hasNext()) {

                    return true;
                    
                }
                
            }
            
            return false;
            
        }

        /**
         * Make sure that we have the current key for each key stream. If we
         * already have a key for that stream then we use it.
         * 
         * @return The #of key streams with an available key in {@link #keys}.
         *         When zero(0), all key streams are exhausted.
         */
        private int advanceKeyStreams() {
            
            // #of key streams with a key for us to examine.
            int navailable = 0;
            
            for(int i=0; i<itrs.length; i++) {
                
                if (exhausted[i])
                    continue;
                
                if (keys[i] == null) {

                    if (itrs[i].hasNext()) {

                        itrs[i].next();

                        keys[i] = itrs[i].getKey();

                        navailable++;

                    } else {
                        
                        exhausted[i] = true;
                        
                    }
                    
                } else {
                    
                    navailable++;
                    
                }
                
            }

            return navailable;
            
        }
        
        /**
         * We are presented with an ordered set of key streams. Each key stream
         * delivers its keys in order. For a given key, we always choose the
         * first stream having that key. Once a key is found, all subsequent key
         * streams are then advanced until their next key is greater than the
         * current key (this can only cause the same key to be skipped).
         * <p>
         * 
         * Each invocation of this method advances one key in the union of the
         * key streams. We test the current key for each stream on each pass and
         * choose the key that orders first across all key streams.
         * <p>
         * 
         * In the simple case with two streams we examine the current
         * {@link #keys key} on each stream, fetching the next key iff there is
         * no key available on that stream and otherwise using the current key
         * from that stream. If the keys are the same, then we choose the first
         * stream and also clear the current {@link #keys key} for the other
         * stream so that we will skip the current entry on that key stream. If
         * the keys differ, then we choose the stream with the lessor key and
         * clear the {@link #keys key} for that stream to indicate that it has
         * been consumed. In any case we set the index of the choosen stream on
         * {@link #current} so that the other methods on this api will use the
         * corresponding key and value from that stream and return the current
         * value for the choosen stream.
         * 
         * @todo generalize to N>2 key streams.
         */
        public Object next() {

            assert srcs.length == 2;

            int navailable = advanceKeyStreams();

            if(navailable == 0) {
                
                throw new NoSuchElementException();
                
            }
            
            /*
             * Generalization to N streams might sort {key,order,itr} tuples.
             * The choice of the stream with the lessor key is then the first
             * entry in sorted tuples if the comparator pays attention to the
             * stream [order] in addition to the keys.
             * 
             * if a stream is exhausted then we no longer consider it as a key
             * source.
             */
            
            final int cmp = (keys[0] == null ? 1 : keys[1] == null ? -1
                    : BytesUtil.compareBytes(keys[0], keys[1]));
            
            if( cmp == 0 ) {

                // Choose the first stream in a tie.
                
                current = 0;
                
                // The current key on each stream tied in 1st place is consumed.
                
                keys[0] = keys[1] = null;  
                
            } else {
                
                // Choose the stream with the lessor key.
                
                current = cmp < 0 ? 0 : 1;

                keys[current] = null; // this key was consumed.

            }

            // Return the current object on the choosen stream.
            Object value = itrs[current].getValue();  return value;
            
        }

        public void remove() {
            
            throw new UnsupportedOperationException();
            
        }
        
    }
    
}
