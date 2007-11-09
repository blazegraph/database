/*

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
package com.bigdata.btree;

import java.util.Arrays;
import java.util.NoSuchElementException;

/**
     * Provides a read-only view of the source {@link IEntryIterator}s that
     * maintains the order of the visited entries.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public class FusedEntryIterator implements IEntryIterator {

        private final IEntryIterator[] itrs;
        
//        private final boolean[] available;
        private final boolean[] exhausted;

        /**
         * The current key from each source and <code>null</code> if we need
         * to get the next key from that source.
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

        public FusedEntryIterator(IEntryIterator[] srcs) {
        
            assert srcs != null;
            
            assert srcs.length > 0;            
            
            for( int i=0; i<srcs.length; i++) {
                
                assert srcs[i] != null;
                
            }
            
            this.itrs = srcs;
            
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

            assert itrs.length == 2;

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