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
 * Created on Aug 9, 2008
 */

package com.bigdata.btree.view;

import java.util.NoSuchElementException;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleCursor;
import com.bigdata.btree.ITupleCursor2;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.keys.KeyBuilder;

/**
 * Layers on the additional methods from the {@link ITupleCursor} interface.
 * <p>
 * Note: Both the public methods and the internal fields are strongly typed as
 * {@link ITupleCursor}s rather than {@link ITupleIterator}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class FusedTupleCursor<E> extends FusedTupleIterator<ITupleCursor<E>, E>
        implements ITupleCursor<E> {

    /**
     * The source view.  This is required by {@link #getIndex()} and also to
     * implement {@link #remove()}.
     */
    private final IIndex ndx;

    /**
     * <code>true</code> iff the current direction of iterator progress is
     * "forward", meaning that {@link #hasNext()} was called and that the
     * {@link #next()} {@link ITuple}s are currently buffered by
     * {@link #sourceTuple}.
     * <p>
     * This is field always <code>true</code> for this class, but the
     * direction can be changed in the {@link FusedTupleCursor} subclass by
     * calling {@link ITupleCursor#hasPrior()}. If the direction is changed
     * then {@link #sourceTuple}s MUST be cleared before testing whether or not
     * the next/prior tuple exists. Likewise, {@link ITupleCursor#seek(byte[])}
     * must clear {@link #sourceTuple}s but can leave this flag alone.
     */
    private boolean forward = true;
    
    /**
     * @param flags
     * @param deleted
     * @param srcs
     *            The source iterators.
     * @param ndx
     *            The {@link FusedView} from which the source iterators were
     *            drawn (required). Note that the semantics of {@link #remove()}
     *            on a fused view require that the tuple is overwritten by a
     *            delete marker in the 1st index of the view.
     */
    public FusedTupleCursor(int flags, boolean deleted, ITupleCursor<E>[] srcs,
            IIndex ndx) {

        super(flags, deleted, srcs);

        if (ndx == null)
            throw new IllegalArgumentException();
        
        this.ndx = ndx;
        
    }

    final public IIndex getIndex() {

        return ndx;
        
    }

    /**
     * <p>
     * Set the direction of iterator progress. Clears {@link #sourceTuple} iff
     * the current direction is different from the new direction and is
     * otherwise a NOP.
     * </p>
     * <p>
     * Note: Care is required for sequences such as
     * </p>
     * 
     * <pre>
     * ITuple t1 = next();
     * 
     * ITuple t2 = prior();
     * </pre>
     * 
     * <p>
     * to visit the same tuple for {@link #next()} and {@link #prior()}.
     * </p>
     * 
     * @param forward
     *            <code>true</code> iff the new direction of iterator progress
     *            is forward using {@link #hasNext()} and {@link #next()}.
     */
    private void setForwardDirection(boolean forward) {

        if (this.forward != forward) {

            if (INFO)
                log.info("Changing direction: forward=" + forward);

            /*
             * This is the last key visited -or- null iff nothing has been
             * visited.
             */
            final byte[] lastKeyVisited;
            if (lastVisited == -1) {
                
                lastKeyVisited = null;
                
            } else {
            
//                lastKeyVisited = ((ITupleCursor2<E>) sourceIterator[lastVisited])
//                        .tuple().getKey();
                lastKeyVisited = lastKeyBuffer.getKey();
                
                if (INFO)
                    log.info("key for last tuple visited="
                            + BytesUtil.toString(lastKeyVisited));
            }
            
            for (int i = 0; i < n; i++) {
  
                /*
                 * Recover the _current_ tuple for each source iterator.
                 */

                // current tuple for the source iterator.
                ITuple<E> tuple = ((ITupleCursor2<E>) sourceIterator[i])
                        .tuple();
                
                if(INFO)
                    log.info("sourceIterator[" + i + "]=" + tuple);

                if (lastKeyVisited != null) {

                    /*
                     * When we are changing to [forward == true] (visiting the
                     * next tuples in the index order), then we advance the
                     * source iterator zero or more tuples until it is
                     * positioned GT the lastVisitedKey.
                     * 
                     * When we are changing to [forward == false] (visiting the
                     * prior tuples in the index order), then we backup the
                     * source iterator zero or more tuples until it is
                     * positioned LT the lastVisitedKey.
                     */
                    
                    while (tuple != null) {

                        final int ret = BytesUtil.compareBytes(//
                                tuple.getKey(), //
                                lastKeyVisited  //
                                );

                        final boolean ok = forward ? ret > 0 : ret < 0;

                        if(ok) break;
                        
                        /*
                         * If the source iterator is currently positioned on the
                         * same key as the last tuple that we visited then we
                         * need to move it off of that key - either to the
                         * previous or the next visitable tuple depending on the
                         * new direction for the iterator.
                         */

                        if (forward) {
                            
                            if (sourceIterator[i].hasNext()) {
                            
                                // next tuple
                                tuple = sourceIterator[i].next();
                                
                            } else {
                                
                                // exhausted in this direction.
                                tuple = null;
                                
                            }
                            
                        } else {
                            
                            if (sourceIterator[i].hasPrior()) {
                            
                                // prior tuple
                                tuple = sourceIterator[i].prior();
                                
                            } else {
                                
                                // exhausted in this direction.
                                tuple = null;
                                
                            }
                            
                        }

                        if (INFO)
                            log.info("skipping tuple: source=" + i
                                    + ", direction="
                                    + (forward ? "next" : "prior")
                                    + ", newTuple=" + tuple);

                    }

                }
                
                sourceTuple[i] = tuple;
                    
                // as assigned to source[i].
                if (INFO)
                    log.info("sourceTuple   [" + i + "]=" + sourceTuple[i]);

            }
            
            // set the new iterator direction.
            this.forward = forward;
            
            // clear current since the old lookahead choice is no longer valid.
            this.current = -1;

        }

    }

    public boolean hasNext() {

        setForwardDirection(true/*forward*/);
        
        return super.hasNext();

    }
    
    /**
     * Note: The implementation of {@link #hasPrior()} closes parallels the
     * implementation of {@link #hasNext()} in the base class.
     */
    public boolean hasPrior() {

        setForwardDirection(false/*forward*/);
            
        /*
         * Until we find an undeleted tuple (or any tuple if DELETED is
         * true).
         */
        while (true) {

            if (current != -1) {

                if(INFO) log.info("Already matched: source=" + current);
                
                return true;

            }

            /*
             * First, make sure that we have a tuple for each source iterator
             * (unless that iterator is exhausted).
             */

            int nexhausted = 0;

            for (int i = 0; i < n; i++) {

                if (sourceTuple[i] == null) {

                    if (sourceIterator[i].hasPrior()) {

                        sourceTuple[i] = sourceIterator[i].prior();

                        if (DEBUG)
                            log.debug("read sourceTuple[" + i + "]="
                                    + sourceTuple[i]);
                        
                    } else {

                        nexhausted++;

                    }

                }

            }

            if (nexhausted == n) {

                // the aggregate iterator is exhausted.

                return false;

            }

            /*
             * Now consider the current tuple for each source iterator in turn
             * and choose the _first_ iterator having a tuple whose key orders
             * GTE all the others (or LTE if [reverseScan == true]). This is the
             * previous tuple to be visited by the aggregate iterator.
             */
            {

                // current is index of the smallest key so far.
                assert current == -1;

                byte[] key = null; // smallest key so far.

                for (int i = 0; i < n; i++) {

                    if (sourceTuple[i] == null) {

                        // This source is exhausted.
                        
                        continue;
                        
                    }

                    if (current == -1) {

                        current = i;

                        key = sourceTuple[i].getKey();

                        assert key != null;

                    } else {

                        final byte[] tmp = sourceTuple[i].getKey();

                        final int ret = BytesUtil.compareBytes(tmp, key);

//                        if (reverseScan ? ret < 0 : ret > 0) {
                        if (ret > 0) {

                            /*
                             * This key orders GT the current key.
                             * 
                             * Note: This test MUST be strictly GT since GTE
                             * would break the precedence in which we are
                             * processing the source iterators and give us the
                             * key from the last source by preference when we
                             * need the key from the first source by preference.
                             */

                            current = i;

                            key = tmp;

                        }

                    }

                }

                assert current != -1;

            }
            
            if (sourceTuple[current].isDeletedVersion() && !deleted) {

                /*
                 * The tuple is marked as "deleted" and the caller did not
                 * request deleted tuples so we skip this key and begin again
                 * with the next key visible under the fused iterator view.
                 */

                if(INFO) {
                    
                    log.info("Skipping deleted: source=" + current + ", tuple="
                            + sourceTuple[current]);
                    
                }

                /*
                 * Clear tuples from other sources having the same key as the
                 * current tuple.
                 */
                
                clearCurrent();
                
                continue;
                
            }

            if(INFO) {
                
                log.info("Will visit next: source=" + current + ", tuple: "
                        + sourceTuple[current]);
                
            }
            
            return true;
            
        }

    }

    public ITuple<E> prior() {

        if (!hasPrior())
            throw new NoSuchElementException();

        return consumeLookaheadTuple();

    }
    
    public ITuple<E> seek(final byte[] key) {
        
        // clear last visited.
        lastVisited = -1;
        
        // clear current.
        current = -1;

        // save the sought key.
        lastKeyBuffer.reset().append(key);
        
        for (int i = 0; i < n; i++) {

            sourceTuple[i] = sourceIterator[i].seek(key);

            if (sourceTuple[i] != null && current == -1) {

                /*
                 * Choose the tuple reported by the first source iterator in the
                 * order in which the source iterators are processed. Any
                 * iterator that does not have a tuple for the seek key will
                 * report a null. The first non-null will therefore be the first
                 * iterator having an exact match for the given key.
                 */
                
                if (INFO) {

                    log.info("Found match: source=" + i + ", key="
                            + BytesUtil.toString(key));
                    
                }
                
                current = i;

            }

        }

        // the lookahead tuples are primed for forward traversal.
        forward = true;

        if(!deleted){
        
            for(int i=0; i<n; i++) {
                
                if (sourceTuple[i] != null && sourceTuple[i].isDeletedVersion()) {

                    /*
                     * The tuple is marked as "deleted" and the caller did not
                     * request deleted tuples. In this case seek(byte[]) must
                     * return null.
                     */
                    if (INFO)
                        log.info("Skipping deleted: source=" + current
                                + ", tuple=" + sourceTuple[current]);

                    /*
                     * Clear tuples from other sources having the same key as the
                     * current tuple.
                     */
                    
                    clearCurrent();

                    return null;
                    
                }

            }
            
        }
        
        if (current == -1) {

            /*
             * There is no tuple equal to the sought key.
             */

            // no tuple for that key.
            return null;

        } else {

            /*
             * There is a tuple for that key, so consume and return it.
             */
            
            return consumeLookaheadTuple();
       
        }
        
    }

    /**
     * Extended to make a copy of the key for each visited tuple.
     * 
     * @see #lastKeyBuffer
     */
    protected ITuple<E> consumeLookaheadTuple() {

        assert current != -1;
        
        lastKeyBuffer.reset().append(sourceTuple[current].getKey());
        
        return super.consumeLookaheadTuple();
        
    }
    
    final public ITuple<E> seek(Object key) {

        if (key == null)
            throw new IllegalArgumentException();

        return seek(getIndex().getIndexMetadata().getTupleSerializer()
                .serializeKey(key));
        
    }

    /**
     * Delegates the operation to the source view (correct deletion requires
     * that a delete marker for the tuple is written onto first source index
     * rather than deleting the tuple from the source from which it was
     * materialized).
     * <p>
     * Note: You must specify {@link IRangeQuery#CURSOR} in order for
     * {@link #remove()} to be supported.
     */
    @SuppressWarnings("unchecked")
    @Override
    public void remove() {

        if (lastVisited == -1)
            throw new IllegalStateException();

//        /*
//         * Use the last tuple actually visited for the source cursor that
//         * materialized the tuple to be removed.
//         */
//        final ITuple tuple = ((ITupleCursor2)sourceIterator[lastVisited]).tuple();
//        
//        assert tuple != null;
//        
//        if(!tuple.getKeysRequested()) {
//            
//            throw new UnsupportedOperationException("KEYS not specified");
//            
//        }
//        
//        final byte[] key = tuple.getKey();

        // the last key visited by the fused iterator.
        final byte[] key = lastKeyBuffer.getKey();
        
        /*
         * Assuming that the ctor was given the FusedView, this will cause a
         * deleted marker to be written on the first index in the fused view for
         * this key. This is the correct semantics for deleting a tuple from a
         * fused view.
         */
        
        ndx.remove( key );

    }

    /**
     * A copy of the key for the last tuple visited.
     */
    final private KeyBuilder lastKeyBuffer = new KeyBuilder(0);

}
