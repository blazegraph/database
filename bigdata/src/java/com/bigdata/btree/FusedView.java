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
 * Created on Feb 11, 2008
 */

package com.bigdata.btree;

import java.util.UUID;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.btree.IIndexProcedure.IKeyRangeIndexProcedure;
import com.bigdata.btree.IIndexProcedure.ISimpleIndexProcedure;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.ICounterSet;
import com.bigdata.isolation.IsolatedFusedView;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.service.Split;

/**
 * <p>
 * A fused view providing read-write operations on multiple B+-Trees mapping
 * variable length unsigned byte[] keys to arbitrary values. The sources MUST
 * support deletion markers. The order of the sources MUST correspond to the
 * recency of their data. Writes will be directed to the first source in the
 * sequence (the most recent source). Deletion markers are used to prevent a
 * miss on a source from reading through to an older source. If a deletion
 * marker is encoutered the index entry will be understood as "not found" in the
 * fused view rather than reading through to an older source where it might
 * still have a binding.
 * </p>
 * 
 * @todo There is no efficient way to implement the {@link ILinearList} API for
 *       a fused view. Unlike a range-partitioned view, the keys in a fused view
 *       may be in any of the source indices. The only way to find the
 *       {@link ILinearList#keyAt(int)} an index is to use an iterator over the
 *       fused view and scan until the #of tuples traversed is equal to the
 *       given index. Likewise, the only way to find the
 *       {@link ILinearList#indexOf(byte[])} a key is to use an iterator over
 *       the fused view and scan until the key is matched.
 * 
 * @todo consider implementing {@link ILocalBTree} here and collapsing
 *       {@link ILocalBTreeView} and {@link ILocalBTree}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class FusedView implements IIndex, ILocalBTreeView {

    protected static final Logger log = Logger.getLogger(FusedView.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final protected boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final protected boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();

    /**
     * Holds the various btrees that are the sources for the view.
     */
    protected final AbstractBTree[] srcs;

    /**
     * A {@link ThreadLocal} {@link Tuple} that is used to copy the value
     * associated with a key out of the btree during lookup operations.
     */
    protected final ThreadLocal<Tuple> lookupTuple = new ThreadLocal<Tuple>() {

        @Override
        protected com.bigdata.btree.Tuple initialValue() {

            return new Tuple(srcs[0],VALS);

        }

    };
    
    /**
     * A {@link ThreadLocal} {@link Tuple} that is used for contains() tests.
     * The tuple does not copy either the keys or the values. Contains is
     * implemented as a lookup operation that either return this tuple or
     * <code>null</code>. When isolation is supported, the version metadata
     * is examined to determine if the matching entry is flagged as deleted in
     * which case contains() will report "false".
     */
    protected final ThreadLocal<Tuple> containsTuple = new ThreadLocal<Tuple>() {

        @Override
        protected com.bigdata.btree.Tuple initialValue() {

            return new Tuple(srcs[0],0);
            
        }
        
    };
    
    final public AbstractBTree[] getSources() {

        // Note: clone the array to prevent modification.
        return srcs.clone();
        
    }

    public BTree getMutableBTree() {
        
        return (BTree) srcs[0];
        
    }
    
    public IResourceMetadata[] getResourceMetadata() {
        
        IResourceMetadata[] resources = new IResourceMetadata[srcs.length];

        for(int i=0; i<srcs.length; i++) {
            
            resources[i] = srcs[i].getStore().getResourceMetadata();
            
        }

        return resources;
        
    }

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
     * 
     * @exception IllegalArgumentException
     *                if a source is used more than once.
     * @exception IllegalArgumentException
     *                unless all sources have the same indexUUID
     * @exception IllegalArgumentException
     *                unless all sources support delete markers.
     */
    public FusedView(final AbstractBTree[] srcs) {
        
        checkSources(srcs);
        
        this.srcs = srcs.clone();
        
    }
    
    /**
     * Checks the sources to make sure that they are all either isolatable, 
     * all non-null, and all have the same index UUID.
     * 
     * @param srcs The sources for a view.
     */
    static void checkSources(final AbstractBTree[] srcs) {
        
        if (srcs == null)
            throw new IllegalArgumentException("sources is null");

        /*
         * @todo allow this as a degenerate case, or create a factory that
         * produces the appropriate view?
         */
        if (srcs.length < 2) {
            
            throw new IllegalArgumentException(
                    "At least two sources are required");
            
        }
        
        for( int i=0; i<srcs.length; i++) {
            
            if (srcs[i] == null)
                throw new IllegalArgumentException("Source null @ index=" + i);

            if (!srcs[i].getIndexMetadata().getDeleteMarkers()) {

                throw new IllegalArgumentException(
                        "Source does not maintain delete markers @ index=" + i);

            }
                        
            for(int j=0; j<i; j++) {
                
                if (srcs[i] == srcs[j])
                    
                    throw new IllegalArgumentException(
                            "Source used more than once"
                            );
                

                if (!srcs[i].getIndexMetadata().getIndexUUID().equals(
                        srcs[j].getIndexMetadata().getIndexUUID())) {
                    
                    throw new IllegalArgumentException(
                            "Sources have different index UUIDs @ index=" + i
                            );
                    
                }
                
            }
            
        }
        
    }

    /** @deprecated. */
    final public UUID getIndexUUID() {
       
        return getIndexMetadata().getIndexUUID();
        
    }

    public IndexMetadata getIndexMetadata() {
        
        return srcs[0].getIndexMetadata();
        
    }
    
    synchronized final public ICounterSet getCounters() {

        if (counterSet == null) {

            counterSet = new CounterSet();

            for(int i=0; i<srcs.length; i++) {
        
                /*
                 * @todo might have to clone the counters from the index since
                 * they could already have another parent.
                 */
                
                counterSet.makePath("view[" + i + "]").attach(
                        srcs[i].getCounters());
            
            }

        }
        
        return counterSet;
        
    }
    private CounterSet counterSet;

    /**
     * The counter for the first source.
     */
    public ICounter getCounter() {
        
        return srcs[0].getCounter();
        
    }
    
    /**
     * Resolves the old value against the view and then directs the write to the
     * first of the sources specified to the ctor.
     */
    public byte[] insert(byte[] key, byte[] value) {

        byte[] oldval = lookup(key);
        
        srcs[0].insert(key, value);
        
        return oldval;

    }
    
    /**
     * Resolves the old value against the view and then directs the write to the
     * first of the sources specified to the ctor. The remove is in fact treated
     * as writing a deleted marker into the index.
     */
    public byte[] remove(byte[] key) {

        /*
         * Slight optimization prevents remove() from writing on the index if
         * there is no entry under that key (or if there is already a deleted
         * entry under that key).
         */
        Tuple tuple = lookup(key, lookupTuple.get());

        if (tuple == null || tuple.isDeletedVersion()) {

            /*
             * Either there was no entry under that key or the entry is already
             * marked as deleted in the view so we are done.
             */
            
            return null;
            
        }

        final byte[] oldval = tuple.getValue();
        
        srcs[0].remove(key);
        
        return oldval;

    }
    
    /**
     * Return the first value for the key in an ordered search of the trees in
     * the view.
     */
    final public byte[] lookup(byte[] key) {

        Tuple tuple = lookup(key, lookupTuple.get());

        if (tuple == null || tuple.isDeletedVersion()) {

            /*
             * Interpret a deletion marker as "not found".
             */
            
            return null;
            
        }

        return tuple.getValue();
        
    }

    /**
     * Per {@link AbstractBTree#lookup(byte[], Tuple)} but0 processes the
     * {@link AbstractBTree}s in the view in their declared sequence and stops
     * when it finds the first index entry for the key, even it the entry is
     * marked as deleted for that key.
     * 
     * @param key
     *            The search key.
     * @param tuple
     *            A tuple to be populated with data and metadata about the index
     *            entry (required).
     * 
     * @return <i>tuple</i> iff an index entry was found under that <i>key</i>.
     */
    final public Tuple lookup(final byte[] key, final Tuple tuple) {

        return lookup(0, key, tuple);

    }

    /**
     * Core implementation processes the {@link AbstractBTree}s in the view in
     * their declared sequence and stops when it finds the first index entry for
     * the key, even it the entry is marked as deleted for that key.
     * 
     * @param startIndex
     *            The index of the first source to be read. This permits the
     *            lookup operation to start at an index into the {@link #srcs}
     *            other than zero. This is used by {@link IsolatedFusedView} to
     *            read from just the groundState (everything except the
     *            writeSet, which is the source at index zero(0)).
     * @param key
     *            The search key.
     * @param tuple
     *            A tuple to be populated with data and metadata about the index
     *            entry (required).
     * 
     * @return <i>tuple</i> iff an index entry was found under that <i>key</i>.
     */
    final protected Tuple lookup(int startIndex, final byte[] key, final Tuple tuple) {

        for( int i=0; i<srcs.length; i++) {

            if( srcs[i].lookup(key, tuple) == null) {
                
                // No match yet.
                
                continue;
                
            }

            return tuple;
            
        }

        // no match.
        
        return null;

    }
    
    /**
     * Processes the {@link AbstractBTree}s in the view in sequence and returns
     * true iff the first {@link AbstractBTree} with an index entry under the
     * key is non-deleted.
     */
    final public boolean contains(byte[] key) {

        Tuple tuple = lookup(key,containsTuple.get());
        
        if (tuple == null || tuple.isDeletedVersion()) {

            /*
             * Interpret a deletion marker as "not found".
             */
            
            return false;
            
        }

        return true;
        
    }

    /**
     * Returns the sum of the range count on each index in the view. This is the
     * maximum #of entries that could lie within that key range. However, the
     * actual number could be less if there are entries for the same key in more
     * than one source index.
     * 
     * @todo this could be done using concurrent threads.
     * @todo watch for overflow of {@link Long#MAX_VALUE}
     */
    final public long rangeCount() {
        
        long count = 0;
        
        for (int i = 0; i < srcs.length; i++) {
            
            count += srcs[i].rangeCount();
            
        }
        
        return count;
        
    }

    /**
     * Returns the sum of the range count on each index in the view. This is the
     * maximum #of entries that could lie within that key range. However, the
     * actual number could be less if there are entries for the same key in more
     * than one source index.
     * 
     * @todo this could be done using concurrent threads.
     * @todo watch for overflow of {@link Long#MAX_VALUE}
     */
    final public long rangeCount(byte[] fromKey, byte[] toKey) {
        
        long count = 0;
        
        for (int i = 0; i < srcs.length; i++) {
            
            count += srcs[i].rangeCount(fromKey, toKey);
            
        }
        
        return count;
        
    }

    /**
     * The exact range count is obtained using a key-range scan over the view.
     * 
     * @todo watch for overflow of {@link Long#MAX_VALUE}
     */
    final public long rangeCountExact(byte[] fromKey, byte[] toKey) {

        final ITupleIterator itr = rangeIterator(fromKey, toKey,
                0/* capacity */, 0/* flags */, null/* filter */);

        long n = 0;

        while (itr.hasNext()) {

            itr.next();

            n++;

        }

        return n;
        
    }
    
    public ITupleIterator rangeIterator() {

        return rangeIterator(null, null);
        
    }

    /**
     * Returns an iterator that visits the distinct entries. When an entry
     * appears in more than one index, the entry is choosen based on the order
     * in which the indices were declared to the constructor.
     */
    final public ITupleIterator rangeIterator(byte[] fromKey, byte[] toKey) {

        return rangeIterator(fromKey, toKey, 0/* capacity */,
                DEFAULT/* flags */, null/* filter */);
        
    }

    /**
     * Core implementation.
     */
    public ITupleIterator rangeIterator(byte[] fromKey, byte[] toKey,
            int capacity, int flags, ITupleFilter filter) {

        return new FusedEntryIterator(srcs, fromKey, toKey, capacity, flags, filter);

    }
    
    final public Object submit(byte[] key, ISimpleIndexProcedure proc) {
        
        return proc.apply(this);
        
    }

    @SuppressWarnings("unchecked")
    final public void submit(byte[] fromKey, byte[] toKey,
            final IKeyRangeIndexProcedure proc, final IResultHandler handler) {

        Object result = proc.apply(this);
        
        if (handler != null) {
            
            handler.aggregate(result, new Split(null,0,0));
            
        }
        
    }
    
    @SuppressWarnings("unchecked")
    final public void submit(int fromIndex, int toIndex, byte[][] keys, byte[][] vals,
            AbstractIndexProcedureConstructor ctor, IResultHandler aggregator) {

        Object result = ctor.newInstance(this, fromIndex, toIndex, keys, vals)
                .apply(this);

        if (aggregator != null) {

            aggregator.aggregate(result, new Split(null, fromIndex, toIndex));

        }
        
    }

}
