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
/*
 * Created on Nov 13, 2007
 */

package com.bigdata.journal;

import java.util.UUID;

import com.bigdata.btree.IIndexProcedure;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.ICounter;
import com.bigdata.btree.IEntryFilter;
import com.bigdata.btree.IEntryIterator;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IResultHandler;
import com.bigdata.btree.ReadOnlyCounter;
import com.bigdata.btree.IIndexProcedure.IIndexProcedureConstructor;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.service.Split;

/**
 * Light-weight implementation of a read-committed index view.
 * <p>
 * A delegation model is used since commits or overflows of the journal might
 * invalidate the index objects that actually read on the journal and/or index
 * segments. The delegation strategy checks the commit counters and looks up a
 * new delegate index object each time the commit counter is updated. In this
 * way, newly committed data are always made visible to the next operation on
 * the index. In-progress writes are NOT visible since we only read from a
 * delegate index discovered by resolving the index name against the most recent
 * {@link ICommitRecord}.
 * 
 * @todo consider a singleton factory for {@link ReadCommittedIndex} views on
 *       the journal.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ReadCommittedIndex implements IIndex {

    /**
     * The journal.
     */
    final protected AbstractJournal journal;
    
    /**
     * The name of the index.
     */
    final protected String name;
    
    /**
     * The last commit record for which an index was returned.
     */
    private ICommitRecord commitRecord;
    
    /**
     * The last index returned.
     */
    private IIndex index;
    
    public ReadCommittedIndex(AbstractJournal journal, String name) {
    
        if (journal == null)
            throw new IllegalArgumentException();

        if (name == null)
            throw new IllegalArgumentException();
        
        this.journal = journal;
        
        this.name = name;
        
        this.index = getIndex();
        
    }

    /**
     * Return the current read-committed view of the named index.
     * <p>
     * Note: This is <code>synchronized</code> so that the operation will be
     * atomic (with respect to the callers) if there are multiple tasks running
     * for the same read-committed index.
     * 
     * @return The read-committed index view.
     * 
     * @exception IllegalStateException
     *                if the named index is not registered.
     */
    synchronized protected IIndex getIndex() {
        
        /*
         * Obtain the most current {@link ICommitRecord} on the journal. All
         * read operations are against the named index as resolved using
         * this commit record.
         */

        ICommitRecord currentCommitRecord = journal.getCommitRecord();
        
        if (commitRecord != null
                && index != null
                && commitRecord.getCommitCounter() == currentCommitRecord
                        .getCommitCounter()) {

            /*
             * the commit record has not changed so we have the correct
             * index view.
             */
            return index;
            
        }
        
        // update the commit record.
        this.commitRecord = currentCommitRecord;
        
        /*
         * Lookup the current committed index view against that commit
         * record.
         */
        this.index = journal.getIndex(name, commitRecord);
        
        if(index == null) {
            
            throw new IllegalStateException("Index not defined: "+name);
            
        }

        if (index.getIndexMetadata().getPartitionMetadata() != null) {

            /*
             * FIXME The view is a view of multiple index resources. Use the
             * same code that is used to create such views elsewhere (currently
             * on the journal) to prevent double-opening of btrees or index
             * segments.
             */

            throw new UnsupportedOperationException();
            
//            return new ReadOnlyFusedView(pmd);

        }

        return index;

    }
    
    final public UUID getIndexUUID() {

        return getIndex().getIndexMetadata().getIndexUUID();
        
    }

    final public IndexMetadata getIndexMetadata() {
        
        return getIndex().getIndexMetadata();
        
    }
    
    final public String getStatistics() {
        
        return getClass().getSimpleName() + " : "+ getIndex().getStatistics();
        
    }

    final public boolean contains(byte[] key) {
        
        return getIndex().contains(key);
        
    }

    final public byte[] lookup(byte[] key) {
        
        return getIndex().lookup(key);
        
    }

    /**
     * @exception UnsupportedOperationException always.
     */
    final public byte[] insert(byte[] key, byte[] value) {

        throw new UnsupportedOperationException();
        
    }

    /**
     * @exception UnsupportedOperationException always.
     */
    final public byte[] remove(byte[] key) {
        
        throw new UnsupportedOperationException();
        
    }

    final public long rangeCount(byte[] fromKey, byte[] toKey) {
        
        return getIndex().rangeCount(fromKey, toKey);
        
    }

    final public IEntryIterator rangeIterator(byte[] fromKey, byte[] toKey) {

        return getIndex().rangeIterator(fromKey, toKey, 0/* capacity */,
                KEYS|VALS/* flags */, null/* filter */);
        
    }

    final public IEntryIterator rangeIterator(byte[] fromKey, byte[] toKey,
            int capacity, int flags, IEntryFilter filter) {
        
        return getIndex()
                .rangeIterator(fromKey, toKey, capacity, flags, filter);
        
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

        Object result = ctor.newInstance(n, 0/* offset */, keys, vals).apply(getIndex());
        
        aggregator.aggregate(result, new Split(null,0,n));

    }

    final public ICounter getCounter() {
        return new ReadOnlyCounter(getIndex().getCounter());
    }

    /**
     * Note: The definition of this view can evolve since it is not based
     * on a fixed time but on the last committed state of the index.
     */
    public IResourceMetadata[] getResourceMetadata() {

        return getIndex().getResourceMetadata();

    }

}
