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

import com.bigdata.btree.BatchContains;
import com.bigdata.btree.BatchInsert;
import com.bigdata.btree.BatchLookup;
import com.bigdata.btree.BatchRemove;
import com.bigdata.btree.ICounter;
import com.bigdata.btree.IEntryIterator;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IIndexWithCounter;
import com.bigdata.btree.ReadOnlyCounter;

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
public class ReadCommittedIndex implements IIndexWithCounter {

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
        
        return index;
        
    }
    
    public UUID getIndexUUID() {
        return getIndex().getIndexUUID();
    }
    
    public boolean contains(byte[] key) {
        return getIndex().contains(key);
    }

    /**
     * @exception UnsupportedOperationException always.
     */
    public Object insert(Object key, Object value) {
        throw new UnsupportedOperationException();
    }

    public Object lookup(Object key) {
        return getIndex().lookup(key);
    }

    /**
     * @exception UnsupportedOperationException always.
     */
    public Object remove(Object key) {
        throw new UnsupportedOperationException();
    }

    public int rangeCount(byte[] fromKey, byte[] toKey) {
        return getIndex().rangeCount(fromKey, toKey);
    }

    public IEntryIterator rangeIterator(byte[] fromKey, byte[] toKey) {
        return getIndex().rangeIterator(fromKey, toKey);
    }

    public void contains(BatchContains op) {
        getIndex().contains(op);
    }

    /**
     * @exception UnsupportedOperationException always.
     */
    public void insert(BatchInsert op) {
        throw new UnsupportedOperationException();
    }

    public void lookup(BatchLookup op) {
        getIndex().lookup(op);
    }

    /**
     * @exception UnsupportedOperationException always.
     */
    public void remove(BatchRemove op) {
        throw new UnsupportedOperationException();
    }

    public ICounter getCounter() {
        
        IIndex src = getIndex();
        
        if(src instanceof IIndexWithCounter) {
        
            IIndexWithCounter ndx = (IIndexWithCounter) src;
            
            return new ReadOnlyCounter(ndx.getCounter());
            
        } else {
            
            throw new UnsupportedOperationException();
            
        }
        
    }

}
