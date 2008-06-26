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
 * Created on Apr 8, 2008
 */

package com.bigdata.rdf.store;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.rdf.spo.ChunkedIterator;
import com.bigdata.rdf.spo.ISPOFilter;
import com.bigdata.rdf.spo.ISPOIterator;
import com.bigdata.rdf.spo.SPOArrayIterator;
import com.bigdata.rdf.spo.SPOIterator;
import com.bigdata.rdf.util.KeyOrder;
import com.bigdata.rdf.util.RdfKeyBuilder;

/**
 * Basic implementation resolves indices dynamically against the outer
 * class.
 * 
 * @see AbstractTripleStore#getAccessPath(KeyOrder)
 * @see AbstractTripleStore#getAccessPath(long, long, long)
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AccessPath implements IAccessPath {

    final public Logger log = Logger.getLogger(IAccessPath.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final public boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final public boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();

    /** the backing database (from the ctor). */
    final AbstractTripleStore db;
    
    /** the key order identifying the access path (from the ctor). */
    final KeyOrder keyOrder;

    /** The triple pattern (from the ctor). */
    final long s, p, o;
    
    final byte[] fromKey;
    
    final byte[] toKey;

    /** true iff the triple pattern is fully bound. */
    final boolean allBound;
         
    public long[] getTriplePattern() {
        
        return new long[]{s,p,o};
        
    }
    
    private IIndex getStatementIndex() {
        
        return db.getStatementIndex( keyOrder );
        
    }

    public KeyOrder getKeyOrder() {

        return keyOrder;
        
    }

    public boolean isEmpty() {

        /*
         * Note: empty iff the iterator can not visit anything.
         */
        
        return !iterator(1/* limit */, 1/* capacity */).hasNext();
        
    }
    
    public long rangeCount() {
        
        return getStatementIndex().rangeCount(fromKey,toKey);
        
    }

    public ITupleIterator rangeQuery() {
        
        return getStatementIndex().rangeIterator(fromKey, toKey);
        
    }

    public ISPOIterator iterator() {
        
        return iterator(null/*filter*/);
        
    }

    public ISPOIterator iterator(ISPOFilter filter) {

        if (allBound) {

            // Optimization for point test.

            return new SPOArrayIterator(db, this, 1/* limit */, filter);

        }

        if (db.isConcurrent()) {

            /*
             * This is an async incremental iterator that buffers some but not
             * necessarily all statements.
             */
            
            return iterator(0/* limit */, 0/* capacity */, filter);

        } else {
            
            /*
             * This is a synchronous read that buffers all statements.
             * 
             * Note: This limits the capacity of index scans to the
             * available memory. This is primarily a problem during
             * inference, where it imposes a clear upper bound on the size
             * of the store whose closure can be computed. It is also a
             * problem in high level query if any access path is relatively
             * unselective, e.g., 1-bound.
             */

            return new SPOArrayIterator(db, this, 0/* no limit */,
                    filter);

        }

    }

    /**
     * Note: Return an iterator that will use transparent read-ahead when no
     * limit is specified (limit is zero) or the limit is "small".
     * 
     * @see SPOIterator
     */
    public ISPOIterator iterator(int limit, int capacity) {

        return iterator(limit, capacity, null/*filter*/);
        
    }

    /**
     * Note: Return an iterator that will use transparent read-ahead when no
     * limit is specified (limit is zero) or the limit is "small".
     * 
     * @see SPOIterator
     */
    public ISPOIterator iterator(int limit, int capacity, ISPOFilter filter) {

        if (allBound) {

            // Optimization for point test.

            return new SPOArrayIterator(db, this, 1/* limit */, filter);

        }

        if (limit > 0 && limit < 100) {

            /*
             * Use a light-weight synchronous fully buffered variant when
             * the limit is small, especially when all that you are doing is
             * an existence test (limit := 1).
             */

            return new SPOArrayIterator(db, this, limit, filter);

        }

        boolean async = true;

        return new SPOIterator(this, limit, capacity, async, filter);

    }
    
    /**
     * Chooses the best access path for the given triple pattern.
     * 
     * @param db The database.
     * 
     * @param keyOrder
     * 
     * @param s
     *            The term identifier for the subject -or-
     *            {@link IRawTripleStore#NULL}.
     * @param p
     *            The term identifier for the predicate -or-
     *            {@link IRawTripleStore#NULL}.
     * @param o
     *            The term identifier for the object -or-
     *            {@link IRawTripleStore#NULL}.
     */
    AccessPath(final AbstractTripleStore db, final KeyOrder keyOrder, long s, long p, long o) {

        if (db == null)
            throw new IllegalArgumentException();
        
        if (keyOrder == null)
            throw new IllegalArgumentException();
        
        this.db = db;
        
        this.keyOrder = keyOrder;
        
        this.s = s;
        
        this.p = p;
        
        this.o = o;
        
        this.allBound = (s != NULL && p != NULL & o != NULL);

        // thread-local instance.
        final RdfKeyBuilder keyBuilder = db.getKeyBuilder();

        if (s != NULL && p != NULL && o != NULL) {
    
            assert keyOrder == KeyOrder.SPO;
            
            fromKey = keyBuilder.statement2Key(s, p, o);

            toKey = keyBuilder.statement2Key(s, p, o + 1);

        } else if (s != NULL && p != NULL) {

            assert keyOrder == KeyOrder.SPO;
            
            fromKey = keyBuilder.statement2Key(s, p, NULL);

            toKey = keyBuilder.statement2Key(s, p + 1, NULL);

        } else if (s != NULL && o != NULL) {

            assert keyOrder == KeyOrder.OSP;
            
            fromKey = keyBuilder.statement2Key(o, s, NULL);

            toKey = keyBuilder.statement2Key(o, s + 1, NULL);

        } else if (p != NULL && o != NULL) {

            assert keyOrder == KeyOrder.POS;
            
            fromKey = keyBuilder.statement2Key(p, o, NULL);

            toKey = keyBuilder.statement2Key(p, o + 1, NULL);

        } else if (s != NULL) {

            assert keyOrder == KeyOrder.SPO;
            
            fromKey = keyBuilder.statement2Key(s, NULL, NULL);

            toKey = keyBuilder.statement2Key(s + 1, NULL, NULL);

        } else if (p != NULL) {

            assert keyOrder == KeyOrder.POS;
            
            fromKey = keyBuilder.statement2Key(p, NULL, NULL);

            toKey = keyBuilder.statement2Key(p + 1, NULL, NULL);

        } else if (o != NULL) {

            assert keyOrder == KeyOrder.OSP;
            
            fromKey = keyBuilder.statement2Key(o, NULL, NULL);

            toKey = keyBuilder.statement2Key(o + 1, NULL, NULL);

        } else {

            /*
             * Note: The KeyOrder does not matter when you are fully
             * unbound.
             */
            
            fromKey = toKey = null;

        }

    }

    /**
     * Representation of the state for the access path (key order, triple
     * pattern, and from/to keys).
     */
    public String toString() {
        
        return super.toString() + ": " + keyOrder + ", {" + s + "," + p + ","
                + o + "}, fromKey="
                + (fromKey == null ? "n/a" : BytesUtil.toString(fromKey))
                + ", toKey="
                + (toKey == null ? "n/a" : BytesUtil.toString(toKey));
        
    }
    
    public int removeAll() {

        return removeAll(null/* filter */);

    }

    public int removeAll(ISPOFilter filter) {

        return db.removeStatements(iterator(filter));

    }

    /**
     * The implementation uses a key scan to find the first term identifer for
     * the given index. It then forms a fromKey that starts at the next possible
     * term identifier and does another scan, thereby obtaining the 2nd distinct
     * term identifier for that position on that index. This process is repeated
     * iteratively until the key scan no longer identifies a match. This
     * approach skips quickly over regions of the index which have many
     * statements for the same term and makes N+1 queries to identify N distinct
     * terms. Note that there is no way to pre-compute the #of distinct terms
     * that will be identified short of running the queries.
     * 
     * @see ChunkedIterator
     * 
     * @todo This will need to be modified to return a chunked iterator that
     *       encapsulates the logic so that the distinct term scan may be
     *       applied when very large #s of terms would be visited.
     *       <p>
     *       If the indices are range partitioned and the iterator only
     *       guarentee "distinct" (and not also ordered) then those steps can be
     *       parallelized. The only possibility for conflict is when the last
     *       distinct term identifier is read from one index before the right
     *       sibling index partition has reported its first distinct term
     *       identifier. We could withhold the first result from each partition
     *       until the partition that proceeds it in the metadata index has
     *       completed, which would give nearly full parallelism.
     *       <p>
     *       If the indices are range partitioned and distinct + ordered is
     *       required, then the operation can not be parallelized, or if it is
     *       parallelized then a merge sort must be done before returning the
     *       first result.
     *       <p>
     *       Likewise, if the indices are hash partitioned, then we can do
     *       parallel index scans and a merge sort but the caller will have to
     *       wait for the merge sort to complete before obtaining the 1st
     *       result.
     */
    public Iterator<Long> distinctTermScan() {

        final int capacity = 10000;
        
        final ArrayList<Long> ids = new ArrayList<Long>(capacity);
        
        byte[] fromKey = null;
        
        final byte[] toKey = null;
        
        final IIndex ndx = getStatementIndex();
        
        ITupleIterator itr = ndx.rangeIterator(fromKey, toKey, capacity,
                IRangeQuery.KEYS, null/* filter */);
        
//        long[] tmp = new long[IRawTripleStore.N];
        
        // thread-local instance.
        final RdfKeyBuilder keyBuilder = db.getKeyBuilder();
        
        while(itr.hasNext()) {
            
            ITuple tuple = itr.next();
            
            // clone of the key.
//            final byte[] key = itr.getKey();
            
            // copy of the key in a reused buffer.
//            final byte[] key = tuple.getKeyBuffer().array();
            
            // extract the term ids from the key. 
//            RdfKeyBuilder.key2Statement( key , tmp);
//            
//            final long id = tmp[0];
            
            final long id = KeyBuilder.decodeLong( tuple.getKeyBuffer().array(), 0);
            
            // append tmp[0] to the output list.
            ids.add(id);

//            log.debug(ids.size() + " : " + id + " : "+ toString(id));
            
            // restart scan at the next possible term id.

            final long nextId = id + 1;
            
            fromKey = keyBuilder.statement2Key(nextId, NULL, NULL);
            
            // new iterator.
            itr = ndx.rangeIterator(fromKey, toKey, capacity,
                    IRangeQuery.KEYS, null/* filter */);
            
        }
        
//        log.debug("Distinct key scan: KeyOrder=" + keyOrder + ", #terms=" + ids.size());
        
        return ids.iterator();
        
    }

}
