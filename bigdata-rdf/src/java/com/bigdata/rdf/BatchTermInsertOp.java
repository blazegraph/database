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
 * Created on Mar 2, 2007
 */

package com.bigdata.rdf;

import java.util.Arrays;

import com.bigdata.btree.Errors;
import com.bigdata.btree.IBatchOp;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ISimpleBTree;
import com.bigdata.rdf.model.OptimizedValueFactory.TermIdComparator;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;
import com.bigdata.rdf.model.OptimizedValueFactory._ValueSortKeyComparator;

/**
 * A batch operation for loading data into the terms and ids indices. This
 * operation is designed to write on <em>unisolated</em> indices - that is,
 * outside of any transactional isolation. This strategy relies on the fact that
 * (a) the terms index is the sole source of term ids; and (b) the batch api
 * provides an atomic guarentee for operations on a single index partition.
 * Accordingly, this batch operation takes responsibility for breaking down the
 * terms according to the then current index partitions.
 * <p>
 * If a term is not found in the term index, then it is synchronously inserted.
 * The insert operation defines the identifier. This occurs during a
 * single-threaded write on the unisolated terms index. In practice, we lookup
 * and, if lookup fails, define, the term identifier for all terms that would be
 * found in a single index partition at once. Batch operations for terms that
 * would be found in different index partitions may be executed in parallel. If
 * the operation fails for any single partition, then it may proceed for all
 * remaining partitions or abort eagerly.
 * <p>
 * If a term identifier is NOT found, then the operation caused the term
 * identifier to be assigned and must record the id:term mapping in the ids
 * index. Since the writes on the ids index will be on different partitions and,
 * more likely than not, different hosts, we can not obtain a simple guarentee
 * of atomicity across writes on both the terms and the ids index. Therefore, in
 * order to guard against failures or concurrent inserts, we may be required to
 * conditionally insert terms in to the ids index regardless of whether they are
 * found on the terms index since we could not otherwise guarentee that the
 * write on the terms index partition and the corresponding (scattered) writes
 * on the ids index partition would be atomic as a unit.
 * <p>
 * We optimize the scattered writes on the ids index by sorting by the term
 * identifier and scattering ordered writes against the various partitions of
 * the ids index.
 * 
 * @todo If the batch term insert operation were done transactionally then we
 *       could guarentee atomicity across the writes on the terms and ids
 *       indices.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class BatchTermInsertOp implements IBatchOp {

//    /**
//     * The #of tuples to be processed.
//     */
//    public final int ntuples;
//    
//    /**
//     * The keys for each tuple.
//     */
//    public final byte[][] keys;
//    
//    /**
//     * The value corresponding to each key.
//     */
//    public final boolean[] contains;
//    
//    /**
//     * The index of the tuple that is currently being processed.
//     */
//    public int tupleIndex = 0;
//    
//    /**
//     * Create a batch operation to lookup/define terms.
//     * 
//     * @param ntuples
//     *            The #of tuples in the operation (in).
//     * 
//     * @param keys
//     *            A series of keys paired to values (in). Each key is an
//     *            variable length unsigned byte[]. The keys must be presented in
//     *            sorted order in order to obtain maximum efficiency for the
//     *            batch operation.
//     * 
//     * @param contains
//     *            An array of boolean flags, one per tuple (in,out). On input,
//     *            the tuple will be tested iff the corresponding element is
//     *            <code>false</code> (this supports chaining of this operation
//     *            on a view over multiple btrees). On output, the array element
//     *            corresponding to a tuple will be true iff the key exists.
//     * 
//     * @exception IllegalArgumentException
//     *                if the dimensions of the arrays are not sufficient for the
//     *                #of tuples declared.
//     */
//    public BatchTermInsertOp(int ntuples, byte[][] keys, boolean[] contains) {
//        
//        if (ntuples <= 0)
//            throw new IllegalArgumentException(Errors.ERR_NTUPLES_NON_POSITIVE);
//
//        if (keys == null)
//            throw new IllegalArgumentException(Errors.ERR_KEYS_NULL);
//
//        if (keys.length < ntuples)
//            throw new IllegalArgumentException(Errors.ERR_NOT_ENOUGH_KEYS);
//
//        if (contains == null)
//            throw new IllegalArgumentException(Errors.ERR_VALS_NULL);
//
//        if (contains.length < ntuples)
//            throw new IllegalArgumentException(Errors.ERR_NOT_ENOUGH_VALS);
//
//        this.ntuples = ntuples;
//        this.keys = keys;
//        this.contains = contains;
//
//    }
//
//    /**
//     * Applies the operation.
//     * 
//     * @param btree
//     */
//    public void apply(ISimpleBTree btree) {
//        
//        while( tupleIndex < ntuples ) {
//
//            // skip tuples already marked as true.
//            if (contains[tupleIndex]) {
//
//                tupleIndex++;
//
//                continue;
//
//            }
//
//            contains[tupleIndex] = btree.contains(keys[tupleIndex]);
//
//            tupleIndex ++;
//
//        }
//
//    }
//    
//    /**
//     * Batch insert of terms into the database.
//     * 
//     * @param terms
//     *            An array whose elements [0:nterms-1] will be inserted.
//     * @param numTerms
//     *            The #of terms to insert.
//     * @param haveKeys
//     *            True if the terms already have their sort keys.
//     * @param sorted
//     *            True if the terms are already sorted by their sort keys (in
//     *            the correct order for a batch insert).
//     * 
//     * @exception IllegalArgumentException
//     *                if <code>!haveKeys && sorted</code>.
//     */
//    public BatchTermInsertOp( _Value[] terms, int numTerms, boolean haveKeys, boolean sorted,
//            RdfKeyBuilder keyBuilder ) {
//
//        if (numTerms == 0)
//            return;
//
//        if (!haveKeys && sorted)
//            throw new IllegalArgumentException("sorted requires keys");
//        
//        long begin = System.currentTimeMillis();
//        long keyGenTime = 0; // time to convert unicode terms to byte[] sort keys.
//        long sortTime = 0; // time to sort terms by assigned byte[] keys.
//        long insertTime = 0; // time to insert terms into the forward and reverse index.
//        
//        System.err.print("Writing "+numTerms+" terms ("+terms.getClass().getSimpleName()+")...");
//
//        {
//
//            /*
//             * First make sure that each term has an assigned sort key.
//             */
//            if(!haveKeys) {
//
//                long _begin = System.currentTimeMillis();
//                
//                generateSortKeys(keyBuilder, terms, numTerms);
//                
//                keyGenTime = System.currentTimeMillis() - _begin;
//
//            }
//            
//            /* 
//             * Sort terms by their assigned sort key.  This places them into
//             * the natural order for the term:id index.
//             */
//
//            if (!sorted ) {
//            
//                long _begin = System.currentTimeMillis();
//                
//                Arrays.sort(terms, 0, numTerms, _ValueSortKeyComparator.INSTANCE);
//
//                sortTime += System.currentTimeMillis() - _begin;
//                
//            }
//
//            {
//            
//                /*
//                 * Lookup the term in the term:id index. If it is there then
//                 * take its termId and mark it as 'known' so that we can avoid
//                 * testing the reverse index. Otherwise, insert the term into
//                 * the term:id index which gives us its termId.
//                 * 
//                 * @todo use batch api?
//                 */
//
//                IIndex termId = getTermIdIndex();
//                AutoIncCounter counter = getCounter();
//                
//                long _begin = System.currentTimeMillis();
//                
//                for (int i = 0; i < numTerms; i++) {
//
//                    _Value term = terms[i];
//
//                    if (!term.known) {
//
//                        //assert term.termId==0L; FIXME uncomment this and figure out why this assertion is failing.
//                        assert term.key != null;
//
//                        // Lookup in the forward index.
//                        Long tmp = (Long)termId.lookup(term.key);
//                        
//                        if(tmp == null) { // not found.
//
//                            // assign termId.
//                            term.termId = counter.nextId();
//                        
//                            // insert into index.
//                            if(termId.insert(term.key, Long.valueOf(term.termId))!=null) {
//                                
//                                throw new AssertionError();
//                                
//                            }
//                            
//                        } else { // found.
//                        
//                            term.termId = tmp.longValue();
//                            
//                            term.known = true;
//                        
//                        }
//                        
//                    } else assert term.termId != 0L;
//                    
//                }
//
//                insertTime += System.currentTimeMillis() - _begin;
//                
//            }
//            
//        }
//        
//        {
//            
//            /*
//             * Sort terms based on their assigned termId.
//             * 
//             * FIXME The termId should be converted to a byte[8] for this sort
//             * order since that is how the termId is actually encoded when we
//             * insert into the termId:term (reverse) index.  We could also 
//             * fake this with a comparator that compared the termIds as if they
//             * were _unsigned_ long integers.
//             */
//
//            long _begin = System.currentTimeMillis();
//
//            Arrays.sort(terms, 0, numTerms, TermIdComparator.INSTANCE);
//
//            sortTime += System.currentTimeMillis() - _begin;
//
//        }
//        
//        {
//        
//            /*
//             * Add unknown terms to the reverse index, which is the index that
//             * we use to lookup the RDF value by its termId to serialize some
//             * data as RDF/XML or the like.
//             * 
//             * Note: We only insert terms that were reported as "not found" when
//             * we inserted them into the forward mapping. This reduces the #of
//             * index tests that we perform.
//             * 
//             * @todo use batch api?
//             */
//            
//            IIndex idTerm = getIdTermIndex();
//            
//            long _begin = System.currentTimeMillis();
//            
//            for (int i = 0; i < numTerms; i++) {
//
//                _Value term = terms[i];
//                
//                assert term.termId != 0L;
//                
//                if (!term.known) {
//                    
//                    final byte[] idKey = keyBuilder.id2key(term.termId);
//
//                    if(idTerm.insert(idKey, term) != null) {
//
//                        // term was already in this index.
//                        
//                        throw new AssertionError();
//                        
//                    }
//                    
//                    term.known = true; // now in the fwd and rev indices.
//                    
//                }
//
//            }
//
//            insertTime += System.currentTimeMillis() - _begin;
//
//        }
//
//        long elapsed = System.currentTimeMillis() - begin;
//        
//        System.err.println("in " + elapsed + "ms; keygen=" + keyGenTime
//                + "ms, sort=" + sortTime + "ms, insert=" + insertTime + "ms");
//        
//    }

}
