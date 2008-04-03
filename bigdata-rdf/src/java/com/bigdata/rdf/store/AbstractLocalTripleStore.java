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
 * Created on May 21, 2007
 */

package com.bigdata.rdf.store;

import java.util.Properties;

import com.bigdata.btree.IIndex;
import com.bigdata.search.FullTextIndex;
import com.bigdata.service.IBigdataClient;

/**
 * Abstract base class for both transient and persistent {@link ITripleStore}
 * implementations using local storage.
 * <p>
 * This implementation presumes unisolated writes on local indices and a single
 * client writing on a local database. Unlike the {@link ScaleOutTripleStore}
 * this implementation does NOT feature auto-commit for unisolated writes. The
 * implication of this is that the client controls the commit points which means
 * that it is easier to guarentee that the KB is fully consistent since partial
 * writes can be abandoned.
 * 
 * @deprecated Is this class still required?
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractLocalTripleStore extends AbstractTripleStore {

    public AbstractLocalTripleStore(Properties properties) {
        
        super(properties);
        
    }

    /**
     * FIXME Not supported since the search engine is accepting an
     * {@link IBigdataClient} rather than one or more {@link IIndex}s.
     */
    public FullTextIndex getSearchEngine() {
        
        return null;
        
    }
    
//    /**
//     * 
//     * @todo This could use two threads to write on the indices if we divide the
//     *       work into batches. The first thread would write on the term:id
//     *       index, placing a batch on a queue every N terms. The second thread
//     *       would read batches from the queue, re-order them for the id:term
//     *       index, and then write on the id:term index.
//     *       <p>
//     *       Note: For different values of N this might or might not be faster
//     *       since it does not perform sustained ordered writes on both indices
//     *       but it does allow two threads to operation concurrently writing on
//     *       each index and should reduce the total latency of this operation.
//     *       <p>
//     *       The {@link ScaleOutTripleStore} is already required to partition
//     *       its writes, but {@link ClientIndexView} can add transparent
//     *       parallelism in that case.
//     */
//    public void addTerms( RdfKeyBuilder keyBuilder, _Value[] terms, int numTerms) {
//
//        if (numTerms == 0)
//            return;
//
//        long begin = System.currentTimeMillis();
//        long keyGenTime = 0; // time to convert unicode terms to byte[] sort keys.
//        long sortTime = 0; // time to sort terms by assigned byte[] keys.
//        long insertTime = 0; // time to insert terms into the forward and reverse index.
//
//        {
//
//            /*
//             * First make sure that each term has an assigned sort key.
//             */
//            {
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
//            {
//            
//                long _begin = System.currentTimeMillis();
//                
//                Arrays.sort(terms, 0, numTerms, _ValueSortKeyComparator.INSTANCE);
//
//                sortTime += System.currentTimeMillis() - _begin;
//                
//            }
//
//            /*
//             * Insert terms into the foward index.
//             */
//            {
//
//                final long _begin = System.currentTimeMillis();
//                
//                /*
//                 * Lookup the term in the term:id index. If it is there then
//                 * take its termId and mark it as 'known' so that we can avoid
//                 * testing the reverse index. Otherwise, insert the term into
//                 * the term:id index which gives us its termId.
//                 */
//
//                IIndexWithCounter termId = (IIndexWithCounter)getTermIdIndex();
//
//                final boolean isolatableIndex = termId.isIsolatable();
//                
//                ICounter counter = termId.getCounter();
//                
//                if(counter.get()==IRawTripleStore.NULL) {
//                    
//                    // Never assign NULL as a term identifier!
//                    counter.inc();
//                    
//                }
//                
//                for (int i = 0; i < numTerms; i++) {
//
//                    _Value term = terms[i];
//
//                    if (term.termId!=IRawTripleStore.NULL) {
//                        /*
//                         * The termId is already assigned.
//                         * 
//                         * Note: among other things, this happens when there are
//                         * duplicate references in the terms[].
//                         */
//                        continue;
//                    }
//                    
//                    if (!term.known) {
//
//                        assert term.termId==IRawTripleStore.NULL;
//                        assert term.key != null;
//
//                        // Lookup in the forward index.
//                        Object tmp = termId.lookup(term.key);
//                        
//                        if(tmp == null) { // not found.
//
//                            /*
//                             * Assign the termId.
//                             * 
//                             * Note: We set the low two bits to indicate whether
//                             * a term is a literal, bnode, URI, or statement so
//                             * that we can tell at a glance (without lookup up
//                             * the term in the index) what "kind" of thing the
//                             * term identifier stands for.
//                             * 
//                             * @todo we could use negative term identifiers
//                             * except that we pack the termId in a manner that
//                             * does not allow negative integers. a different
//                             * pack routine would allow us all bits.
//                             */
//                            term.termId = counter.inc()<<2;
//                            
//                            if(term instanceof Literal) {
//                                
//                                term.termId |= TERMID_CODE_LITERAL;
//                                
//                            } else if(term instanceof URI) {
//                                
//                                term.termId |= TERMID_CODE_URI;
//                                
//                            } else if(term instanceof BNode) {
//                                
//                                term.termId |= TERMID_CODE_BNODE;
//                                
//                            } else {
//                                
//                                throw new AssertionError(
//                                        "Unknown term class: class="
//                                                + term.getClass());
//                                
//                            }
//
//                            /*
//                             * Insert into forward mapping from serialized term to packed term
//                             * identifier.
//                             */
//                            if(isolatableIndex) {
//                               
//                                // used to serialize term identifers.
//                                final DataOutputBuffer idbuf = new DataOutputBuffer(
//                                        Bytes.SIZEOF_LONG);
//
//                                // format the term identifier as a packed long integer.
//                                try {
//                                
//                                    idbuf.reset().packLong(term.termId);
//                                    
//                                } catch (IOException ex) {
//                                    
//                                    throw new RuntimeException(ex);
//                                    
//                                }
//
//                                if (termId.insert(term.key, idbuf.toByteArray()) != null) {
//
//                                    throw new AssertionError();
//
//                                }
//                                
//                            } else {
//
//                                if (termId.insert(term.key, Long.valueOf(term.termId)) != null) {
//
//                                    throw new AssertionError();
//
//                                }
//
//                            }
//                            
//                        } else { // found.
//                        
//                            if (isolatableIndex) {
//                                
//                                try {
//
//                                    term.termId = new DataInputBuffer((byte[]) tmp).unpackLong();
//
//                                } catch (IOException ex) {
//
//                                    throw new RuntimeException(ex);
//
//                                }
//                            } else {
//
//                                term.termId = (Long)tmp;
//                                
//                            }
//
//                            // the term was found in the forward index.
//                            term.known = true;
//                        
//                        }
//                        
//                    } else assert term.termId != IRawTripleStore.NULL;
//                    
//                }
//
//                insertTime += System.currentTimeMillis() - _begin;
//                
//            }
//            
//        }
//                
//        /*
//         * Sort terms based on their assigned termId.
//         */
//        {
//
//            long _begin = System.currentTimeMillis();
//
//            Arrays.sort(terms, 0, numTerms, TermIdComparator.INSTANCE);
//
//            sortTime += System.currentTimeMillis() - _begin;
//
//        }
//        
//        /*
//         * Insert terms into the reverse index.
//         */
//        {
//
//            long _begin = System.currentTimeMillis();
//          
//            /*
//             * Add unknown terms to the reverse index, which is the index that
//             * we use to lookup the RDF value by its termId to serialize some
//             * data as RDF/XML or the like.
//             * 
//             * Note: We only insert terms that were reported as "not found" when
//             * we inserted them into the forward mapping. This reduces the #of
//             * index tests that we perform.
//             */
//            
//            IIndex idTerm = getIdTermIndex();
//            
//            final boolean isolatableIndex2 = idTerm.isIsolatable();
//            
//            // reused for all terms serialized.
//            DataOutputBuffer out = new DataOutputBuffer();
//            
//            for (int i = 0; i < numTerms; i++) {
//
//                _Value term = terms[i];
//                
//                assert term.termId != IRawTripleStore.NULL;
//                
//                if (!term.known) {
//                    
//                    /*
//                     * Insert into the reverse mapping from the term identifier
//                     * to serialized term.
//                     * 
//                     * Note: if there are duplicate references in terms[] then
//                     * [term.known] gets set for the first reference and the
//                     * remaining references get skipped.
//                     */
//
//                    // form the key from the term identifier.
//                    final byte[] idKey = keyBuilder.id2key(term.termId);
//
//                    // insert the serialized term under that key.
//                    if (idTerm.insert(idKey, (isolatableIndex2 ? term
//                            .serialize(out.reset()) : term)) != null) {
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
//        if (numTerms > 1000 || elapsed > 3000) {
//
//            log.info("Wrote " + numTerms + " in " + elapsed + "ms; keygen="
//                    + keyGenTime + "ms, sort=" + sortTime + "ms, insert="
//                    + insertTime + "ms");
//            
//        }
//
//        if(textIndex) {
//
//            /*
//             * Populate the free text index from the terms when that feature is
//             * enabled.
//             */
//            
//            indexTermText( terms, numTerms );
//            
//        }
//        
//    }

}
