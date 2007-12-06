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

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import org.openrdf.model.Literal;
import org.openrdf.model.Value;

import com.bigdata.btree.ICounter;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IIndexWithCounter;
import com.bigdata.cache.LRUCache;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.model.OptimizedValueFactory;
import com.bigdata.rdf.model.OptimizedValueFactory.TermIdComparator;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;
import com.bigdata.rdf.model.OptimizedValueFactory._ValueSortKeyComparator;
import com.bigdata.service.ClientIndexView;

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
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractLocalTripleStore extends AbstractTripleStore {

    public AbstractLocalTripleStore(Properties properties) {
        
        super(properties);
        
    }

    /**
     * Handles both unisolatable and isolatable indices.
     */
    public long addTerm(Value value) {

        final _Value val = OptimizedValueFactory.INSTANCE.toNativeValue(value);
        
        if(val.known) {
            
            assert val.termId != IRawTripleStore.NULL;
            
            return val.termId;

        }

        /*
         * The forward mapping assigns the identifier.
         * 
         * Note: this code tests for existance based on the foward mapping so
         * that we can avoid the use of the reverse mapping if we know that the
         * term exists.
         */

        final IIndexWithCounter terms = (IIndexWithCounter) getTermIdIndex();

        final boolean isolatableIndex = terms.isIsolatable();
        
        // formulate key from the RDF value.
        final byte[] termKey = keyBuilder.value2Key(val);

        // Lookup in the forward index.
        final Object tmp = terms.lookup(termKey);

        if (tmp == null) { // not found.

            final ICounter counter = terms.getCounter();
            
            if(counter.get()==IRawTripleStore.NULL) {
                
                // Never assign NULL as a term identifier!
                counter.inc();
                
            }

            /*
             * Assign the termId.
             * 
             * Note: We set the low bit iff the term is a
             * literals so that we can tell at a glance whether
             * a term identifier is a literal or not.
             * 
             * FIXME back port to the scale-out version as well.
             * 
             * @todo we could use negative term identifiers
             * except that we pack the termId in a manner that
             * does not allow negative integers. a different
             * pack routine would allow us all bits.
             */
            val.termId = counter.inc()<<1;
            
            if(val instanceof Literal) {
                
                val.termId |= 0x01L;
                
            }

            /*
             * Insert into forward mapping from serialized term to packed term
             * identifier.
             */
            if(isolatableIndex) {
               
                // used to serialize term identifers.
                final DataOutputBuffer idbuf = new DataOutputBuffer(
                        Bytes.SIZEOF_LONG);

                // format the term identifier as a packed long integer.
                try {
                
                    idbuf.reset().packLong(val.termId);
                    
                } catch (IOException ex) {
                    
                    throw new RuntimeException(ex);
                    
                }

                if (terms.insert(termKey, idbuf.toByteArray()) != null) {

                    throw new AssertionError();

                }
                
            } else {

                if (terms.insert(termKey, new Long(val.termId)) != null) {

                    throw new AssertionError();

                }

            }

            /*
             * Insert into the reverse mapping from the term identifier to
             * serialized term.
             */
            {

                final IIndex ids = getIdTermIndex();

                final boolean isolatableIndex2 = ids.isIsolatable();
                
                // form the key from the term identifier.
                final byte[] idKey = keyBuilder.id2key(val.termId);

                // insert the serialized term under that key.
                if (ids.insert(idKey, (isolatableIndex2?val.serialize():val)) != null) {

                    throw new AssertionError();

                }
                
            }

        } else { // found.

            /*
             * the term was found on the forward lookup, so we are done.
             */

            if (isolatableIndex) {
                
                try {

                    val.termId = new DataInputBuffer((byte[]) tmp).unpackLong();

                } catch (IOException ex) {

                    throw new RuntimeException(ex);

                }
                
            } else {

                val.termId = (Long) tmp;

            }

        }

        val.known = true;
        
        return val.termId;

    }

    /**
     * Note: Handles both unisolatable and isolatable indices.
     * <P>
     * Note: Sets {@link _Value#termId} and {@link _Value#known} as
     * side-effects.
     */
    final public _Value getTerm(long id) {

        _Value value = null;
        
        value = termCache.get(id);
        
        if(value!=null) return value;
        
        IIndex ndx = getIdTermIndex();
        
        final boolean isolatableIndex = ndx.isIsolatable();
        
        Object data = ndx.lookup(keyBuilder.id2key(id));

        if (data == null) {

            return null;
            
        }

        value = (isolatableIndex?_Value.deserialize((byte[])data):(_Value)data);
        
        termCache.put(id, value, false/*dirty*/);
        
        // @todo modify unit test to verify that these fields are being set.

        value.termId = id;
        
        value.known = true;
        
        return value;

    }
    
    /**
     * Recently resolved term identifers are cached to improve performance when
     * externalizing statements.
     * 
     * FIXME backport to scale-out version.
     */
    protected LRUCache<Long, _Value> termCache = new LRUCache<Long, _Value>(10000);
       
    /**
     * Note: Handles both unisolatable and isolatable indices.
     * <p>
     * Note: If {@link _Value#key} is set, then that key is used. Otherwise the
     * key is computed and set as a side effect.
     * <p>
     * Note: If {@link _Value#termId} is set, then returns that value
     * immediately. Otherwise looks up the termId in the index and sets
     * {@link _Value#termId} as a side-effect.
     */
    final public long getTermId(Value value) {

        if(value==null) return IRawTripleStore.NULL;
        
        _Value val = (_Value) OptimizedValueFactory.INSTANCE
                .toNativeValue(value);
        
        if (val.termId != IRawTripleStore.NULL) {

            return val.termId;
            
        }

        IIndex ndx = getTermIdIndex();
        
        final boolean isolatableIndex = ndx.isIsolatable();

        if (val.key == null) {

            // generate key iff not on hand.
            val.key = keyBuilder.value2Key(val);
            
        }
        
        // lookup in the forward index.
        Object tmp = ndx.lookup(val.key);
        
        if (tmp == null) {

            return IRawTripleStore.NULL;
            
        }
        
        if (isolatableIndex) {
            
            try {

                val.termId = new DataInputBuffer((byte[])tmp).unpackLong();

            } catch (IOException ex) {

                throw new RuntimeException(ex);

            }
            
        } else {

            val.termId = (Long) tmp;

        }

        // was found in the reverse mapping.
        val.known = true;
        
        return val.termId;

    }

    /**
     * 
     * @todo This could use two threads to write on the indices if we divide the
     *       work into batches. The first thread would write on the term:id
     *       index, placing a batch on a queue every N terms. The second thread
     *       would read batches from the queue, re-order them for the id:term
     *       index, and then write on the id:term index.
     *       <p>
     *       Note: For different values of N this might or might not be faster
     *       since it does not perform sustained ordered writes on both indices
     *       but it does allow two threads to operation concurrently writing on
     *       each index and should reduce the total latency of this operation.
     *       <p>
     *       The {@link ScaleOutTripleStore} is already required to partition
     *       its writes, but {@link ClientIndexView} can add transparent
     *       parallelism in that case.
     */
    public void insertTerms( _Value[] terms, int numTerms, boolean haveKeys, boolean sorted ) {

        if (numTerms == 0)
            return;

        if (!haveKeys && sorted)
            throw new IllegalArgumentException("sorted requires keys");
        
        long begin = System.currentTimeMillis();
        long keyGenTime = 0; // time to convert unicode terms to byte[] sort keys.
        long sortTime = 0; // time to sort terms by assigned byte[] keys.
        long insertTime = 0; // time to insert terms into the forward and reverse index.

        {

            /*
             * First make sure that each term has an assigned sort key.
             */
            if (!haveKeys) {

                long _begin = System.currentTimeMillis();
                
                generateSortKeys(keyBuilder, terms, numTerms);
                
                keyGenTime = System.currentTimeMillis() - _begin;

            }
            
            /* 
             * Sort terms by their assigned sort key.  This places them into
             * the natural order for the term:id index.
             */

            if (!sorted ) {
            
                long _begin = System.currentTimeMillis();
                
                Arrays.sort(terms, 0, numTerms, _ValueSortKeyComparator.INSTANCE);

                sortTime += System.currentTimeMillis() - _begin;
                
            }

            /*
             * Insert terms into the foward index.
             */
            {

                final long _begin = System.currentTimeMillis();
                
                /*
                 * Lookup the term in the term:id index. If it is there then
                 * take its termId and mark it as 'known' so that we can avoid
                 * testing the reverse index. Otherwise, insert the term into
                 * the term:id index which gives us its termId.
                 */

                IIndexWithCounter termId = (IIndexWithCounter)getTermIdIndex();

                final boolean isolatableIndex = termId.isIsolatable();
                
                ICounter counter = termId.getCounter();
                
                if(counter.get()==IRawTripleStore.NULL) {
                    
                    // Never assign NULL as a term identifier!
                    counter.inc();
                    
                }
                
                for (int i = 0; i < numTerms; i++) {

                    _Value term = terms[i];

                    if (term.termId!=IRawTripleStore.NULL) {
                        /*
                         * The termId is already assigned.
                         * 
                         * Note: among other things, this happens when there are
                         * duplicate references in the terms[].
                         */
                        continue;
                    }
                    
                    if (!term.known) {

                        assert term.termId==IRawTripleStore.NULL;
                        assert term.key != null;

                        // Lookup in the forward index.
                        Object tmp = termId.lookup(term.key);
                        
                        if(tmp == null) { // not found.

                            /*
                             * Assign the termId.
                             * 
                             * Note: We set the low bit iff the term is a
                             * literals so that we can tell at a glance whether
                             * a term identifier is a literal or not.
                             * 
                             * FIXME back port to the scale-out version as well.
                             * 
                             * @todo we could use negative term identifiers
                             * except that we pack the termId in a manner that
                             * does not allow negative integers. a different
                             * pack routine would allow us all bits.
                             */
                            term.termId = counter.inc()<<1;
                            
                            if(term instanceof Literal) {
                                
                                term.termId |= 0x01L;
                                
                            }

                            /*
                             * Insert into forward mapping from serialized term to packed term
                             * identifier.
                             */
                            if(isolatableIndex) {
                               
                                // used to serialize term identifers.
                                final DataOutputBuffer idbuf = new DataOutputBuffer(
                                        Bytes.SIZEOF_LONG);

                                // format the term identifier as a packed long integer.
                                try {
                                
                                    idbuf.reset().packLong(term.termId);
                                    
                                } catch (IOException ex) {
                                    
                                    throw new RuntimeException(ex);
                                    
                                }

                                if (termId.insert(term.key, idbuf.toByteArray()) != null) {

                                    throw new AssertionError();

                                }
                                
                            } else {

                                if (termId.insert(term.key, Long.valueOf(term.termId)) != null) {

                                    throw new AssertionError();

                                }

                            }
                            
                        } else { // found.
                        
                            if (isolatableIndex) {
                                
                                try {

                                    term.termId = new DataInputBuffer((byte[]) tmp).unpackLong();

                                } catch (IOException ex) {

                                    throw new RuntimeException(ex);

                                }
                            } else {

                                term.termId = (Long)tmp;
                                
                            }

                            // the term was found in the forward index.
                            term.known = true;
                        
                        }
                        
                    } else assert term.termId != IRawTripleStore.NULL;
                    
                }

                insertTime += System.currentTimeMillis() - _begin;
                
            }
            
        }
                
        /*
         * Sort terms based on their assigned termId.
         */
        {

            long _begin = System.currentTimeMillis();

            Arrays.sort(terms, 0, numTerms, TermIdComparator.INSTANCE);

            sortTime += System.currentTimeMillis() - _begin;

        }
        
        /*
         * Insert terms into the reverse index.
         */
        {

            long _begin = System.currentTimeMillis();
          
            /*
             * Add unknown terms to the reverse index, which is the index that
             * we use to lookup the RDF value by its termId to serialize some
             * data as RDF/XML or the like.
             * 
             * Note: We only insert terms that were reported as "not found" when
             * we inserted them into the forward mapping. This reduces the #of
             * index tests that we perform.
             */
            
            IIndex idTerm = getIdTermIndex();
            
            final boolean isolatableIndex2 = idTerm.isIsolatable();
            
            // reused for all terms serialized.
            DataOutputBuffer out = new DataOutputBuffer();
            
            for (int i = 0; i < numTerms; i++) {

                _Value term = terms[i];
                
                assert term.termId != IRawTripleStore.NULL;
                
                if (!term.known) {
                    
                    /*
                     * Insert into the reverse mapping from the term identifier
                     * to serialized term.
                     * 
                     * Note: if there are duplicate references in terms[] then
                     * [term.known] gets set for the first reference and the
                     * remaining references get skipped.
                     */

                    // form the key from the term identifier.
                    final byte[] idKey = keyBuilder.id2key(term.termId);

                    // insert the serialized term under that key.
                    if (idTerm.insert(idKey, (isolatableIndex2 ? term
                            .serialize(out.reset()) : term)) != null) {

                        throw new AssertionError();

                    }

                    term.known = true; // now in the fwd and rev indices.
                    
                }

            }

            insertTime += System.currentTimeMillis() - _begin;

        }

        long elapsed = System.currentTimeMillis() - begin;

        if(numTerms>1000) {

            log.info("Wrote " + numTerms + " in " + elapsed + "ms; keygen="
                    + keyGenTime + "ms, sort=" + sortTime + "ms, insert="
                    + insertTime + "ms");
            
        }
        
    }

}
