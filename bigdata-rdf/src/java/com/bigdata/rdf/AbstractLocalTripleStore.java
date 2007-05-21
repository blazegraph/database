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
 * Created on May 21, 2007
 */

package com.bigdata.rdf;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import org.openrdf.model.Value;

import com.bigdata.btree.ICounter;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IIndexWithCounter;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.btree.MutableKeyBuffer;
import com.bigdata.btree.MutableValueBuffer;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.isolation.UnisolatedBTree;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.model.OptimizedValueFactory.TermIdComparator;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;
import com.bigdata.rdf.model.OptimizedValueFactory._ValueSortKeyComparator;
import com.bigdata.rdf.util.AddIds;
import com.bigdata.rdf.util.AddTerms;

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
 * FIXME change the indices over to {@link UnisolatedBTree} (for this and its
 * derived classes). The code currently assumes control over the serialization
 * of the leaf when it should only assume control over the serialization of
 * individual values (which are only applicable to the terms and ids indices
 * right now as there are no values for the statement indices at this time).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractLocalTripleStore extends AbstractTripleStore {

    public AbstractLocalTripleStore(Properties properties) {
        
        super(properties);
        
    }

    /**
     */
    final public long addTerm(Value value) {

        final _Value val = (_Value) value;
        
        if(val.known) {
            
            assert val.termId !=0L;
            
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

        // formulate key from the RDF value.
        final byte[] termKey = keyBuilder.value2Key(val);

        // Lookup in the forward index.
        final byte[] tmp = (byte[]) terms.lookup(termKey);

        if (tmp == null) { // not found.

            final ICounter counter = terms.getCounter();
            
            if(counter.get()==NULL) {
                
                // Never assign NULL as a term identifier!
                counter.inc();
                
            }

            // assign termId.
            val.termId = counter.inc();

            /*
             * Insert into forward mapping from serialized term to packed term
             * identifier.
             */
            {
               
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
                
            }

            /*
             * Insert into the reverse mapping from the term identifier to
             * serialized term.
             */
            {

                final IIndex ids = getIdTermIndex();

                // form the key from the term identifier.
                final byte[] idKey = keyBuilder.id2key(val.termId);

                // insert the serialized term under that key.
                if (ids.insert(idKey, val.serialize()) != null) {

                    throw new AssertionError();

                }
                
            }

        } else { // found.

            /*
             * the term was found on the forward lookup, so we are done.
             */

            try {

                val.termId = new DataInputBuffer(tmp).unpackLong();
                
            } catch(IOException ex) {
                
                throw new RuntimeException(ex);
                
            }

        }

        val.known = true;
        
        return val.termId;

    }

    /**
     * Batch insert of terms into the database.
     * 
     * @param terms
     *            An array whose elements [0:nterms-1] will be inserted.
     * @param numTerms
     *            The #of terms to insert.
     * @param haveKeys
     *            True if the terms already have their sort keys.
     * @param sorted
     *            True if the terms are already sorted by their sort keys (in
     *            the correct order for a batch insert).
     * 
     * @exception IllegalArgumentException
     *                if <code>!haveKeys && sorted</code>.
     * 
     * @todo refactor until we can share code for this method with the
     *       client-federation version.
     */
    final public void insertTerms( _Value[] terms, int numTerms, boolean haveKeys, boolean sorted ) {

        if (numTerms == 0)
            return;

        if (!haveKeys && sorted)
            throw new IllegalArgumentException("sorted requires keys");
        
        long begin = System.currentTimeMillis();
        long keyGenTime = 0; // time to convert unicode terms to byte[] sort keys.
        long sortTime = 0; // time to sort terms by assigned byte[] keys.
        long insertTime = 0; // time to insert terms into the forward and reverse index.
        
        System.err.print("Writing "+numTerms+" terms ("+terms.getClass().getSimpleName()+")...");

        {

            /*
             * First make sure that each term has an assigned sort key.
             */
            if(!haveKeys) {

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
                 * Create a key buffer holding the sort keys. This does not
                 * allocate new storage for the sort keys, but rather aligns the
                 * data structures for the call to splitKeys().
                 */
                final byte[][] termKeys = new byte[numTerms][];

                for (int i = 0; i < numTerms; i++) {

                    termKeys[i] = terms[i].key;

                }

                AddTerms op = new AddTerms(new MutableKeyBuffer(numTerms,
                        termKeys));

                final AddTerms.Result result;

                try {

                    result = (AddTerms.Result) op
                            .apply((IIndexWithCounter) getTermIdIndex());

                    // Copy the assigned term identifiers.
                    for(int i=0;i<numTerms; i++) {
                        
                        terms[i].termId = result.ids[i];
                        
                    }
                    
                } catch (Exception ex) {

                    throw new RuntimeException(ex);

                }
                
// /*
// * Lookup the term in the term:id index. If it is there then
//                 * take its termId and mark it as 'known' so that we can avoid
//                 * testing the reverse index. Otherwise, insert the term into
//                 * the term:id index which gives us its termId.
//                 * 
//                 * @todo use batch api?
//                 */
//
//                IIndexWithCounter termId = (IIndexWithCounter)getTermIdIndex();
//
//                ICounter counter = termId.getCounter();
//                
//                if(counter.get()==NULL) {
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
//                            term.termId = counter.inc();
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

                insertTime += System.currentTimeMillis() - _begin;
                
            }
            
        }
        
        {
            
            /*
             * Sort terms based on their assigned termId.
             */

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
             * Create a key buffer to hold the keys generated from the term
             * identifers and then generate those keys. The terms are already in
             * sorted order by their term identifiers from the previous step.
             */
            final byte[][] keys = new byte[numTerms][];
            final byte[][] vals = new byte[numTerms][];
            
            {
                
                // Buffer is reused for each serialized term.
                final DataOutputBuffer out = new DataOutputBuffer();

                // Private key builder removes single-threaded constraint.
                KeyBuilder keyBuilder = new KeyBuilder(Bytes.SIZEOF_LONG); 
                
                for(int i=0; i<numTerms; i++) {
                    
                    final _Value term = terms[i];
                    
                    keys[i] = keyBuilder.reset().append(term.termId).getKey();
                 
                    // Serialize the term.
                    vals[i] = term.serialize(out.reset());

                    // The term will be known iff the operation succeeds.
                    terms[i].known = true;

                }
                
            }

            // create batch operation for this partition.
            AddIds op = new AddIds(
                    new MutableKeyBuffer(numTerms, keys),
                    new MutableValueBuffer(numTerms, vals));

            // apply batch operation (result is "null").
            try {
                
                op.apply( (IIndexWithCounter)getIdTermIndex());
                
            } catch(Exception ex) {

                throw new RuntimeException(ex);
                
            }

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

            insertTime += System.currentTimeMillis() - _begin;

        }

        long elapsed = System.currentTimeMillis() - begin;
        
        System.err.println("in " + elapsed + "ms; keygen=" + keyGenTime
                + "ms, sort=" + sortTime + "ms, insert=" + insertTime + "ms");
        
    }

}
