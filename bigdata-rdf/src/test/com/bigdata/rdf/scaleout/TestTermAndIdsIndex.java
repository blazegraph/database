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
 * Created on May 7, 2007
 */

package com.bigdata.rdf.scaleout;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import junit.framework.TestCase2;

import org.CognitiveWeb.extser.LongPacker;
import org.CognitiveWeb.extser.ShortPacker;
import org.openrdf.model.Value;
import org.openrdf.vocabulary.RDF;
import org.openrdf.vocabulary.RDFS;
import org.openrdf.vocabulary.XmlSchema;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IEntryIterator;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IKeyBuffer;
import com.bigdata.btree.KeyBufferSerializer;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.btree.MutableKeyBuffer;
import com.bigdata.btree.UnicodeKeyBuilder;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.isolation.UnisolatedBTree;
import com.bigdata.journal.IIndexStore;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.RdfKeyBuilder;
import com.bigdata.rdf.TempTripleStore;
import com.bigdata.rdf.TripleStore;
import com.bigdata.rdf.model.OptimizedValueFactory.TermIdComparator;
import com.bigdata.rdf.model.OptimizedValueFactory._BNode;
import com.bigdata.rdf.model.OptimizedValueFactory._Literal;
import com.bigdata.rdf.model.OptimizedValueFactory._URI;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;
import com.bigdata.rdf.model.OptimizedValueFactory._ValueSortKeyComparator;
import com.bigdata.service.AbstractServerTestCase;
import com.bigdata.service.BigdataClient;
import com.bigdata.service.ClientIndexView;
import com.bigdata.service.DataServer;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.ICounter;
import com.bigdata.service.IProcedure;
import com.bigdata.service.MetadataServer;
import com.bigdata.service.ClientIndexView.Split;

/**
 * Test suite for use of the terms and ids indices.
 * <p>
 * In order to run connected to data services, you MUST specify at least the
 * following properties to the JVM and have access to the resources in
 * <code>src/resources/config</code>.
 * 
 * <pre>
 *     -Djava.security.policy=policy.all -Djava.rmi.server.codebase=http://proto.cognitiveweb.org/maven-repository/bigdata/jars/ *
 * </pre>
 * 
 * @todo consider an integration point for a full text index when loading new
 *       terms into the lexicon.
 * 
 * FIXME write a stress test with concurrent threads inserting terms and having
 * occasional failures between the insertion into terms and the insertion into
 * ids and verify that the resulting mapping is always fully consistent because
 * the client verifies that the term is in the ids mapping on each call to
 * addTerm().
 * 
 * FIXME write a performance test with (a) a single client thread; and (b)
 * concurrent clients/threads loaded terms into the KB and use it to tune the
 * batch load operations (there is little point to tuning the single term load
 * operations).
 * 
 * FIXME Expose a "submit" operator on {@link IIndex}. The submit operation on
 * the data service MUST specify an index and an index partition. The scale-out
 * {@link ClientIndexView} class will auto-partition used submit operations with
 * an appropriate factory method for breaking down the operation into the
 * various spanned partitions. The submitted operators need to be downloadable
 * code - and they might run in a fairly restrictive sandbox for additional
 * security.
 * 
 * FIXME Add "getCounter()" method to {@link IIndex}. This will provide a
 * restart-safe counter that can be used in an unisolated context to produce
 * unique identifiers. The counter will use the int32 partition identifier for
 * generated values, which will be origin unsigned one within a given partition
 * (reserving partId:0 as an unassignable value). The counter will be per-<em>partition</em>,
 * so the {@link UnisolatedBTree} needs to store the counter values for each
 * partition mapped onto the same mutable btree.
 * <p>
 * Potentially nice features, but not required at this time include named
 * counters, counters registered for a specified starting value, etc. It may be
 * necessary to register a named counter, in which case the counter could
 * (should? must?) be imposed on each data service that can absorb writes for
 * the scale-out index.
 * 
 * FIXME write a version of addTerm() and addTerms() that runs as a "submit"
 * operator on the data service (these are actually _two_ operations addTerms()
 * and addIds() since the submitted unisolated operation is limited to a single
 * partition of a single index). The serialization of the sent and returned data
 * is handled by the specific submit operatation class itself, so it is
 * responsible for any IO tuning. Since there is likely to be reuse of certain
 * kinds of data, implementors should be aware of {@link IKeyBuffer} and its
 * implementations and various compression algorithms (hamming, dictinary) that
 * are available.
 * <p>
 * Consider AbstractIndexProcedure extends IProcedure, requiring the index name
 * (the task data service also needs the index partitionId) and not having raw
 * access to the {@link IIndexStore} since it may only read/write on a single
 * index partition at a time.
 * 
 * @todo batch ordered insertion into the indices.
 * 
 * @todo various range scan operations required to support the
 *       {@link TripleStore} APIs.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestTermAndIdsIndex extends TestCase2 {

    /**
     * 
     */
    public TestTermAndIdsIndex() {
    }

    /**
     * @param arg0
     */
    public TestTermAndIdsIndex(String arg0) {
        super(arg0);
    }

    RdfKeyBuilder keyBuilder = new RdfKeyBuilder(new UnicodeKeyBuilder());
    
    /**
     * Starts in {@link #setUp()}.
     */
    MetadataServer metadataServer0;
    /**
     * Starts in {@link #setUp()}.
     */
    DataServer dataServer1;
    /**
     * Starts in {@link #setUp()}.
     */
    DataServer dataServer0;
    /**
     * Starts in {@link #setUp()}.
     */
    BigdataClient client;
    /**
     * Connected in {@link #setUp()}.
     */
    IBigdataFederation fed;
    
    /**
     * The terms index.
     */
    ClientIndexView terms;
    
    /**
     * The ids index.
     */
    ClientIndexView ids;
    
    /**
     * FIXME Work the setup, teardown, and APIs until I can use either an
     * embedded database or a client-service divide with equal ease. This will
     * be especially important for the {@link TempTripleStore}, which normally
     * uses only local resources. The best way is to defined an interface
     * IBigdataClient and then provide both embedded and data-server
     * implementations.
     */
    public void setUp() throws Exception {
        
        log.info(getName());

        // @todo verify that this belongs here vs in a main(String[]).
        System.setSecurityManager(new SecurityManager());

//      final String groups = ".groups = new String[]{\"" + getName() + "\"}";
      
      /*
       * Start up a data server before the metadata server so that we can make
       * sure that it is detected by the metadata server once it starts up.
       */
      dataServer1 = new DataServer(new String[] {
              "src/resources/config/standalone/DataServer1.config"
//              , AbstractServer.ADVERT_LABEL+groups 
              });

      new Thread() {

          public void run() {
              
              dataServer1.run();
              
          }
          
      }.start();

      /*
       * Start the metadata server.
       */
      metadataServer0 = new MetadataServer(
              new String[] { "src/resources/config/standalone/MetadataServer0.config"
//                      , AbstractServer.ADVERT_LABEL+groups
                      });
      
      new Thread() {

          public void run() {
              
              metadataServer0.run();
              
          }
          
      }.start();

      /*
       * Start up a data server after the metadata server so that we can make
       * sure that it is detected by the metadata server once it starts up.
       */
      dataServer0 = new DataServer(
              new String[] { "src/resources/config/standalone/DataServer0.config"
//                      , AbstractServer.ADVERT_LABEL+groups
                      });

      new Thread() {

          public void run() {
              
              dataServer0.run();
              
          }
          
      }.start();

      client = new BigdataClient(
              new String[] { "src/resources/config/standalone/Client.config"
//                      , BigdataClient.CLIENT_LABEL+groups
                      });

      // Wait until all the services are up.
      AbstractServerTestCase.getServiceID(metadataServer0);
      AbstractServerTestCase.getServiceID(dataServer0);
      AbstractServerTestCase.getServiceID(dataServer1);
      
      // verify that the client has/can get the metadata service.
      assertNotNull("metadataService", client.getMetadataService());

      // Connect to the federation.
      fed = client.connect();
      
      // Register indices.
      fed.registerIndex("terms");
      fed.registerIndex("ids");
      
      // Obtain unisolated views on those indices.
      terms = (ClientIndexView) fed.getIndex(IBigdataFederation.UNISOLATED, "terms");
      ids = (ClientIndexView) fed.getIndex(IBigdataFederation.UNISOLATED, "ids");
      
    }

    public void tearDown() throws Exception {

        /*
         * @todo add fed.disconnect().
         */
        
        if(metadataServer0!=null) {

            metadataServer0.destroy();
        
            metadataServer0 = null;

        }

        if(dataServer0!=null) {

            dataServer0.destroy();
        
            dataServer0 = null;

        }
        
        if (dataServer1 != null) {
            
            dataServer1.destroy();

            dataServer1 = null;
            
        }
        
        if(client!=null) {

            client.terminate();

            client = null;
            
        }
        
        log.info(getName());

    }
    
    /**
     * Add a term into the term:id index and the id:term index, returning the
     * assigned term identifier (non-batch API).
     * 
     * @param value
     *            The term.
     * 
     * @return The assigned term identifier.
     */
    public long addTerm(Value value) {

        final _Value[] terms = new _Value[]{(_Value)value};
        
        insertTerms(terms, 1, false, false);
        
        return terms[0].termId;
        
//        final _Value val = (_Value) value;
//        
//        if(val.known) {
//            
//            assert val.termId !=0L;
//            
//            return val.termId;
//
//        }
//
//        /*
//         * The forward mapping assigns the identifier.
//         * 
//         * Note: this code tests for existance based on the foward mapping so
//         * that we can avoid the use of the reverse mapping if we know that the
//         * term exists.
//         */
//
//        final IIndex termId = terms;
//        final IIndex idTerm = ids;
//
//        // formulate key from the RDF value.
//        final byte[] termKey = keyBuilder.value2Key(val);
//
//        /*
//         * Test the forward index.
//         * 
//         * @todo we could avoid the use of a Long() constructor by assigning a
//         * NULL term identifier if there is an index miss.
//         */
//        Long tmp;
//        {
//
//            byte[] data = (byte[]) termId.lookup(termKey);
//            
//            if(data==null) {
//            
//                tmp = null;
//                
//            } else {
//                
//                try {
//
//                    tmp = new DataInputBuffer(data).unpackLong();
//                    
//                } catch(IOException ex) {
//                    
//                    throw new RuntimeException(ex);
//                    
//                }
//                
//            }
//            
//        }
//
//        if (tmp != null) {
//
//            /*
//             * the term was found on the forward lookup, so we are done.
//             * 
//             * FIXME Per my comments below, the client is required to also
//             * validate the reverse index [ids] in order to make sure that the
//             * assignment is both consistent and complete - this requirement
//             * arises from the lack of isolation in the insertion into [terms]
//             * and [ids] with the consequence that a term could be inserted into
//             * the forward mapping [terms] and (if the client then fails) the
//             * consistent assignment of the reverse mapping in [ids] will never
//             * be made.
//             */
//
//            val.termId = tmp.longValue();
//            
//        } else {
//            
//            /*
//             * the term was not in the database.
//             */
//
//            /*
//             * assign the next term identifier from the persistent counter.
//             * 
//             * FIXME This MUST be a persistent within partition counter used in
//             * the unisolated write operation!  The test on the terms index MUST
//             * also occur in the same unisolated operation in order to for the
//             * assignment of counter values to be consistent!
//             */
//            val.termId = _counter++; 
//            
//            /*
//             * insert into the forward mapping.
//             * 
//             * FIXME reuse a buffer for the conversion of Long to byte[].
//             * Consider the advantage of not packing since we then know the
//             * actual length of the byte[] or using getNibbleCount() to
//             * exact-size the byte[] to avoid a needless copy.
//             */
//            DataOutputBuffer out = new DataOutputBuffer(8);
//            try {
//                out.packLong(val.termId);
//            } catch(IOException ex) {
//                throw new RuntimeException(ex);
//            }
//            if(termId.insert(termKey, out.toByteArray())!=null) {
//                /*
//                 * It should be impossible for there to be a concurrent
//                 * assignment since we are (SHOULD BE) running in an unisolated
//                 * write thread.
//                 */
//                throw new AssertionError();
//            }
//
//            /*
//             * Insert into the reverse mapping from identifier to term.
//             * 
//             * Note: This is a _consistent_ assignment. It does NOT matter if
//             * another client is simultaneously adding the same term since the
//             * term identifier was assigned by the [terms] index. What does
//             * matter is that each client attempt tests _both_ the [terms] and
//             * the [ids] indices in addTerm() so that we are guarenteed that
//             * those indices remain consistent. If we only insert into the [ids]
//             * index when the term was NOT found in [terms] then a client that
//             * fails after inserting into the [terms] index would cause the
//             * reverse mapping in the [ids] index to remain undefined.
//             * 
//             * @todo this should be written as a conditional assignment so that
//             * we do not trigger IOs from a write if there is an undeleted key
//             * for this term.
//             */
//
//            idTerm.insert(keyBuilder.id2key(val.termId), val.serialize());
//
//        }
//
//        val.known = true;
//        
//        return val.termId;

    }

    /**
     * Return the RDF {@link Value} given a term identifier (non-batch api).
     * 
     * @return the RDF value or <code>null</code> if there is no term with
     *         that identifier in the index.
     */
    public _Value getTerm(long id) {

        byte[] data = (byte[]) ids.lookup(keyBuilder.id2key(id));

        if (data == null)
            return null;

        return _Value.deserialize(data);

    }

    /**
     * Return the pre-assigned termId for the value (non-batch API).
     * 
     * @param value
     *            The value.
     * 
     * @return The pre-assigned termId -or- {@link TripleStore#NULL} iff the
     *         term is not known to the database.
     * 
     * @todo cache some well-known values? E.g., those defined by the
     *       InferenceEngine.
     */
    public long getTermId(Value value) {

        _Value val = (_Value) value;
        
        if( val.termId != TripleStore.NULL ) return val.termId; 

        Long id = (Long)ids.lookup(keyBuilder.value2Key(value));
        
        if( id == null ) return TripleStore.NULL;

        val.termId = id.longValue();

        return val.termId;

    }

    public static interface IIndex2 extends IIndex {

        /**
         * Return a named counter. The counter is mutable iff the index view is
         * mutable. The counter is restart-safe iff the backing store is
         * restart-safe.  For a scale-out index, the high int32 bits of the long
         * values obtained from the counter will be the partition identifier for
         * the index partition. 
         * 
         * @param name
         *            The name of the counter.
         * 
         * @return The counter.
         */
        public ICounter getCounter(String name);

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
        
        System.err.print("Writing "+numTerms+" terms ("+terms.getClass().getSimpleName()+")...");

        {

            /*
             * First make sure that each term has an assigned sort key.
             * 
             * @todo clone the RdfKeyBuilder for concurrency (it is not
             * thread-safe).
             */
            if(!haveKeys) {

                long _begin = System.currentTimeMillis();
                
                generateSortKeys(keyBuilder, terms, numTerms);
                
                keyGenTime = System.currentTimeMillis() - _begin;

            }
            
            /*
             * Sort terms by their assigned sort key. This places them into the
             * natural order for the term:id index.
             * 
             * FIXME "known" terms should be filtered out of this operation. A
             * term is marked as "known" within a client if it was successfully
             * asserted against both the terms and ids index (or if it was
             * discovered on lookup against the ids index).
             */

            if (!sorted ) {
            
                long _begin = System.currentTimeMillis();
                
                Arrays.sort(terms, 0, numTerms, _ValueSortKeyComparator.INSTANCE);

                sortTime += System.currentTimeMillis() - _begin;
                
            }

            /*
             * For each term that does not have a pre-assigned term identifier,
             * execute a remote unisolated batch operation that assigns the term
             * identifier.
             * 
             * @todo parallelize operations across index partitions?
             */
            {
                
                long _begin = System.currentTimeMillis();

                ClientIndexView termId = this.terms;

                /*
                 * Create a key buffer holding the sort keys. This does not
                 * allocate new storage for the sort keys, but rather aligns the
                 * data structures for the call to splitKeys().
                 */
                byte[][] termKeys = new byte[numTerms][];
                
                {
                    
                    for(int i=0; i<numTerms; i++) {
                        
                        termKeys[i] = terms[i].key;
                        
                    }
                    
                }

                // split up the keys by the index partitions.
                List<Split> splits = termId.splitKeys(numTerms, termKeys);

                // for each split.
                Iterator<Split> itr = splits.iterator();
                
                while(itr.hasNext()) {

                    Split split = itr.next();
                    
                    byte[][] keys = new byte[split.ntuples][];
                    
                    for(int i=split.fromIndex, j=0; i<split.toIndex; i++, j++) {
                        
                        keys[j] = terms[i].key;
                        
                    }
                    
                    // create batch operation for this partition.
                    AddTerms op = new AddTerms(new MutableKeyBuffer(
                            split.ntuples, keys));

                    // submit batch operation.
                    AddTerms.Result result = (AddTerms.Result) termId.submit(
                            split.pmd, op);
                    
                    // copy the assigned/discovered term identifiers.
                    for(int i=split.fromIndex, j=0; i<split.toIndex; i++, j++) {
                        
                        terms[i].termId = result.ids[j];
                        
                    }
                    
                }
                
                insertTime += System.currentTimeMillis() - _begin;
                
            }
            
        }
        
        {
            
            /*
             * Sort terms based on their assigned termId.
             * 
             * FIXME The termId MUST be in a total ordering based on the
             * unsigned long integer. This could be accomplished by converting
             * to a byte[8] for this sort order since that is how the termId is
             * actually encoded when we insert into the termId:term (reverse)
             * index. However, it will be faster to fake this with a comparator
             * that compared the termIds as if they were _unsigned_ long
             * integers (which will be a faster sort). The unsigned byte[] keys
             * will still need to be generated before the records can be
             * inserted into the [ids] index.
             */

            long _begin = System.currentTimeMillis();

            Arrays.sort(terms, 0, numTerms, TermIdComparator.INSTANCE);

            sortTime += System.currentTimeMillis() - _begin;

        }
        
        {
        
            /*
             * Add terms to the reverse index, which is the index that we use to
             * lookup the RDF value by its termId to serialize some data as
             * RDF/XML or the like.
             * 
             * Note: Every term asserted against the forward mapping [terms]
             * MUST be asserted against the reverse mapping [ids] EVERY time.
             * This is required in order to assure that the reverse index
             * remains complete and consistent. Otherwise a client that writes
             * on the terms index and fails before writing on the ids index
             * would cause those terms to remain undefined in the reverse index.
             */
            
            long _begin = System.currentTimeMillis();
            
            ClientIndexView idTerm = this.ids;

            /*
             * Create a key buffer to hold the keys generated from the term
             * identifers and then generate those keys. The terms are already in
             * sorted order by their term identifiers from the previous step.
             */
            byte[][] idKeys = new byte[numTerms][];
            
            {

                // Private key builder removes single-threaded constraint.
                KeyBuilder keyBuilder = new KeyBuilder(Bytes.SIZEOF_LONG); 
                
                for(int i=0; i<numTerms; i++) {
                    
                    idKeys[i] = keyBuilder.reset().append(terms[i].termId).getKey();
                    
                }
                
            }

            // split up the keys by the index partitions.
            List<Split> splits = idTerm.splitKeys(numTerms, idKeys);

            // for each split.
            Iterator<Split> itr = splits.iterator();

            // Buffer is reused for each serialized term.
            DataOutputBuffer out = new DataOutputBuffer();
            
            while(itr.hasNext()) {

                Split split = itr.next();
                
                byte[][] keys = new byte[split.ntuples][];
                
                byte[][] vals = new byte[split.ntuples][];
                
                for(int i=split.fromIndex, j=0; i<split.toIndex; i++, j++) {
                    
                    // Encode the term identifier as an unsigned long integer.
                    keys[j] = idKeys[i];
                    
                    // Serialize the term.
                    vals[j] = terms[i].serialize(out.reset());
                    
                }
                
                // create batch operation for this partition.
                AddIds op = new AddIds(
                        new MutableKeyBuffer(split.ntuples, keys),
                        new MutableValueBuffer(split.ntuples, vals));

                // submit batch operation (result is "null").
                idTerm.submit(split.pmd, op);
                
                /*
                 * Iff the unisolated write succeeds then this client knows that
                 * the term is now in both the forward and reverse indices. We
                 * codify that knowledge by setting the [known] flag on the
                 * term.
                 */
                for(int i=split.fromIndex, j=0; i<split.toIndex; i++, j++) {
                    
                    terms[i].known = true;

                }
                
            }
            
            insertTime += System.currentTimeMillis() - _begin;

        }

        long elapsed = System.currentTimeMillis() - begin;
        
        System.err.println("in " + elapsed + "ms; keygen=" + keyGenTime
                + "ms, sort=" + sortTime + "ms, insert=" + insertTime + "ms");
        
    }

    /**
     * Generate the sort keys for the terms.
     * 
     * @param keyBuilder
     *            The object used to generate the sort keys - <em>this is not
     *            safe for concurrent writers</em>
     * @param terms
     *            The terms whose sort keys will be generated.
     * @param numTerms
     *            The #of terms in that array.
     * 
     * @see #createCollator()
     * @see UnicodeKeyBuilder
     */
    public void generateSortKeys(RdfKeyBuilder keyBuilder, _Value[] terms, int numTerms) {
        
        for (int i = 0; i < numTerms; i++) {

            _Value term = terms[i];

            if (term.key == null) {

                term.key = keyBuilder.value2Key(term);

            }

        }

    }
    
    /**
     * This unisolated operation inserts terms into the terms index, assigning
     * identifiers to terms as a side-effect. The use of this operation MUST be
     * followed by the the use of {@link AddIds} to ensure that the reverse
     * mapping from id to term is defined before any statements are inserted
     * using the assigned term identifiers. The client MUST NOT make assertions
     * using the assigned term identifiers until the corresponding
     * {@link AddIds} operation has suceeded.
     * <p>
     * In order for the lexicon to remain consistent if the client fails for any
     * reason after the forward mapping has been made restart-safe and before
     * the reverse mapping has been made restart-safe clients MUST always use a
     * successful {@link AddTerms} followed by a successful {@link AddIds}
     * before inserting statements using term identifiers into the statement
     * indices. In particular, a client MUST NOT treat lookup against the terms
     * index as satisifactory evidence that the term also exists in the reverse
     * mapping.
     * <p>
     * Note that it is perfectly possible that a concurrent client will overlap
     * in the terms being inserted. The results will always be fully consistent
     * if the rules of the road are observed since (a) unisolated operations are
     * single-threaded; (b) term identifiers are assigned in an unisolated
     * atomic operation by {@link AddTerms}; and (c) the reverse mapping is
     * made consistent with the assignments made/discovered by the forward
     * mapping.
     * <p>
     * Note: The {@link AddTerms} and {@link AddIds} operations may be analyzed
     * as a batch and variant of the following pseudocode.
     * 
     * <pre>
     *  
     *  for each term:
     *  
     *  termId = null;
     *  
     *  synchronized (ndx) {
     *    
     *    counter = ndx.getCounter();
     *  
     *    termId = ndx.lookup(term.key);
     *    
     *    if(termId == null) {
     * 
     *       termId = counter.inc();
     *       
     *       ndx.insert(term.key,termId);
     *       
     *       }
     *  
     *  }
     *  
     * </pre>
     * 
     * In addition, the actual operations against scale-out indices are
     * performed on index partitions rather than on the whole index.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class AddTerms implements IProcedure, Externalizable {

        /**
         * 
         */
        private static final long serialVersionUID = 1679137119603652663L;
        
        private IKeyBuffer keys;

        /**
         * De-serialization constructor.
         */
        public AddTerms() {
            
        }
        
        public AddTerms(IKeyBuffer keys) {

            assert keys != null;

            this.keys = keys;
            
        }
        
        /**
         * FIXME Locate a per index partition getCounter() on IIndex?
         * IIndexStore?
         */
        private ICounter getCounter(/*String name, int partitionId*/) {
            
            throw new UnsupportedOperationException();
            
        }
        
        /**
         * For each term whose serialized key is mapped to the current index
         * partition, lookup the term in the <em>terms</em> index. If it is
         * there then note its assigned termId. Otherwise, use the partition
         * local counter to assign the term identifier, note the term identifer
         * so that it can be communicated back to the client, and insert the
         * {term,termId} entry into the <em>terms</em> index.
         * 
         * @param ndx
         *            The terms index.
         * 
         * @return The {@link Result}, which contains the discovered / assigned
         *         term identifiers.
         */
        public Object apply(IIndex ndx) throws Exception {

            final int numTerms = keys.getKeyCount();
            
            // used to store the discovered / assigned term identifiers.
            final long[] ids = new long[numTerms];
            
            // used to assign term identifiers.
            final ICounter counter = getCounter();
            
            // used to serialize term identifers.
            final DataOutputBuffer idbuf = new DataOutputBuffer(
                    Bytes.SIZEOF_LONG);
            
            for (int i = 0; i < numTerms; i++) {

                final byte[] term = keys.getKey(i);
                
                final long termId;

                // Lookup in the forward index.
                byte[] tmp = (byte[]) ndx.lookup(term);

                if (tmp == null) { // not found.

                    // assign termId.
                    termId = counter.inc();

                    // format as packed long integer.
                    idbuf.reset().packLong(termId);

                    // insert into index.
                    if (ndx.insert(term, idbuf.toByteArray()) != null) {

                        throw new AssertionError();

                    }

                } else { // found.

                    termId = new DataInputBuffer(tmp).unpackLong();

                }

                ids[i] = termId;

            }

            return new Result(ids);

        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

            /*
             * Read the entire input stream into a buffer.
             */
            DataOutputBuffer buf = new DataOutputBuffer(in);
            
            /*
             * Unpack the buffer.
             */
            DataInputBuffer is = new DataInputBuffer(buf.buf,0,buf.len);
            
            keys = KeyBufferSerializer.INSTANCE.getKeys(is);
            
        }

        public void writeExternal(ObjectOutput out) throws IOException {

            /*
             * Setup a buffer for serialization.
             */
            DataOutputBuffer buf = new DataOutputBuffer();
            
            /*
             * Serialize the keys onto the buffer.
             */
            KeyBufferSerializer.INSTANCE.putKeys(buf, keys);
            
            /*
             * Copy the serialized form onto the caller's output stream.
             */
            out.write(buf.buf,0,buf.len);
                        
        }

        /**
         * Object encapsulates the discovered / assigned term identifiers and
         * provides efficient serialization for communication of those data to
         * the client.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
         *         Thompson</a>
         * @version $Id$
         */
        public static class Result implements Externalizable {

            public long[] ids;
            
            /**
             * 
             */
            private static final long serialVersionUID = -8307927320589290348L;

            /**
             * De-serialization constructor.
             */
            public Result() {
                
            }
            
            public Result(long[] ids) {
                
                assert ids != null;
                
                assert ids.length > 0;
                
                this.ids = ids;
                
            }

            private final static transient short VERSION0 = 0x0;
            
            public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

                final short version = ShortPacker.unpackShort(in);
                
                if(version!=VERSION0) {
                    
                    throw new IOException("Unknown version: "+version);
                    
                }
                
                final int n = (int) LongPacker.unpackLong(in);
                
                ids = new long[n];
                
                for(int i=0; i<n; i++) {
                    
                    ids[i] = LongPacker.unpackLong(in);
                    
                }
                
            }

            public void writeExternal(ObjectOutput out) throws IOException {

                final int n = ids.length;
                
                ShortPacker.packShort(out, VERSION0);
                
                LongPacker.packLong(out,n);

                for(int i=0; i<n; i++) {
                    
                    LongPacker.packLong(out, ids[i]);
                    
                }
                
            }
            
        }
        
    }
    
    /**
     * Unisolated write operation makes consistent assertions on the
     * <em>ids</em> index based on the data developed by the {@link AddTerms}
     * operation.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class AddIds implements IProcedure, Externalizable {

        /**
         * 
         */
        private static final long serialVersionUID = 7387694802894116258L;
        
        private IKeyBuffer keys;
        private IValueBuffer vals;
        
        /**
         * De-serialization constructor.
         */
        public AddIds() {
            
        }
        
        public AddIds(IKeyBuffer keys,IValueBuffer vals) {

            assert keys != null;
            assert vals != null;
            assert keys.getKeyCount() == vals.getValueCount();
            
            this.keys = keys;
            this.vals = vals;
            
        }
        
        /**
         * Conditionally inserts each key-value pair into the index. The keys
         * are the term identifiers. The values are the terms as serialized by
         * {@link _Value#serialize()}. Since a conditional insert is used, the
         * operation does not cause terms that are already known to the ids
         * index to be re-inserted, thereby reducing writes of dirty index
         * nodes.
         * 
         * @param ndx
         *            The index.
         * 
         * @return <code>null</code>.
         */
        public Object apply(IIndex ndx) throws Exception {

            final int n = keys.getKeyCount();

            for(int i=0; i<n; i++) {
        
                final byte[] key = keys.getKey(i);
                
                final byte[] val;

                /*
                 * Note: Validation SHOULD be disabled except for testing.
                 */
                final boolean validate = true; // FIXME turn off validation.
                
                if (validate) {

                    /*
                     * When the term identifier is found in the reverse mapping
                     * this code path validates that the serialized term is the
                     * same.
                     */
                    byte[] oldval = (byte[]) ndx.lookup(key);
                    
                    val = vals.getValue(i);
                    
                    if( oldval == null ) {
                        
                        if (ndx.insert(key, val) != null) {

                            throw new AssertionError();

                        }
                        
                    } else {

                        /*
                         * Note: This would fail if the serialization of the
                         * term was changed. In order to validate when different
                         * serialization formats might be in use you have to
                         * actually deserialize the terms. However, I have the
                         * validation logic here just as a santity check while
                         * getting the basic system running - it is not meant to
                         * be deployed.
                         */

                        if (BytesUtil.bytesEqual(val, oldval)) {

                            throw new RuntimeException(
                                    "Consistency problem: id="
                                            + KeyBuilder.decodeLong(key, 0));
                            
                        }
                        
                    }
                    
                } else {
                    
                    /*
                     * This code path does not validate that the term identifier
                     * is mapped to the same term. This is the code path that
                     * you SHOULD use.
                     */

                    if (!ndx.contains(key)) {

                        val = vals.getValue(i);
                        
                        if (ndx.insert(key, val) != null) {

                            throw new AssertionError();

                        }

                    }

                }
                
            }
            
            return null;
            
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

            /*
             * Read the entire input stream into a buffer.
             */
            DataOutputBuffer buf = new DataOutputBuffer(in);
            
            /*
             * Unpack the buffer.
             */
            DataInputBuffer is = new DataInputBuffer(buf.buf,0,buf.len);
            
            keys = KeyBufferSerializer.INSTANCE.getKeys(is);

            vals = ValueBufferSerializer.INSTANCE.deserialize(is);
            
            assert keys.getKeyCount() == vals.getValueCount();

        }

        public void writeExternal(ObjectOutput out) throws IOException {

            /*
             * Setup a buffer for serialization.
             */
            DataOutputBuffer buf = new DataOutputBuffer();
            
            /*
             * Serialize the keys onto the buffer.
             */
            KeyBufferSerializer.INSTANCE.putKeys(buf, keys);
            
            /*
             * Serialize the values onto the buffer.
             * 
             * @todo Use suitable compression on the value buffer when
             * serialized.
             */
            ValueBufferSerializer.INSTANCE.serialize(buf, vals);
            
            /*
             * Copy the serialized form onto the caller's output stream.
             */
            out.write(buf.buf,0,buf.len);

        }

    }

    public static interface IValueBuffer {
        
        /**
         * The capacity of the buffer.
         */
        public int capacity();
        
        /**
         * The #of "defined" values in the buffer (defined values MAY be null
         * but are always paired to a key).
         */
        public int getValueCount();

        /**
         * The value in the buffer at the specified index.
         * 
         * @param index
         *            The index into the buffer. The indices into the buffer are
         *            dense and are origin ZERO(0).
         * 
         * @return The value (MAY be null).
         */
        public byte[] getValue(int index);
        
    }
    
    public static class MutableValueBuffer implements IValueBuffer {

        private int nvals; 
        private final byte[][] vals;
        
        public MutableValueBuffer(int nvals, byte[][] vals) {
            
            assert nvals >= 0;
            assert vals != null;
            assert vals.length > 0;
            assert nvals <= vals.length;
            
            this.nvals = nvals;
            
            this.vals = vals;
            
        }
        
        final public int capacity() {
            
            return vals.length;
            
        }
        
        final public byte[] getValue(int index) {
            
            if (index >= nvals) {

                throw new IndexOutOfBoundsException();
                
            }
            
            return vals[index];

        }

        final public int getValueCount() {
            
            return nvals;
            
        }
        
    }

    /**
     * The serialized record has a version# and indicates what kind of
     * compression technique was applied.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo write test case, move to com.bigdata.btree package.
     * 
     * @todo support dictinary compression.
     * 
     * @todo support hamming code compression.
     */
    public static class ValueBufferSerializer {

        public static final transient ValueBufferSerializer INSTANCE = new ValueBufferSerializer();
        
        private static final short VERSION0 = 0x0;
        
        private static final short COMPRESSION_NONE = 0x0;
        private static final short COMPRESSION_DICT = 0x1;
        private static final short COMPRESSION_HAMM = 0x2;
        private static final short COMPRESSION_RUNL = 0x3;
        
        /**
         * Serialize the {@link IValueBuffer}.
         * 
         * @param out
         *            Used to write the record (NOT reset by this method).
         * @param buf
         *            The data.
         *            
         * @return The serialized data.
         */
        public byte[] serialize(DataOutputBuffer out, IValueBuffer buf) {

            assert out != null;
            assert buf != null;
            
            try {
                
                final short version = VERSION0;

                out.packShort(version);

                // @todo support other compression strategies.
                final short compression = COMPRESSION_NONE;

                out.packShort(compression);

                switch (compression) {

                case COMPRESSION_NONE:
                    serializeNoCompression(version, out, buf);
                    break;
                    
                default:
                    throw new UnsupportedOperationException();
                
                }
                
            } catch(IOException ex) {
                
                throw new RuntimeException(ex);
                
            }
            
            return out.toByteArray();
            
        }

        private void serializeNoCompression(short version,
                DataOutputBuffer out, IValueBuffer buf) throws IOException {
            
            final int n = buf.getValueCount();
            
            // #of values.
            out.packLong( n );
            
            // vector of value lengths.
            for(int i=0; i<n; i++) {
                
                byte[] val = buf.getValue(i);

                /*
                 * Note: adds one so that we can differentiate between NULL
                 * and byte[0].
                 */
                out.packLong(val == null ? 0L : val.length + 1);
                
            }

            for(int i=0; i<n; i++) {
                
                byte[] val = buf.getValue(i);
            
                if(val==null) continue;
                
                out.write(val);
                
            }
            
        }

        /**
         * 
         * @param version
         * @param in
         * @return
         * @throws IOException
         * 
         * @todo Deserialization could choose a format in which the values were
         *       a byte[] and the data were only extracted as necessary rather
         *       unpacked into a byte[][] at once. This could improve
         *       performance for some cases. Since the performance depends on
         *       the use, this might be a choice that the caller could indicate.
         */
        private IValueBuffer deserializeNoCompression(short version,DataInputBuffer in) throws IOException {
            
            final int n = (int)in.unpackLong();
            
            final byte[][] vals = new byte[n][];

            for (int i = 0; i < n; i++) {

                /*
                 * Note: length is encoded as len + 1 so that we can identify
                 * nulls.
                 */
                int len = (int) in.unpackLong();

                // allocate the byte[] for this value.
                vals[i] = len == 0 ? null : new byte[len - 1];
                
            }
            
            for( int i=0; i<n; i++) {
                
                if(vals[i] == null) continue;
                
                in.readFully(vals[i]);
                
            }
            
            return new MutableValueBuffer(n,vals);
            
        }
        
        /**
         * 
         * @param in
         * @return
         */
        public IValueBuffer deserialize(DataInputBuffer in) {

            try {
            
                final short version = in.unpackShort();
                
                if (version != VERSION0) {

                    throw new RuntimeException("Unknown version: " + version);
                    
                }
                
                final short compression = in.unpackShort();
            
                switch (compression) {

                case COMPRESSION_NONE:
                    
                    return deserializeNoCompression(version, in);
                    
                default:
                    throw new UnsupportedOperationException();
                
                }

            } catch(IOException ex) {
                
                throw new RuntimeException(ex);
                
            }
            
        }
        
    }
    
    /**
     * Test helper verifies that the term is not in the lexicon, adds the term
     * to the lexicon, verifies that the term can be looked up by its assigned
     * term identifier, verifies that the term is now in the lexicon, and
     * verifies that adding the term again returns the same term identifier.
     * 
     * @param term The term.
     */
    protected void doAddTermTest(_Value term) {
        
        assertEquals(0L,getTermId(term));
        
        final long id = addTerm(term);
        
        assertEquals(id,getTermId(term));

        assertEquals(term,getTerm(id));

        assertEquals(id,addTerm(term));

    }
    
    /**
     * Simple test of inserting one term at a time into the lexicon.
     * 
     * @todo bnodes do not need to be in the reverse mapping, in which case
     * the test helper needs to verify that the bnode is NOT FOUND in the
     * [ids] index and the application needs to interpret a NOT FOUND as
     * meaning that the term identifier stands in for a bnode.
     */
    public void test_addTerm() {

        doAddTermTest(new _URI("http://www.bigdata.com"));
        doAddTermTest(new _URI(RDF.TYPE));
        doAddTermTest(new _URI(RDFS.SUBCLASSOF));
        doAddTermTest(new _URI(XmlSchema.DECIMAL));

        doAddTermTest(new _Literal("abc"));
        doAddTermTest(new _Literal("abc", new _URI(XmlSchema.DECIMAL)));
        doAddTermTest(new _Literal("abc", "en"));

        doAddTermTest(new _BNode(UUID.randomUUID().toString()));
        doAddTermTest(new _BNode("a12"));

    }
    
    /**
     * Simple test of batch insertion of terms into the lexicon.
     * 
     * @todo bnodes do not need to be in the reverse mapping, in which case
     * the test helper needs to verify that the bnode is NOT FOUND in the
     * [ids] index and the application needs to interpret a NOT FOUND as
     * meaning that the term identifier stands in for a bnode.
     */
    public void test_insertTerms() {

        _Value[] terms = new _Value[] {//
                
                new _URI("http://www.bigdata.com"),//
                
                new _URI(RDF.TYPE),//
                new _URI(RDFS.SUBCLASSOF),//
                new _URI(XmlSchema.DECIMAL),//

                new _Literal("abc"),//
                new _Literal("abc", new _URI(XmlSchema.DECIMAL)),//
                new _Literal("abc", "en"),//

                new _BNode(UUID.randomUUID().toString()),//
                new _BNode("a12") //
        };

        insertTerms(terms, terms.length, false, false);
        
        for( int i=0; i<terms.length; i++) {

            // check the forward mapping (term -> id)
            assertEquals("forward mapping", terms[i], getTerm(terms[i].termId));

            // check the reverse mapping (id -> term)
            assertEquals("reverse mapping", terms[i].termId,
                    getTermId(terms[i]));

            /*
             * check flag set iff term was successfully asserted against both
             * the forward and reverse mappings.
             */
            assertTrue("known", terms[i].known);
            
        }

        /**
         * Dumps the forward mapping.
         */
        {
            
            System.err.println("terms index (forward mapping).");
            
            IEntryIterator itr = this.terms.rangeIterator(null, null);
            
            while(itr.hasNext()) {
                
                // the term identifier.
                byte[] val = (byte[]) itr.next();
                
                /*
                 * The sort key for the term. This is not readily decodable. See
                 * RdfKeyBuilder for specifics.
                 */
                byte[] key = itr.getKey();

                /* 
                 * deserialize the term identifier (packed long integer).
                 */
                final long id;
                try {
                
                    id = new DataInputBuffer(val).unpackLong();
                    
                } catch(IOException ex) {
                    
                    throw new RuntimeException(ex);
                    
                }
                
                System.err.println(BytesUtil.toString(key)+":"+id);
                
            }
            
        }
        
        /**
         * Dumps the reverse mapping.
         */
        {

            System.err.println("ids index (reverse mapping).");

            IEntryIterator itr = this.ids.rangeIterator(null, null);
            
            while(itr.hasNext()) {
                
                // the serialized term.
                byte[] val = (byte[]) itr.next();
                
                // the sort key for the term identifier.
                byte[] key = itr.getKey();
                
                // decode the term identifier from the sort key.
                final long id = KeyBuilder.decodeLong(key, 0);

                _Value term = _Value.deserialize(val);
                
                System.err.println( id + ":" + term );
                
            }
        
        }
        
    }
    
    public void test_rangeScan() {

        fail("write test");

    }
    
    public void test_restartSafe() {

        fail("write test");
        
    }
    
}
