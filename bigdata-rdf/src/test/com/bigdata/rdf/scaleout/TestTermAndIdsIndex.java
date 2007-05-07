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

import java.io.IOException;

import junit.framework.TestCase2;

import org.openrdf.model.Value;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.UnicodeKeyBuilder;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.rdf.RdfKeyBuilder;
import com.bigdata.rdf.TempTripleStore;
import com.bigdata.rdf.TripleStore;
import com.bigdata.rdf.model.OptimizedValueFactory._URI;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;
import com.bigdata.service.AbstractServerTestCase;
import com.bigdata.service.BigdataClient;
import com.bigdata.service.DataServer;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.MetadataServer;

/**
 * Test suite for use of the terms and ids indices.
 * <p>
 * In order to run connected to data services, you MUST specify at least the
 * following properties to the JVM and have access to the resources in
 * <code>src/resources/config</code>.
 * 
 * <pre>
 *   -Djava.security.policy=policy.all -Djava.rmi.server.codebase=http://proto.cognitiveweb.org/maven-repository/bigdata/jars/ *
 * </pre>
 * 
 * @todo single point insertion into the indices.
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
    IIndex terms;
    
    /**
     * The ids index.
     */
    IIndex ids;
    
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
      terms = fed.getIndex(IBigdataFederation.UNISOLATED, "terms");
      ids = fed.getIndex(IBigdataFederation.UNISOLATED, "ids");
      
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
     * FIXME This must be a remote partition local counter that is accessed
     * from within the unisolated operation!
     */
    private long _counter = 1L;
    
    /**
     * Add a term into the term:id index and the id:term index, returning the
     * assigned term identifier (non-batch API).
     * 
     * @param value
     *            The term.
     * 
     * @return The assigned term identifier.
     * 
     * FIXME this must be a remote unisolated operation using a (named?)
     * partition local counter!
     */
    public long addTerm(Value value) {

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

        final IIndex termId = terms;
        final IIndex idTerm = ids;

        // formulate key from the RDF value.
        final byte[] termKey = keyBuilder.value2Key(val);

        /*
         * Test the forward index.
         * 
         * @todo we could avoid the use of a Long() constructor by assigning a
         * NULL term identifier if there is an index miss.
         */
        Long tmp;
        {

            byte[] data = (byte[]) termId.lookup(termKey);
            
            if(data==null) {
            
                tmp = null;
                
            } else {
                
                try {

                    tmp = new DataInputBuffer(data).unpackLong();
                    
                } catch(IOException ex) {
                    
                    throw new RuntimeException(ex);
                    
                }
                
            }
            
        }

        if (tmp != null) {

            /*
             * the term was found on the forward lookup, so we are done.
             * 
             * FIXME Per my comments below, the client is required to also
             * validate the reverse index [ids] in order to make sure that the
             * assignment is both consistent and complete - this requirement
             * arises from the lack of isolation in the insertion into [terms]
             * and [ids] with the consequence that a term could be inserted into
             * the forward mapping [terms] and (if the client then fails) the
             * consistent assignment of the reverse mapping in [ids] will never
             * be made.
             */

            val.termId = tmp.longValue();
            
        } else {
            
            /*
             * the term was not in the database.
             */

            /*
             * assign the next term identifier from the persistent counter.
             * 
             * FIXME This MUST be a persistent within partition counter used in
             * the unisolated write operation!  The test on the terms index MUST
             * also occur in the same unisolated operation in order to for the
             * assignment of counter values to be consistent!
             */
            val.termId = _counter++; 
            
            /*
             * insert into the forward mapping.
             * 
             * FIXME reuse a buffer for the conversion of Long to byte[].
             * Consider the advantage of not packing since we then know the
             * actual length of the byte[] or using getNibbleCount() to
             * exact-size the byte[] to avoid a needless copy.
             */
            DataOutputBuffer out = new DataOutputBuffer(8);
            try {
                out.packLong(val.termId);
            } catch(IOException ex) {
                throw new RuntimeException(ex);
            }
            if(termId.insert(termKey, out.toByteArray())!=null) {
                /*
                 * It should be impossible for there to be a concurrent
                 * assignment since we are (SHOULD BE) running in an unisolated
                 * write thread.
                 */
                throw new AssertionError();
            }

            /*
             * Insert into the reverse mapping from identifier to term.
             * 
             * Note: This is a _consistent_ assignment. It does NOT matter if
             * another client is simultaneously adding the same term since the
             * term identifier was assigned by the [terms] index. What does
             * matter is that each client attempt tests _both_ the [terms] and
             * the [ids] indices in addTerm() so that we are guarenteed that
             * those indices remain consistent. If we only insert into the [ids]
             * index when the term was NOT found in [terms] then a client that
             * fails after inserting into the [terms] index would cause the
             * reverse mapping in the [ids] index to remain undefined.
             * 
             * @todo this should be written as a conditional assignment so that
             * we do not trigger IOs from a write if there is an undeleted key
             * for this term.
             */

            idTerm.insert(keyBuilder.id2key(val.termId), val.serialize());

        }

        val.known = true;
        
        return val.termId;

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
     * @return The pre-assigned termId -or- 0L iff the term is not known to the
     *         database.
     * 
     * @todo cache some well-known values?  E.g., those defined by the InferenceEngine.
     */
    public long getTermId(Value value) {

        _Value val = (_Value) value;
        
        if( val.termId != 0l ) return val.termId; 

        Long id = (Long)ids.lookup(keyBuilder.value2Key(value));
        
        if( id == null ) return 0L;

        val.termId = id.longValue();

        return val.termId;

    }

    /**
     * FIXME write a stress test with concurrent threads inserting terms and
     * having occasional failures between the insertion into terms and the
     * insertion into ids and verify that the resulting mapping is always fully
     * consistent because the client verifies that the term is in the ids
     * mapping on each call to addTerm().
     * 
     * FIXME write a performance test with (a) a single client thread; and (b)
     * concurrent clients/threads loaded terms into the KB and use it to tune
     * the batch load operations (there is little point to tuning the single
     * term load operations).
     */
    public void test_singlePoint() {

        _URI term1 = new _URI("http://www.bigdata.com");
        
        final long id1 = addTerm(term1);
        
        assertEquals(term1,getTerm(id1));
        
        assertEquals(id1,addTerm(term1));
        
    }
    
    public void test_batchInsert() {

        fail("write test");

    }
    
    public void test_rangeScan() {

        fail("write test");

    }
    
    public void test_restartSafe() {

        fail("write test");
        
    }
    
}
