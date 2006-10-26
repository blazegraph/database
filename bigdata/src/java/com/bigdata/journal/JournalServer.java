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
 * Created on Oct 9, 2006
 */

package com.bigdata.journal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * The network facing interface for a journal file. The journal is used to
 * absorb pre-serialized objects and smaller streams (large streams should be
 * directed to a suitably provisioned read-write database segment, e.g., large
 * pages, large extents, and lots of disk). This means that objects appearing
 * from the network-facing interface of the journal already have an assigned
 * int64 identifier. This service identifies the target journal based on the
 * segment component of the identifier. What the journal does is allocate slots
 * to absorb those objects and update an object index so that they can be
 * recovered. The journal accepts int32 within segment persistent identifiers.
 * The internal addresses for the journal are slot identifiers, which are not
 * visible to the application.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo There needs to be an API for streaming large objects.
 * 
 * @todo Refactor code from the nio test suites.
 * 
 * @todo Support both row oriented operations on the journal and page oriented
 *       operations on the read-optimized database from a single server (and
 *       rename this class).
 * 
 * @todo Support replicated segments, e.g., via chaining or ROWAA.
 */

public class JournalServer {
    

    /**
     * Open journals (indexed by segment).
     * 
     * @todo Change to int32 keys?
     * @todo Define Segment object to encapsulate both the Journal and the
     *       database as well as any metadata associated with the segment, e.g.,
     *       load stats.
     */
    Map<Long,Journal> journals = new HashMap<Long, Journal>();

    /**
     * Active transactions (indexed by the transaction identifier).
     */
    Map<Long,Tx> transactions = new HashMap<Long,Tx>();
    
    /**
     * 
     * @param segment
     * @param properties
     * @throws IOException
     * 
     * @todo Define protocol for journal startup.
     */
    public void openSegment(long segment, Properties properties) throws IOException {
        
        if( journals.containsKey(segment)) {
        
            throw new IllegalStateException();
            
        }
        
        // @todo pass segment in when creating/opening a journal.
        Journal journal = new Journal(properties);

        journals.put(segment, journal);
        
    }

    /**
     * 
     * @param segment
     * @param properties
     * @throws IOException
     * 
     * @todo Define protocol for journal shutdown.
     */
    public void closeSegment(long segment,Properties properties) throws IOException {
        
        Journal journal = journals.remove(segment);

        if( journal == null ) throw new IllegalArgumentException();
        
        /*
         * @todo This is far to abupt. We have to gracefully shutdown the
         * segment.
         */
        journal._bufferStrategy.close();
        
    }
    
    /**
     * Models a request from a client that has been read from the wire and is
     * ready for processing.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo define this and define how it relates to client responses. 
     */
    public static class ClientRequest {
        
    }
    
    /**
     * Models a response that is read to be send down the wire to a client.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo define this and define how it relates to client requests. 
     */
    public static class ClientResponse {
        
    }
    
    /**
     * @todo create with NO open segments, and then accept requests to receive,
     *       open, create, send, close, or delete a segment. When opening a
     *       segment, open both the journal and the database. Keep properties
     *       for defaults? Server options only?
     * 
     * @todo Work out relationship between per-segment and per-transaction
     *       request processing. If requests are FIFO per transaction, then that
     *       has to dominate the queues but we may want to have a set of worker
     *       threads that allow greater parallism when processing requests in
     *       different transactions against different segments.
     */
    public JournalServer(Properties properties) {

        Queue<ClientRequest> requests = new ConcurrentLinkedQueue<ClientRequest>();

        Queue<ClientResponse> responses = new ConcurrentLinkedQueue<ClientResponse>();
        
        
    }
    
    /**
     * The {@link ClientAcceptor} accepts new clients.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public class ClientAcceptor extends Thread {
        
        public ClientAcceptor() {
            
            super("Client-Acceptor");
            
        }
        
    }

    /**
     * The {@link ClientResponder} buffers Read, Write, and Delete requests from
     * the client, places them into a per-transaction queue, and notices when
     * results are available to send back to the client.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo Delete requests are basically a special case of write requests, but
     * they probably deserve distinction in the protocol.
     */
    public class ClientResponder extends Thread {
        
        public ClientResponder() {
        
            super("Client-Responder");
            
        }
        
    }
    
    /**
     * The {@link ClientRequestHandler} consumes buffered client requests from a
     * per-transaction FIFO queue and places responses onto a queue where they
     * are picked up by the {@link ClientResponder} and sent to the client.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo Reads from the journal must read through any FIFO queue, which
     *       means indexing the buffered request by transaction, arrival time,
     *       and objects written. If client requests write directly through then
     *       we can simplify this logic. However, I believe that we need to be
     *       able to suspend writes on the journal during commit processing. If
     *       the client had to block on writes for any transaction, that could
     *       introduce unacceptable latency.
     */
    
    public class ClientRequestHandler extends Thread {

        final Journal journal;
        
        /*
         * @todo handshake with journal to make sure that the writer is
         * exclusive, e.g., obtaining an exclusive file lock might work.
         */
        public ClientRequestHandler(Journal journal) {

            super("Journal-Writer");

            if (journal == null)
                throw new IllegalArgumentException();

            this.journal = journal;

        }

        /**
         * Write request from client.
         * 
         * @param tx
         *            The transaction identifier.
         * @param id
     *            The int32 within-segment persistent identifier.
         * @param data
         *            The data to be written. The bytes from
         *            {@link ByteBuffer#position()} to
         *            {@link ByteBuffer#limit()} will be written.
         */
        public void write(long tx, int id, ByteBuffer data) {

            Tx transaction = transactions.get(tx);
            
            if( transaction == null ) {
                
                // @todo Send back an error.
                
                throw new UnsupportedOperationException();
                
            }
            
            transaction.write(id,data);
            
        }

        /**
         * Read request from the client.
         * 
         * @param tx
         *            The transaction identifier.
         * 
         * @param id
         *            The int32 within-segment persistent identifier.
         */
        public void read(long tx, int id) {

            Tx transaction = transactions.get(tx);
            
            if( transaction == null ) {
                
                // @todo Send back an error.
                
                throw new UnsupportedOperationException();
                
            }
            
            /*
             * @todo If we are doing a row scan or any kind of read-ahead then
             * we can buffer the results into a block and send it back along
             * with an object map so that the client can slice the individual
             * rows out of the block.
             */
            ByteBuffer data = transaction.read(id, null);
            
            if( data == null ) {
                
                /*
                 * FIXME Resolve the object against the database.
                 */
                throw new UnsupportedOperationException("Read from database");
                
            }
            
            /*
             * FIXME Write the data onto a socket to get it back to the client.
             */

        }

        /**
         * Delete request from client.
         * 
         * @param tx
         *            The transaction identifier.
         * @param id
         *            The int32 within-segment persistent identifier.
         */
        public void delete(long tx, int id) {

            Tx transaction = transactions.get(tx);
            
            if( transaction == null ) {
                
                // @todo Send back an error.
                
                throw new UnsupportedOperationException();
                
            }

            transaction.delete(id);

        }

    }

}
