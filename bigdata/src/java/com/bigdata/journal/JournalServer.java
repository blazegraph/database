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

import java.nio.ByteBuffer;


/**
 * The network facing interface for a journal file. The journal is used to
 * absorb pre-serialized objects and smaller streams (large streams should be
 * directed to a suitably provisioned read-write database segment, e.g., large
 * pages, large extents, and lots of disk). This means that objects appearing
 * from the network-facing interface of the journal already have an assigned
 * int64 identifier. What the journal does is allocate slots to absorb those
 * objects, update an object index so that they can be recovered. The internal
 * addresses for the journal are slot identifiers.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo There needs to be an API for streaming large objects.
 */

public class JournalServer {
    
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
     * The {@link ClientResponder} handles client requests.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public class ClientResponder extends Thread {
        
        public ClientResponder() {
        
            super("Client-Responder");
            
        }
        
    }
    
    /**
     * The {@link JournalWriter} consumes buffered client requests from a FIFO
     * queue and writes them onto the journal.
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
    
    public class JournalWriter extends Thread {

        final Journal journal;
        
        /*
         * @todo handshake with journal to make sure that the writer is
         * exclusive, e.g., obtaining an exclusive file lock might work.
         */
        public JournalWriter(Journal journal) {

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
         *            The int64 persistent identifier.
         * @param data
         *            The data to be written. The bytes from
         *            {@link ByteBuffer#position()} to
         *            {@link ByteBuffer#limit()} will be written.
         */
        public void write(long tx, long id, ByteBuffer data) {

            journal.write(tx,id,data);
            
        }

        public byte[] read(long tx, long id) {

            return null;

        }

        public void delete(long tx, long id) {

        }

    }

}
