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
 * Created on Jun 18, 2006
 */
package org.CognitiveWeb.bigdata.nio;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.spi.AbstractInterruptibleChannel;
import java.nio.channels.spi.AbstractSelectableChannel;

import junit.framework.TestCase;

/**
 * Test class uses non-blocking I/O to communicate between collection of clients
 * and a single page server backed by test data. This test is designed to
 * exercise features of the nio package and to stress test a non-blocking design
 * for a custom client-server protocol.
 * 
 * FIXME Verify that we can use non-blocking IO for the segments (disk) and
 * figure out how to coordinate moving data to/from disk and to/from the
 * network. There appear to be four relevant methods (they do not update the
 * file pointer) for this purpose: {@link FileChannel#read(ByteBuffer, long)},
 * {@link FileChannel#write(ByteBuffer, long)},
 * {@link FileChannel#transferFrom(java.nio.channels.ReadableByteChannel, long, long)},
 * and
 * {@link FileChannel#transferTo(long, long, java.nio.channels.WritableByteChannel)}.
 * The {@link DBCache} API will have to be changed to expose similar methods
 * that correctly read from the cache or DB as appropriate and correctly write
 * on the safe, log or DB as appropriate.<br>
 * Note: A {@link FileChannel} is NOT a {@link SelectableChannel}! It is
 * derived from {@link AbstractInterruptibleChannel} rather than from
 * {@link AbstractSelectableChannel}. This means that we can not use selection
 * keys with the files and that may limit the opporunities for non-blocking IO
 * to disk.
 * 
 * <pre>
 *  There is no direct support for asynchronous File IO.
 *  
 *  The way the SEDA team worked around this issue is by using a 
 *  RandomAccessFile, and by having a dedicated queue that would process 
 *  messages placed into the queue.  The stage processing the messages would 
 *  then perform the actions within its thread, and then send along any 
 *  messages as a result from the actions.
 * </pre>
 * 
 * @see http://www.onjava.com/pub/a/onjava/2004/09/01/nio.html
 * @see http://coconut.codehaus.org/
 * @see http://sourceforge.net/projects/pyrasun/
 * @see http://gleamynode.net/dev/tl-netty2-example-sumup/docs/
 * @see http://directory.apache.org/subprojects/mina/index.html
 * @see The -XX:MaxDirectMemorySize option (controls direct memory allocation).
 * 
 * Problems with File#transferTo under Linux (Java 1.5, not resolved?)
 * @see http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=5103988
 * @see http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=5105464
 * 
 * NIO Network File Transfer Seriously Underperforming
 * @see http://forum.java.sun.com/thread.jspa?forumID=4&messageID=2229023&threadID=479140
 * 
 * Note that GC page faults may causing blocking in any case!
 * 
 * @todo Get some performance statistics for the bigdata design features.
 * 
 * @todo Develop protocol for the load balancer to start and stop segment
 *       instances and to report disk, CPU and network statistics to the load
 *       balancer. Actually collecting those statistics may be OS dependent.
 *       When a host start a page server, it will register segments with the
 *       load balancer and the catalog and validate that those segments are
 *       current (i.e., that they are part of a quorum). A host may shed a
 *       segment in two stages. First, it may deny access to the segment for
 *       read only transactions. Second, the load balancer may direct a host to
 *       shed a segment. In this case the load balancer must first verify that
 *       sufficient copies of the segment elsewhere. When the segment is
 *       shutdown it no longer accepts, it should be marked as stale, and it
 *       will be eventually deleted. In order to restart a segment it must
 *       either validate as current (consistent with the quorum), receive a
 *       patch from the quorum, or simply be recreated from the quorum. Since
 *       recreating a segment on one host is as costly as replicating it on
 *       another host, a segment which fails the quorum can be deleted and
 *       replicated onto a host selected by the load balancer.
 * 
 * @todo The client should send the request directly to the host running the
 *       page server whose segment is being read or written. This will minimize
 *       the overhead. When the segment is local, it should all be direct
 *       buffers (or direct non-protocol access). When the segment is remote,
 *       the client writes onto a channel and the remote page server reads into
 *       a buffer and hands that off to the segment. Note that dbCache buffers
 *       pages in memory. If those buffers are direct, then the page server for
 *       the segments on that host can use direct buffers and we are all doing
 *       the same thing. If they are not direct then the page server should not
 *       use a direct buffer since it is being forced into a byte[]. <br>
 *       If we enable the client caching protocol, then dbCache should reserve
 *       its cache for modified pages only. Even if we do not enable client
 *       caching, we are better off placing the read cache in the page server so
 *       that all active segments compete for a share pool of page buffers.
 * 
 * @todo a page server must front for all segments on a host. If a segment is
 *       shed by that host, then a "GONE" response is appropriate. If the page
 *       server knows where it went, then it can response with a REDIRECT.
 *       Otherwise the client is responsible for querying the load manager for a
 *       new location for queries against that segment.
 * 
 * @todo reduce maximum buffer sizes to something quite small for testing
 *       purposes and make this a configuration parameter for the test protocol
 *       and the client and server. This will make it possible to directly
 *       inspect the buffer contents without killing eclipse (trying to convert
 *       64k byte[] to a pretty printed format).
 * 
 * @todo use a page checksum to verify roundtrip rather than storing the entire
 *       copy of the page in the client. consider use of page checksum in
 *       BigData overall to identify errors arising from concurrency in page
 *       reads and updates.
 * 
 * @todo Test with 'localhost' as well as the external interface for this host.
 *       Both should work, but the latter should go out onto the wire.
 * 
 * @todo Test with segments having different page sizes, different numbers of
 *       pages, and in different partitions.
 * 
 * @todo There is something that I seem to remember about an interaction between
 *       character set encoders and decoders and their instances and direct
 *       buffers that I need to track down.
 * 
 * @todo Explore use of the keepAlive and trafficClass attributes that can be
 *       set on the client socket (as well as the other options).
 * 
 * @todo Connect to segment should respond with pageSize, pageCount and page0.
 * 
 * @todo Client should timeout segments that it has only read and close the
 *       socket. It can always reconnect. If it has written pages on the segment
 *       then it should probably hold the socket open until the commit since it
 *       will either need to commit or abort all segments that it touches.
 * 
 * @todo validate clean shutdown protocol expecially with respect to a 2 or 3
 *       phase commit. Add notification of shutdown for clients? That is not
 *       really necessary since clients should be robust to simply pulling the
 *       plug on the page server.
 * 
 * @todo Explore behavior of the protocol under temporary network outage. What
 *       do we have to do to get a nice retry behavior using TCP? UDP? SSL? SSH
 *       tunnels?
 * 
 * @version $Id$
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson
 *         </a>
 */

public class TestPageServer extends TestCase {

//    public static Logger log = Logger.getLogger(TestPageServer.class);
//
//    /**
//     * Port used for the tests.
//     */
//    private int port = 8089;
//
//    Random r = new Random();
//    
//    /**
//     * 
//     */
//    public TestPageServer() {
//        super();
//    }
//
//    public TestPageServer(String name) {
//        super(name);
//    }
//
////    public void setUp() throws Exception
////    {
////    }
////
////    public void tearDown() throws Exception {
////    }
//    
//    /**
//     * Verify that we can create and shutdown a page server. We do this twice to
//     * make sure that the page server is closing its connection.
//     */
//    
//    public void test_pageServer_ctor() throws IOException {
//        
//        final int pageSize = 8196;
//        
//        final int numPages = 10;
//
//        PageServer pageServer = new PageServer(port,
//                new Segment[] { new Segment(new OId(0, 1, 0, 0), pageSize,
//                        numPages) });
//
//        pageServer.shutdown();
//
//        pageServer = new PageServer(port,
//                new Segment[] { new Segment(new OId( 0, 1, 0, 0), pageSize,
//                        numPages) });
//
//        pageServer.shutdown();
//        
//    }
//    
//    /**
//     * Verify that we can create page server, connect to it using a client and
//     * shutdown the client. We verify that attempting to send another request to
//     * the page server and results in an appropriate exception.  We then shutdown
//     * the page server as well.
//     */
//
//    public void test_pageServer_clientConnect() throws IOException {
//
//        final int pageSize = 8196;
//
//        final int numPages = 10;
//        
//        final long txId = 0;
//        
//        PageServer pageServer = new PageServer(port,
//                new Segment[] { new Segment(new OId(0, 1, 0, 0), pageSize,
//                        numPages) });
//        
//        PageClient pageClient = new PageClient(port);
//        
//        log.info(pageClient.toString());
//        
//        pageClient.write(txId,new OId(0,1,0,0),new byte[pageSize]);
//        
//        pageClient.shutdown();
//
//        log.info(pageClient.toString());
//        
//        try {
//            pageClient.write(txId, new OId(0, 1, 0, 0),
//                    new byte[pageSize]);
//            fail("Expecting "+ClosedChannelException.class );
//        } catch( ClosedChannelException ex ) {
//            log.info("Ignoring expected exception: "+ex);
//        }
//            
//        pageServer.shutdown();
//        
//    }
//
//    /**
//     * Verify that a client attempting to connect to a socket that is not
//     * running a page server will timeout with an appropriate exception.
//     */
//    public void test_timeoutPageServer() throws IOException {
//
//        try {
//            
//            PageClient pageClient = new PageClient(port);
//            
//            fail("Expecting: "+ConnectException.class);
//            
//        }
//        catch( ConnectException ex ) {
//            
//            log.info("Ignoring expected exception: "+ex);
//            
//        }
//        
//    }
//    
//    /**
//     * Verify that a WRITE operation correctly reports an error when the client
//     * sends the incorrect amount of data.
//     * 
//     * @todo expand test to check all arguments for WRITE.
//     * 
//     * @throws IOException
//     */
//    
//    public void test_writeCorrectRejection() throws IOException {
//
//        final int pageSize = 8196;
//
//        final int numPages = 10;
//
//        PageServer pageServer = new PageServer(port, 
//                new Segment[] { new Segment(new OId(0, 1, 0, 0), pageSize,
//                        numPages) } );
//
//        PageClient pageClient = new PageClient(port);
//
//        try {
//
//            try {
//        
//                pageClient.write(0, new OId(0,1,0,0), new byte[] {});
//                
//                fail("Expecting: " + IOException.class);
//                
//            } catch (IOException ex) {
//                
//                log.info("Ignoring expected exception: " + ex);
//                
//            }
//
//            try {
//                
//                pageClient.write(0, new OId(0,1,0,0), new byte[] { 1, 2, 3 });
//                
//                fail("Expecting: " + IOException.class);
//                
//            } catch (IOException ex) {
//                
//                log.info("Ignoring expected exception: " + ex);
//                
//            }
//
//        }
//
//        finally {
//
//            pageClient.shutdown();
//
//            pageServer.shutdown();
//
//        }
//        
//    }
//
//    /**
//     * Verify that a client can a page (should be zeros).
//     * 
//     * @todo parameterize test on page size and try range of legal page sizes
//     *       and check illegal page sizes for correct rejection by the client
//     *       and server.
//     */
//    public void test_readPage() throws IOException {
//
//        final int pageSize = 8196;
//        
//        final int numPages = 10;
//        
//        PageServer pageServer = new PageServer(port,
//                new Segment[] { new Segment(new OId(0, 1, 0, 0), pageSize,
//                        numPages) });
//        
//        PageClient pageClient = new PageClient(port);
//
//        try {
//
//            byte[] expectedData = new byte[pageSize];
//
//            byte[] actualData = pageClient.read(0, new OId(0,1,0,0));
//
//            assertEquals(expectedData, actualData);
//
//        } finally {
//
//            pageClient.shutdown();
//
//            pageServer.shutdown();
//
//        }
//
//    }
//    
//    /**
//     * Verify that a client will be redirected if they attempt to read a page
//     * belonging to a segment not found on the page server.
//     * 
//     * @todo The {@link PageClient}does not handle redirects in any interesting
//     *       manner and this test will have to be updated when it does.
//     * 
//     * @todo Does a redirect on a write make sense? Since we have a ROWAA policy
//     *       and all copies of a segment must be written. The only kind of
//     *       redirect that would make sense is "GONE" if the copy of the segment
//     *       had simply been deleted from the page server that the client was
//     *       talking to. In that case, the client needs to update its
//     *       bookkeeping and should no longer direct writes to that page server
//     *       for that segment. This also implies that clients need to be
//     *       notified when a new copy of a segment comes online so that they can
//     *       begin writing on it. Finally, clients should parallelize their
//     *       writes onto the copies of a given segment and that suggests the use
//     *       of non-blocking IO in the client.
//     */
//    public void test_readPageRedirects() throws IOException {
//
//        final int pageSize = 8196;
//        
//        final int numPages = 10;
//        
//        PageServer pageServer = new PageServer(port,
//                new Segment[] { new Segment(new OId(0, 1, 0, 0), pageSize,
//                        numPages) });
//        
//        PageClient pageClient = new PageClient(port);
//
//        try {
//
//            try {
//
//                // attempt to read a page from a segment not found on the page
//                // server.
//                pageClient.read(0, new OId(0,0,0,0));
//
//                fail("Expecting redirect.");
//                
//            } catch( IOException ex ) {
//                
//                log.info("Ignoring expected exception: "+ex );
//                
//            }
//
//        } finally {
//
//            pageClient.shutdown();
//
//            pageServer.shutdown();
//
//        }
//
//    }
//    
//    /**
//     * Verify that a client can write a page and read it back.
//     */
//    public void test_readWritePages() throws IOException {
//
//        final int pageSize = 8196;
//        
//        final int numPages = 10;
//        
//        final long txId = 0;
//        
//        final OId pageId = new OId(0,1,0,0);
//        
//        PageServer pageServer = new PageServer(port,
//                new Segment[] { new Segment(new OId(0, 1, 0, 0), pageSize,
//                        numPages) });
//        
//        PageClient pageClient = new PageClient(port);
//
//        try {
//
//            byte[] expectedData = new byte[pageSize];
//
//            r.nextBytes(expectedData);
//
//            pageClient.write(txId, pageId, expectedData);
//
//            byte[] actualData = pageClient.read(txId, pageId);
//
//            assertEquals(expectedData, actualData);
//        
//        } finally {
//
//            pageClient.shutdown();
//
//            pageServer.shutdown();
//        }
//        
//    }
//    
//    /**
//     * A stress test with random reads and writes of random data onto random
//     * pages in the segment. This does NOT attempt to extend the segment.
//     * 
//     * @todo parameterize this test and vary the page size in the allowable
//     *       range.
//     * 
//     * @throws IOException
//     */
//    public void test_readWritePages2() throws IOException {
//        
//        final int pageSize = 8196;
//        
//        final int numPages = 100;
//
//        final int LIMIT = 10000;
//
//        final long txId = 0L;
//
//        /*
//         * pages are initially zeroed, which corresponds with the initial state
//         * of the test segment.
//         */
//        final byte[][] pages = new byte[numPages][];
//        
//        for( int i=0; i<numPages; i++ ) { 
//            
//            pages[ i ] = new byte[ pageSize ];
//            
//        }
//        
//        PageServer pageServer = new PageServer(port,
//                new Segment[] { new Segment(new OId(0, 1, 0, 0), pageSize,
//                        numPages) });
//        
//        PageClient pageClient = new PageClient(port);
//
//        try {
//
//            for (int i = 0; i < LIMIT; i++) {
//
//                // choose a read or write operation.
//                boolean read = r.nextBoolean();
//
//                // choose a random page.
//                OId pageId = new OId(0, 1, r.nextInt(numPages), 0);
//
//                if (read) {
//
//                    /*
//                     * read a random page from the segment and compare it with
//                     * our local copy.
//                     */
//                    byte[] actualData = pageClient.read(txId, pageId);
//                    byte[] expectedData = pages[pageId.getPage()];
//                    assertEquals("pass=" + i, expectedData, actualData);
//
//                } else {
//
//                    /*
//                     * select a random page, fill it with random data, and write
//                     * it onto the segment.
//                     */
//
//                    byte[] data = pages[pageId.getPage()];
//                    r.nextBytes(data);
//                    pageClient.write(txId, pageId, data);
//
//                }
//
//            }
//        } finally {
//
//            pageClient.shutdown();
//
//            pageServer.shutdown();
//
//        }
//        
//    }
//    
//    /**
//     * Test creates a large number of clients, each running in its own thread.
//     * Each client executes random operations against a single page server using
//     * some appropriate probability distribution. Those operations need to
//     * include connecting and disconnecting so that we can measure latency in
//     * the disconnect protocol. The operations need to include reading and
//     * writing pages so that we can measure bandwith. As long as the clients
//     * connect to the external IP address of the host the bandwidth should
//     * appear on the local network.
//     * 
//     * @todo break test down into a stress test that exercises legal operations
//     *       for a single client and one that does the same thing for multiple
//     *       clients.
//     * 
//     * @todo orient test towards detection of concurrent modification errors in
//     *       the state of the segment.
//     * 
//     * @todo Evolve the test case to a set of test page servers either locally
//     *       created or discovered with jini and qualify (a) single client
//     *       against multiple page servers (this is the uninteresting case since
//     *       the client is single threaded -- unless we change that); and (b)
//     *       multiple clients against multiple page servers.
//     * 
//     * @throws IOException
//     */
//    
//    public void test_stress() throws IOException {
//        
////      PageServer pageServer;
////      PageClient[] pageClients;
//
//    }
//    
//    /**
//     * <p>
//     * Compares byte[]s by value (not reference).
//     * </p>
//     * <p>
//     * Note: This method will only be invoked if both arguments can be typed as
//     * byte[] by the compiler. If either argument is not strongly typed, you
//     * MUST case it to a byte[] or {@link #assertEquals(Object, Object)}will be
//     * invoked instead.
//     * </p>
//     * 
//     * @param expected
//     * @param actual
//     */
//    public void assertEquals( byte[] expected, byte[] actual )
//    {
//
//        assertEquals( null, expected, actual );
//        
//    }
//
//    /**
//     * <p>
//     * Compares byte[]s by value (not reference).
//     * </p>
//     * <p>
//     * Note: This method will only be invoked if both arguments can be typed as
//     * byte[] by the compiler. If either argument is not strongly typed, you
//     * MUST case it to a byte[] or {@link #assertEquals(Object, Object)}will be
//     * invoked instead.
//     * </p>
//     * 
//     * @param msg
//     * @param expected
//     * @param actual
//     */
//    public void assertEquals( String msg, byte[] expected, byte[] actual )
//    {
//
//        if( msg == null ) {
//            msg = "";
//        } else {
//            msg = msg + " : ";
//        }
//    
//        if( expected == null && actual == null ) {
//            
//            return;
//            
//        }
//        
//        if( expected == null && actual != null ) {
//            
//            fail( msg+"Expected a null array." );
//            
//        }
//        
//        if( expected != null && actual == null ) {
//            
//            fail( msg+"Not expecting a null array." );
//            
//        }
//        
//        assertEquals
//            ( msg+"length differs.",
//              expected.length,
//              actual.length
//              );
//        
//        for( int i=0; i<expected.length; i++ ) {
//            
//            assertEquals
//                ( msg+"values differ: index="+i,
//                   expected[ i ],
//                   actual[ i ]
//                 );
//            
//        }
//        
//    }
//
//    /**
//     * Test class for page server design. The page server uses non-blocking I/O
//     * and divides its work among several threads:
//     * <dl>
//     * <dt>acceptor</dt>
//     * <dd>accepts new client connections</dd>
//     * <dt>responder</dt>
//     * <dd>responses to client requests</dd>
//     * <dt>statistics</dt>
//     * <dd>logs periodic statistics for events reported by the other threads
//     * </dd>
//     * </dl>
//     * In addition the page server uses a transient, non-atomic segment to store
//     * the data. The segement is initialized with a fixed number of zeroed
//     * pages. Write operations modify the corresponding page in the segment and
//     * read operations return the current data in the segment.
//     * 
//     * @version $Id$
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson
//     *         </a>
//     * 
//     * @todo Consider use of Java logging for this class (and BigData, dbCache,
//     *       and concurrent) as it enables more fine grained distinctions than
//     *       are available with log4j.
//     * 
//     * @see <a
//     *      href="http://forum.java.sun.com/thread.jspa?threadID=493086&messageID=2320455">
//     *      Letting go (of a port) in NIO </a>
//     * @see <a
//     *      href="http://www.velocityreviews.com/forums/t297832-nio-best-practice.html">
//     *      NIO best practice </a>
//     * @see <a href="http://www.javaperformancetuning.com/news/qotm030.shtml">
//     *      What does volatile do? </a>
//     */
//    
//    public static class PageServer {
//        
//        /**
//         * The "segments" that are being served.
//         */
//        final Segment[] segments;
//        
//        /**
//         * The thread that accepts new client connections.
//         */
//        final AcceptorThread acceptor;
//
//        /**
//         * The thread that responds to client requests.
//         */
//        final ResponderThread responder;
//
//        /**
//         * The thread that logs statistics.
//         */
//        final private StatisticsThread statistics = new StatisticsThread(
//                30 * 1000);
//
//        /**
//         * Create and start the server.
//         * 
//         * @param port
//         *            The port on which to listen for client connections.
//         * 
//         * @param segments
//         *            An array of segments that will be served.
//         * 
//         * @throws IOException
//         */
//        
//        public PageServer(int port, Segment[] segments ) throws IOException {
//
//            if( segments == null ) {
//                
//                throw new IllegalArgumentException();
//                
//            }
//            
//            this.segments = segments;
//            
//            Selector acceptSelector = Selector.open();
//
//            Selector readWriteSelector = Selector.open();
//
//            AcceptedConnectionQueue acceptedConnectionsQueue = new AcceptedConnectionQueue(
//                    readWriteSelector);
//
//            acceptor = new AcceptorThread(acceptSelector,
//                    acceptedConnectionsQueue, port, statistics);
//
//            responder = new ResponderThread(readWriteSelector,
//                    acceptedConnectionsQueue, statistics, segments );
//
//            start();
//            
//        }
//
//        /**
//         * The server is broken down into threads for accepting connections,
//         * responding to requests, and collecting statistics.
//         */
//        private void start() {
//            statistics.start();
//            responder.start();
//            acceptor.start();
//        }
//
//        /**
//         * A FIFO queue of accepted connections from clients that have not yet
//         * entered into read-write interaction with the {@link PageServer}. New
//         * client connections are placed onto the queue when they are accepted
//         * by the {@link PageServer}. They are popped off of the queue by the
//         * {@linkPageServer} when it gets around to adding them to its
//         * read-write selector.
//         * 
//         * @version $Id$
//         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
//         *         Thompson </a>
//         */
//        public static class AcceptedConnectionQueue {
//
//            /**
//             * FIFO queue of accepted client connections awaiting migration to
//             * the read-write selector.
//             */
//            final private LinkedList _connections = new LinkedList();
//
//            /**
//             * The read-write selector. This selector is notified whenever there
//             * is a new client connection on this queue. The thread handling the
//             * selector is responsible for initiating the protocol with the
//             * newly accepted clients.
//             */
//            final private Selector _readWriteSelector;
//
//            /**
//             * Create an ordered list for accepted connections.
//             * 
//             * @param readWriteSelector
//             *            The selector that is notified for new connections.
//             */
//            public AcceptedConnectionQueue(Selector readWriteSelector) {
//                if (readWriteSelector == null) {
//                    throw new IllegalArgumentException();
//                }
//                _readWriteSelector = readWriteSelector;
//            }
//
//            /**
//             * Adds the accepted channel to the list of accepted connections and
//             * notifies the selector using {@link Selector#wakeup()}.
//             * 
//             * @param acceptedChannel
//             *            The newly accepted channel.
//             */
//            public void accept(SocketChannel acceptedChannel) {
//                _connections.add(acceptedChannel);
//                _readWriteSelector.wakeup();
//            }
//
//            /**
//             * Pops off and returns an accepted channel (FIFO queue).
//             * 
//             * @return An accepted channel or <code>null</code> if there are
//             *         no accepted channels waiting in the queue.
//             */
//            public SocketChannel pop() {
//                if (_connections.isEmpty())
//                    return null;
//                return (SocketChannel) _connections.removeFirst();
//            }
//
//        } // AcceptedConnectionQueue
//
//        /**
//         * A thread that waits for new client connections. Connections are
//         * pushed onto the {@link AcceptedConnectionQueue}as they arrive and
//         * the {@link ResponderThread}is notified.
//         * 
//         * @todo Add parameter to limit the #of simultaneous connections.
//         */
//
//        public static class AcceptorThread extends Thread {
//
//            /**
//             * The server socket on which we accept new connections.
//             */
//            final private ServerSocketChannel ssc;
//
//            /**
//             * The selector for the socket on which we accept new connections.
//             */
//            final private Selector connectSelector;
//
//            /**
//             * A FIFO queue onto which we place accepted connections. Items on
//             * this queue are absorbed by the {@link ResponderThread}.
//             */
//            final private AcceptedConnectionQueue acceptedConnectionsQueue;
//
//            /**
//             * A thread containing counters used to track interesting events.
//             */
//            final private StatisticsThread statistics;
//
//            /**
//             * The {@link #run()}method will terminate when this becomes
//             * <code>true</code>
//             */
//            private volatile boolean shutdown = false;
//            
//            /**
//             * Create a new thread to accept client connections.
//             * 
//             * @param connectSelector
//             *            The selector that is used to detect new client
//             *            connections.
//             * @param acceptedConnectionsQueue
//             *            Accepted client connections are pushed onto this FIFO
//             *            queue and will be absorbed by the
//             *            {@link ResponderThread}.
//             * @param port
//             *            The port on which we will listen.
//             * @param statistics
//             *            A thread containing counters used to track interesting
//             *            events.
//             * @throws IOException
//             *             If the server socket could not be provisioned
//             */
//
//            public AcceptorThread(Selector connectSelector,
//                    AcceptedConnectionQueue acceptedConnectionsQueue, int port,
//                    StatisticsThread statistics) throws IOException {
//
//                super("PageServer : Accept Thread");
//
//                if (connectSelector == null || acceptedConnectionsQueue == null
//                        || statistics == null) {
//                    throw new IllegalArgumentException();
//                }
//
//                this.connectSelector = connectSelector;
//
//                this.acceptedConnectionsQueue = acceptedConnectionsQueue;
//
//                this.statistics = statistics;
//
//                /*
//                 * Setup server socket channel on identified port for
//                 * non-blocking I/O.
//                 */
//
//                ssc = ServerSocketChannel.open();
//
//                ssc.configureBlocking(false);
//
//                InetSocketAddress address = new InetSocketAddress(port);
//
//                ssc.socket().bind(address);
//
//                ssc.register(this.connectSelector, SelectionKey.OP_ACCEPT);
//
//                log.info("PageServer accepting connections on: " + address
//                        + ", port=" + port);
//
//            }
//
//            /**
//             * The thread runs in a loop accepting connections. The loop blocks
//             * in {@link Selector#select()}until at least one new connection is
//             * available. If the {@link Selector}on which we are accepting
//             * connections is closed then the loop will terminate.
//             */
//            public void run() {
//
//                while ( ! shutdown ) {
//
//                    try {
//
//                        connectSelector.select();
//
//                        acceptPendingConnections();
//
//                    } catch (ClosedSelectorException ex) {
//
//                        log.info("Selector was closed.", ex);
//
//                        break;
//
//                    } catch (IOException ex) {
//
//                        statistics.acceptor_IOErrors++;
//                        
//                        log.error(ex);
//
//                    }
//
//                }
//
//                /*
//                 * Shutdown the server socket.
//                 */
//                
//                try {
//
//                    // Close the server socket.
//                    ssc.close();
//
//                    /*
//                     * Note: You MUST do another select or the port will be left
//                     * hanging in a LISTENING state.
//                     */
//                    connectSelector.select(50);
//                    
//                    log.info("Closed server socket.");
//                    
//                }
//                
//                catch( IOException ex ) {
//                    
//                    statistics.acceptor_IOErrors++;
//                    
//                    log.error( ex );
//                    
//                }
//                
//            }
//
//            /**
//             * Accepts new client connections by reading their keys from the
//             * selector.
//             * 
//             * @throws IOException
//             */
//            private void acceptPendingConnections() throws IOException {
//
//                Set readyKeys = connectSelector.selectedKeys();
//
//                for (Iterator i = readyKeys.iterator(); i.hasNext();) {
//
//                    SelectionKey key = (SelectionKey) i.next();
//
//                    i.remove();
//
//                    ServerSocketChannel readyChannel = (ServerSocketChannel) key
//                            .channel();
//
//                    SocketChannel incomingChannel = readyChannel.accept();
//
//                    /*
//                     * Add the new connection to a connection list and wake up
//                     * the readWrite selector so that it will notice the new
//                     * connection if it is currently blocked.
//                     */
//
//                    acceptedConnectionsQueue.accept(incomingChannel);
//
//                    statistics.acceptor_connectionsAccepted++;
//                    
//                    log.info("Accepted new connection from "
//                            + incomingChannel.socket().getInetAddress());
//
//                }
//
//            }
//
//            /**
//             * Tell the acceptor thread to stop running and release the server
//             * socket (synchronous operation). Once the thread stops running the
//             * page server will no longer accept new connections.
//             */
//            
//            public void shutdown() throws IOException {
//
//                // mark thread for shutdown.
//                this.shutdown = true;
//
//                // wake up the thread if it is blocked on a select.
//                connectSelector.wakeup();
//                
//                while( isAlive() ) {
//                    
//                    yield(); // yield so that the AcceptorThread can run().
//                    
//                    System.err.print('.');
//                    
//                }
//                
//                log.info("shutdown.");
//                
//            }
//
//        }
//
//        /**
//         * <p>
//         * Daemon thread periodically logs {@link PageServer}statistics.
//         * </p>
//         * <p>
//         * Note: The counter fields are declared as <code>volatile</code> so
//         * that they may be written in one thread and their updated value will
//         * be immediately visible in another thread. In general, there is a risk
//         * of lost updates if a field is not declared as volatile and it is
//         * written from multiple threads. However, we probably do NOT need to
//         * declare these fields as volatile since any given field is written
//         * only by a single thread. The main advantage of declaring the fields
//         * as volatile is that their current values will always be visible to
//         * the {@link StatisticsThread}and therefore reported by that thread.
//         * </p>
//         * 
//         * @version $Id$
//         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
//         *         Thompson </a>
//         */
//        
//        public static class StatisticsThread extends Thread {
//
//            /**
//             * Time to sleep between logging events.
//             */
//            private long _millis;
//            
//            private final Thread thread = this;
//            
//            private volatile boolean shutdown = false;
//
//            volatile long acceptor_IOErrors = 0L;
//            volatile long acceptor_connectionsAccepted = 0L;
//            
//            volatile long responder_IOErrors = 0L;
//            volatile long responder_completeRequests = 0L;
//            volatile long responder_clientStreamsClosed = 0L;
//
//            volatile long responder_readPage = 0L;
//            volatile long responder_writePage = 0L;
//            volatile long responder_prepare   = 0L;
//            volatile long responder_commit    = 0L;
//            volatile long responder_abort     = 0L;
//                        
//            /**
//             * Create a thread to log statistics. The page server starts the
//             * thread and then increments on the created thread in response to
//             * page server events. The {@link #run()}method periodically logs
//             * those counters.
//             * 
//             * @param millis
//             *            Time to sleep between logging events.
//             */
//
//            public StatisticsThread(long millis) {
//
//                super("PageServer : Statistics");
//
//                if (millis <= 0L) {
//
//                    throw new IllegalArgumentException();
//
//                }
//
//                _millis = millis;
//
//                setDaemon(true);
//            }
//
//            public void run() {
//
//                while ( ! shutdown ) {
//
//                    try {
//
//                        Thread.sleep(_millis);
//
//                    } catch (InterruptedException ex) {
//
//                        /*
//                         * Ignore - this just means that we will display the
//                         * counters sooner.
//                         */
//
//                        log.info("Interrupted.");
//                        
//                    }
//
//                    writeStatistics();
//
//                }
//
//            }
//            
//            /**
//             * Log statistics.
//             */
//            public void writeStatistics() {
//                log.info("Acceptor: IOErrors=" + acceptor_IOErrors
//                        + ", acceptedConnections="
//                        + acceptor_connectionsAccepted);
//                log.info("Responder: IOErrors=" + responder_IOErrors
//                        + ", completedRequests="
//                        + responder_completeRequests
//                        + ", clientStreamsClosed="
//                        + responder_clientStreamsClosed);
//            }
//
//            /**
//             * This is a daemon thread and will just go away. However an
//             * explicit shutdown logs the final statisitics.
//             */
//
//            public void shutdown() {
//                
//                this.shutdown = true;
//                
//                /*
//                 * Wake up the thread so that it exits its run() method. It will
//                 * call writeStatistics() when it gets interrupted or times out
//                 * on its sleep.
//                 */
//                
//                thread.interrupt();
//                
////                writeStatistics();
//
//                log.info("shutdown.");
//                
//            }
//            
//        }
//
//        /**
//         * <p>
//         * While non-blocking I/O does not block, the server must also not poll
//         * in a tight loop waiting to read from or write to a client. The
//         * buffers and control state in this class are used to avoid such
//         * polling constructions. An instance of this class is attached to each
//         * {@link SelectionKey}that is being processed by the {@link Responder}
//         * thread.
//         * </p>
//         * <p>
//         * The channel begins in a read mode. Requests are buffered in
//         * {@link #request}until they are complete or recognizably ill-formed.
//         * Either way, the channel is flipped into a write mode and a response
//         * will be written. Responses are buffered in a variable
//         * {@link #responseHeader}plus zero or more additional buffers.
//         * Responses are written back to the client as the {@link #_channel}
//         * becomes available for writing. When the complete request has been
//         * written the channel is filled into a read mode.
//         * </p>
//         * 
//         * @todo This is a half-duplex protocol and does not support
//         *       simultaneous requests and responses or asynchronous updates
//         *       from the server to the client. This will limit network
//         *       utilization and may prevent some use cases. Asynchronous reads
//         *       can be simulated by piggybacking additional data on the next
//         *       response to a client, but we would have to be careful that the
//         *       client explictly polls if it has no other requests to make and
//         *       is waiting on some asynchronous responses.
//         * 
//         * @version $Id$
//         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
//         *         Thompson </a>
//         */
//        
//        public static class Attachment
//        {
//            
//            static private Charset ascii = Charset.forName("US-ASCII");
////            static private CharsetDecoder asciiDecoder = ascii.newDecoder();
//
//            /**
//             * The channel for IO with the client.
//             */
//            final SocketChannel _channel;
//            
//            /**
//             * <p>
//             * True if reading a request and false if writing a response.
//             * </p>
//             * <p>
//             * When <code>true</code>, the request is collected in the
//             * {@link #request}buffer. Each time the {@link SelectorKey}shows
//             * up we read in some more bytes and check to see if the request is
//             * complete. If it is, then we handle the request and send back the
//             * response. Otherwise the next {@link SelectorKey}is read.
//             * </p>
//             * <p>
//             * When <code>false</code>, we are in the midst of writing a
//             * response. Responses are buffered. If the response is not
//             * completely written out by the first write on the channel, then
//             * buffers will report some remaining bytes. In this case the
//             * {@link SelectorKey}is marked for <em>write</em> operations.
//             * The next time the {@link SelectorKey}shows up we will write more
//             * of the buffered data onto the channel. Eventually the buffered
//             * response will be exhausted and the {@link SelectorKey}is marked
//             * for <em>read</em> again.
//             * </p>
//             */
//            private boolean reading = true;
//            
//            /**
//             * <p>
//             * A {@link ByteBuffer}is allocated with the maximum capacity for
//             * the protocol version {@link Protocol_0_1#SZ_MAX_REQUEST}and
//             * attached to the {@link SelectionKey}. Since a request may arrive
//             * over many packets, that buffer is used to gather up requests from
//             * the client.
//             * </p>
//             */
//            private ByteBuffer request;
//
//            /**
//             * <p>
//             * Note: This buffer is also used for variable length response
//             * headers and as such is generally the first element of the {@link
//             * #resourceBuffers} array. The field is public since the caller
//             * must:
//             * <ol>
//             * <li>{@link Buffer#clear()}the buffer</li>
//             * <li>populate it with any variable length data in the response
//             * header</li>
//             * <li>{@link Buffer#flip()}the buffer to prepare it for writing
//             * </li>
//             * <li>pass the field as the first member of the array of response
//             * buffers to
//             * {@link #sendResponse(SelectionKey, short, ByteBuffer[])}</li>
//             * </ol>
//             * Note: {@link #sendError(SelectionKey, IOException)}also uses
//             * this field to buffer the status code and the message before
//             * calling {@link #sendResponse(SelectionKey, ByteBuffer[])}.
//             * </p>
//             */
//            private ByteBuffer responseHeader;
//            
//            /**
//             * An array of one or more buffers containing a response. A
//             * scattering write is performed and data is written from each
//             * buffer in sequence in which there is {@link Buffer#remaining()}
//             * data. The total #of bytes available before the response is
//             * written is {@link #responseLength}. The total #of bytes of the
//             * response written so far is {@link #responseWritten}. The
//             * response has been completely written when these two fields are
//             * equal.
//             */
//            private ByteBuffer[] responseBuffers;
//            
//            /**
//             * The total #of bytes available before the response is written.
//             * This field is set when a new response is buffered and is not
//             * modified during the write of that response on the channel.
//             */
//            private long responseLength;
//            
//            /**
//             * The total #of bytes of the response that have been written on the
//             * channel. When this field equals {@link #responseLength}the
//             * entire response has been written and the control state should be
//             * flipped to read the next request from the channel.
//             */
//            private long responseWritten;
//            
//            /**
//             * This method is invoked before the server begins writing the
//             * response. The #of bytes remaining in each of the buffers is
//             * computed and saved on {@link #responseLength}and the
//             * {@link #reading}flag is set to <code>false</code>. The server
//             * then invokes {@link #writeResponse()}, which tracks the #of
//             * bytes actually written in {@link #responseWritten}.
//             * 
//             * @param response
//             *            An array of buffers containing the response.
//             */
//            
//            private void sendResponse(SelectionKey key, ByteBuffer[] srcs) throws IOException {
//
//                if( key == null ) {
//                    throw new IllegalArgumentException();
//                }
//                
//                if( srcs == null ) {
//                    throw new IllegalArgumentException();
//                }
//
//                if( ! reading ) {
//                    
//                    throw new IllegalStateException();
//                    
//                }
//                
//                /*
//                 * Change over to writing on the channel.
//                 */
//                
//                reading = false;
//                
//                key.interestOps(SelectionKey.OP_WRITE);
//                
//                /*
//                 * Compute the #of bytes that need to be written to the client.
//                 */
//
//                responseBuffers = srcs; // the array of buffers to be written.
//                responseLength  = 0L; // #of bytes to be written.
//                responseWritten = 0L; // #of bytes written so far.
//
//                for (int i = 0; i < srcs.length; i++) {
//
//                    responseLength += srcs[i].remaining();
//
//                }
//
//                /*
//                 * Attempt to write at least part of the response. If we can not
//                 * write the entire response now then we will continue writing
//                 * the remaining buffered data for the response the next time
//                 * this selection key comes around in our run() method.
//                 */
//                
//                writeResponse( key );
//                
//            }
//
//            /**
//             * Write out more data from a buffered response to the client. If
//             * all buffered data has been written, then flip the channel over so
//             * that we can read the next request from that client.
//             */
//            
//            public void writeResponse(SelectionKey key) throws IOException {
//
//                if( key == null ) {
//                    
//                    throw new IllegalArgumentException();
//                    
//                }
//                
//                if( reading ) {
//                    
//                    throw new IllegalStateException();
//                    
//                }
//
//                /*
//                 * Scattering write of remaining bytes in the array of response
//                 * buffers.
//                 */
//                final long nbytes = _channel.write( responseBuffers );
//
//                responseWritten += nbytes;
//
//                log.info("wrote " + nbytes + " : written=" + responseWritten
//                        + " of " + responseLength + " to write");
//
//                if( responseWritten > responseLength ) {
//                
//                    /*
//                     * We wrote too much data.
//                     */
//                    
//                    throw new AssertionError("Wrote too much data: written="
//                            + responseWritten + ", but buffered="
//                            + responseLength);
//                    
//                }
//                
//                if (responseWritten == responseLength) {
//
//                    /*
//                     * Change the channel over to read the next request.
//                     */
//
//                    reading = true;
//
//                    key.interestOps(SelectionKey.OP_READ);
//
//                    /*
//                     * Clear the request buffer before reading the next request
//                     * from the client.
//                     */
//
//                    request.clear();
//                    
//                    log.info("ready to read next request");
//
//                }
//                
//            }
//
//            /**
//             * Send an error response.
//             * 
//             * @param key
//             *            The selection key.
//             * 
//             * @param error
//             *            The status line is taken from the IOException. If
//             *            there is an error in the client request, then this
//             *            must be an instance of {@link ClientError}. If this
//             *            is an instance of {@link RedirectError}then a
//             *            redirect response will be generated. Otherwise the
//             *            error is interpreted as a server error.
//             * 
//             * @throws IOException
//             *             If there is a problem writing the response.
//             */
//
//            protected void sendError(SelectionKey key, IOException error)
//                    throws IOException {
//
//                if( ! reading ) {
//
//                    /*
//                     * If an error occurs after we have begun to write the
//                     * response then we can not use the protocol to send that
//                     * message to the client.
//                     * 
//                     * @todo I am not sure that this is the best action to take.
//                     * This should be carefully review with respect to the page
//                     * server semantics for ACID transactions.
//                     */
//
//                    log.error("Error while sending response", error );
//                    
//                    _channel.close();
//                    
//                    return;
//                    
//                }
//                
//                /*
//                 * @todo could send the stack trace if in a verbose mode.
//                 * 
//                 * @todo should log stack trace if server exception.
//                 * 
//                 * @todo this does an allocation for [chars] and then the
//                 * encoder does another for [buffer]. Can we collapse those into
//                 * operations in the [request] buffer? Make sure that we
//                 * truncate the message line if it would exceed the space
//                 * remaining in the request buffer.
//                 */
//                
//                String msg = error.getMessage();
//                
//                if( msg == null ) {
//                    
//                    /*
//                     * Note: The zero argument exception constructors have a null
//                     * detailed messaged.
//                     */
//                    
//                    msg = "";
//                    
//                }
//                
//                final short statusCode = ((error instanceof ClientError ? Protocol_0_1.STATUS_CLIENT_ERROR
//                        : (error instanceof RedirectError ? Protocol_0_1.STATUS_REDIRECT
//                                : Protocol_0_1.STATUS_SERVER_ERROR)));
//                
//                log.info(msg);
//
//                responseHeader.clear();
//                responseHeader.putShort(statusCode);
//                responseHeader.flip();
//
//                CharBuffer chars = CharBuffer.allocate(msg.length()+2);
//                chars.put(msg);
//                chars.put("\n\n"); // double newline terminates status line.
//                chars.flip();
//                
//                // Translate the Unicode characters into ASCII bytes.
//                ByteBuffer buffer = ascii.newEncoder().encode(chars);
//
//                // Send the response.
//                sendResponse(key, new ByteBuffer[] { responseHeader, buffer });
//                
//            }
//
//            /**
//             * Allocate the buffers and control state for managing non-blocking
//             * IO for requests and responses on a channel. Each instance of this
//             * class is {@link SelectorKey#attached(Object) attached}to the
//             * {@link SelectorKey}for a specific client.
//             */
//            
//            public Attachment(SocketChannel channel, int requestBufferCapacity) {
//                
//                if( channel == null ) {
//                    
//                    throw new IllegalArgumentException();
//                    
//                }
//                
//                if( requestBufferCapacity <= 0 ) {
//                    
//                    throw new IllegalArgumentException();
//                    
//                }
//                
//                _channel = channel;
//                
//                /*
//                 * @todo consider use of a direct buffer here. direct buffers
//                 * absorb O/S memory, but they are passed along directly to the
//                 * O/S I/O routines. So it all depends on what we are using the
//                 * buffer for. If the buffered request is being passed down to a
//                 * memory mapped file (or other nio mechanism) then it should be
//                 * direct.
//                 */
//                
//                request = ByteBuffer.allocate(requestBufferCapacity);
//
//                /*
//                 * @todo Set this based on the maximum variable length response
//                 * header (plus some added capacity for error messages once the
//                 * latter are formatted into this buffer).
//                 */ 
//                
//                responseHeader = ByteBuffer.allocate(1024);
//                
//            }
//            
//        }
//        
//        /**
//         * Thread reads requests and writes responses.
//         */
//        public static class ResponderThread extends Thread {
//
//            private static final int READ_BUFFER_SIZE = 16;
//
//            final private Selector readWriteSelector;
//
//            /**
//             * A list of connections that were accepted by the
//             * {@link AcceptorThread} and need to be picked up by the
//             * {@link ResponderThread}.
//             */
//            final private AcceptedConnectionQueue acceptedConnections;
//            
//            /**
//             * The thread on which we increment counters for interesting events.
//             */
//            final private StatisticsThread statistics;
//
//            /**
//             * Map from {@link OId}to {@link Segment}. The keys of the map
//             * have had their page and slot components masked off.
//             */
//
//            final private HashMap segmentTable = new HashMap();
//            
//            /**
//             * The {@link #run()}method will terminate when this becomes
//             * <code>true</code>
//             */
//            private volatile boolean shutdown = false;
//
//            /**
//             * Create a thread to handle the client-sever protocol.
//             * 
//             * @param readWriteSelector
//             *            The selector on which we detect client events.
//             * 
//             * @param acceptedConnectionsQueue
//             *            A FIFO queue of accepted client connections.
//             *            Connections are removed from this list by this thread
//             *            when it begins to handle the protocol for a newly
//             *            connected client.
//             * 
//             * @param statistics
//             *            A thread containing counters that are incremented in
//             *            response to interesting events.
//             * 
//             * @throws IOException
//             */
//
//            public ResponderThread(Selector readWriteSelector,
//                    AcceptedConnectionQueue acceptedConnections,
//                    StatisticsThread statistics, Segment[] segments ) throws IOException {
//
//                super("PageServer : Reader-Writer");
//
//                if (readWriteSelector == null || acceptedConnections == null
//                        || statistics == null || segments == null) {
//                    
//                    throw new IllegalArgumentException();
//                    
//                }
//                                
//                this.readWriteSelector = readWriteSelector;
//                
//                this.acceptedConnections = acceptedConnections;
//                
//                this.statistics = statistics;
//
//                for( int i=0; i<segments.length; i++ ) {
//                    
//                    Segment segment = segments[ i ];
//
//                    log.info("Registering segment: "+segment.getSegmentId());
//                    
//                    if( segmentTable.put(segment.getSegmentId(), segment) != null ) {
//                        
//                        throw new IllegalArgumentException(
//                                "Duplicate registration for segment="
//                                        + segment.getSegmentId());
//                        
//                    }
//                    
//                }
//                
//                log.info("");
//
//            }
//
//            /**
//             * Thread runs in a loop registering new client connections,
//             * blocking on {@link Selector#select()}for one or more requests,
//             * and then processing new client requests. A client request may
//             * arrive in multiple chunks and those chunks are assembled into a
//             * complete request before the semantics of the request are
//             * executed.
//             */
//            public void run() {
//
//                while ( ! shutdown ) {
//
//                    try {
//
//                        // Register newly accepted client channels.
//                        registerNewChannels();
//
//                        /*
//                         * Block until we can either read client request(s)
//                         * and/or write more data in a buffered response to some
//                         * client (or until awakened).
//                         */
//
//                        int readyKeyCount = readWriteSelector.select();
//
//                        if (readyKeyCount > 0) {
//
//                            Iterator itr = readWriteSelector.selectedKeys().iterator();
//
//                            while( itr.hasNext() ) {
//                            
//                                SelectionKey key = (SelectionKey) itr.next();
//                                
//                                itr.remove();
//
//                                int readyOps = key.readyOps();
//                                
//                                if( ( readyOps & SelectionKey.OP_READ ) == 1 ) {
//
//                                    /*
//                                     * Requests may be read in one pass or in
//                                     * many. This may be the first time we read
//                                     * data for this request, it may be filling
//                                     * in a partial read, and it may result in a
//                                     * complete request. In the latter case the
//                                     * request is dispatched, a response is
//                                     * buffered, the channel is switched over to
//                                     * OP_WRITE for this client and we begin
//                                     * writing the response.
//                                     */
//                                    
//                                    readRequest(key);
//                                    
//                                } else if( ( readyOps & SelectionKey.OP_WRITE ) == 1) {
//                                    
//                                    /*
//                                     * Like requests, responses may be written
//                                     * in multiple operations. This is a
//                                     * continuation write of a buffered
//                                     * response. Normally at least some bytes
//                                     * will have already been written.
//                                     */
//                                    
//                                    ((Attachment)key.attachment()).writeResponse(key);
//                                    
//                                } else {
//
//                                    /*
//                                     * The SelectionKey should only be
//                                     * registered for either OP_READ -or-
//                                     * OP_WRITE.
//                                     */
//                                    throw new AssertionError();
//                                    
//                                }
//                                
//                            }
//
//                        }
//
//                    } catch (IOException ex ) {
//
//                        /*
//                         * IO error.
//                         * 
//                         * @todo Is this always in communication with a specific
//                         * client? Does an error represent a network partition
//                         * event or intermittant problem that we need to respond
//                         * to intelligently?
//                         */
//                        
//                        statistics.responder_IOErrors++;
//                        
//                        log.error( ex.getMessage(), ex );
//
//                    } catch (Throwable t) {
//
//                        /*
//                         * @todo gather stats on other errors too or maybe just
//                         * die since we should not operate a page server with
//                         * errors. There are actually a lot of ways in which the
//                         * code could get into this clause and they could vary
//                         * drammatically in their severity.
//                         */
////                        statistics.responder_IOErrors++;
//                        
//                        log.error( t.getMessage(), t );
//
//                    }
//
//                }
//
//                /*
//                 * Note: This does not close existing client connections. They
//                 * are left to fend for themselves.
//                 */
//                
//            }
//
//            /**
//             * <p>
//             * Accepts new {@link SocketChannel client connections}in a FIFO
//             * manner from the {@link AcceptedConnectionQueue}provided to the
//             * constructor, registers them with our selector and attaches a key
//             * object used to gather partial requests into complete requests. An
//             * {@link Attachment}is added to the {@link SelectionKey}to manage
//             * the request and response buffers and their control state.
//             * </p>
//             * 
//             * @throws IOException
//             */
//
//            protected void registerNewChannels() throws IOException {
//
//                SocketChannel channel;
//
//                while (null != (channel = acceptedConnections.pop())) {
//
//                    channel.configureBlocking(false);
//                    
//                    /*
//                     * Begin the channel ready to read from the client.
//                     * 
//                     * @todo If we generate a response when the client connects
//                     * then we would change this to begin with OP_WRITE. For
//                     * example, we might send out a summary of active segments
//                     * on this host.
//                     * 
//                     * @todo If we implement a full duplex protocol
//                     * (asynchronous messaging) then we need to always use
//                     * (OP_READ|OP_WRITE).
//                     */
//                    channel.register(readWriteSelector, SelectionKey.OP_READ,
//                            new Attachment(channel,Protocol_0_1.SZ_MAX_REQUEST));
//
//                }
//
//            }
//
//            /**
//             * Read a request from a channel and appends it into a per channel
//             * buffer object. When the complete request has been buffered it is
//             * then dispatched.
//             * 
//             * @param key
//             *            The selection key.
//             * 
//             * @throws IOException
//             */
//
//            protected void readRequest(SelectionKey key) throws IOException {
//
//                SocketChannel incomingChannel = (SocketChannel) key.channel();
//
//                Socket incomingSocket = incomingChannel.socket();
//
//                Attachment attachment = (Attachment) key.attachment();
//                
//                ByteBuffer readBuffer = attachment.request;
//
//                /*
//                 * Accumulate bytes into the read buffer. If complete request is
//                 * on hand then process the request.
//                 * 
//                 * Note: buffers are NOT thread safe. This is not an issue here
//                 * since we are single threaded.
//                 */
//                
//                try {
//                    
//                    int bytesRead = incomingChannel.read(readBuffer);
//                    
//                    if( bytesRead == -1 ) {
//                        
//                        /*
//                         * No more data is available from this socket.
//                         * 
//                         * @todo I believe that we don't want to immediately
//                         * close the incomingChannel and its socket since that
//                         * creates a 2MLS delay before the socket can be reused.
//                         * Instead we want to give the client an opportunity to
//                         * close the socket and then eventually close it
//                         * ourselves if the client does not get around to it.
//                         */
//                        
//                        statistics.responder_clientStreamsClosed++;
//                        
//                        key.cancel();
//                        
//                        incomingChannel.close();
//                        
//                        return;
//                        
//                    }
//                    
//                    log.info("bytesRead="+bytesRead);
//                    
//                    // dispatch handling of the request and production of
//                    // the response.
//                    if( handleRequest(key,attachment,readBuffer) ) {
//                    
//                        // The complete request was processsed.
//                        statistics.responder_completeRequests++;
//
//                    }
//                    
//                } catch (IOException ex) {
//                    
//                    statistics.responder_IOErrors++;
//
//                    attachment.sendError(key,ex);
//                
//                }
//                
//            }
//
//            /**
//             * Handle the request. This first examine the request to see if it
//             * is complete. If the request is complete and well formed, then its
//             * semantics are executed. If the request is incomplete and not
//             * ill-formed then the method will return <code>false</code> and
//             * the server will look for more data for this request.
//             * 
//             * @param key
//             *            The selection key.
//             * 
//             * @param attachment
//             *            The object attached to that selection key.
//             * 
//             * @param request
//             *            The {@link Attachment#request}buffer for that
//             *            <i>attachment </i>. The request needs to be tested
//             *            using absolute <i>get() </i> operations so that the
//             *            <em>position</em> and <em>limit</em> attributes on
//             *            the buffer are not modified as a side effect of this
//             *            test.
//             * 
//             * @return <code>true</code> iff the request was complete and
//             *         well-formed. <code>false</code> if the request was
//             *         incomplete and more data needs to be buffered.
//             * 
//             * @throws ClientError
//             *             If the request is invalid.
//             * 
//             * @throws IOException
//             *             If there is a problem writing a response.
//             */
//
//            protected boolean handleRequest(SelectionKey key,
//                    Attachment attachment, ByteBuffer request)
//                    throws IOException
//            {
//                
//                // #of bytes currently in the request buffer.
//                final int bytesAvail = request.position();
//                
//                if( bytesAvail < Protocol_0_1.SZ_HEADER ) {
//                    /*
//                     * Do not bother to test for validity until we have the
//                     * entire header.
//                     */
//                    log.info("partial header");
//                    return false;
//                }
//                
//                // position in buffer that we are examining.
//                int pos = 0;
//                
//                // magic
//                final int magic = request.getInt(pos);
//                pos += Protocol_0_1.SZ_MAGIC;
//                if (Protocol_0_1.MAGIC != magic) {
//                    throw new ClientError("Bad magic: expected="
//                            + Integer.toHexString(Protocol_0_1.MAGIC)
//                            + ", not " + Integer.toHexString(magic));
//                }
//
//                // version
//                final short version = request.getShort(pos);
//                pos += Protocol_0_1.SZ_VERSION;
//                if (Protocol_0_1.VERSION != version) {
//                    throw new ClientError("Wrong version: expected="
//                            + Protocol_0_1.VERSION + ", not " + version);
//                }
//
//                // operator
//                final short op = request.getShort(pos);
//                pos += Protocol_0_1.SZ_OP;
//                log.info("operator="+op);
//                switch( op ) {
////                case Protocol_0_1.OP_LOCK:
////                    break;
//                case Protocol_0_1.OP_READ: {
//                    if( bytesAvail < Protocol_0_1.SZ_READ_REQUEST ) {
//                        // partial request.
//                        return false;
//                    }
//                    long txId = request.getLong(pos); pos+= Protocol_0_1.SZ_LONG;
//                    long pageId = request.getLong(pos); pos += Protocol_0_1.SZ_LONG;
//                    log.info("READ(txId="+txId+",pageId="+new OId(pageId)+")");
//                    /*
//                     * prepare the response.
//                     * 
//                     * @todo will segment expose ByteBuffer or byte[]?
//                     */
//                    Segment segment = getSegment(pageId);
//                    byte[] data = segment.read(OId.getPage(pageId));
//                    ByteBuffer responseHeader = attachment.responseHeader;
//                    responseHeader.clear();
//                    responseHeader.putShort(Protocol_0_1.STATUS_OK);
//                    responseHeader.putInt(data.length);
//                    responseHeader.flip();
//                    attachment.sendResponse(key, new ByteBuffer[] {
//                            responseHeader, ByteBuffer.wrap(data) });
//                    return true; // handled complete request.
//                }
//                case Protocol_0_1.OP_WRITE: {
//                    if( bytesAvail < Protocol_0_1.SZ_WRITE_REQUEST ) {
//                        // partial request.
//                        return false;
//                    }
//                    long txId = request.getLong(pos); pos+= Protocol_0_1.SZ_LONG;
//                    long pageId = request.getLong(pos); pos += Protocol_0_1.SZ_LONG;
//                    int nbytes = request.getInt(pos); pos += Protocol_0_1.SZ_INT;
//                    log.info("WRITE(txId="+txId+",pageId="+new OId(pageId)+",nbytes="+nbytes+")");
//                    Segment segment = getSegment(pageId);
//                    if( nbytes != segment.getPageSize() ) {
//                        throw new ClientError("pageSize: expected="+segment.getPageSize()+", but found="+nbytes);
//                    }
//                    int nrequired = Protocol_0_1.SZ_WRITE_REQUEST + nbytes;
//                    if( bytesAvail < nrequired ) return false; // waiting on byte[].
//                    if( bytesAvail > nrequired ) throw new ClientError("Request overrun.");
//                    log.debug( "request: pos="+request.position()+", limit="+request.limit()+", capacity="+request.capacity() );
//                    // @todo efficient use of ByteBuffer to write on segment.
//                    byte[] data = new byte[ nbytes ];
//                    request.position(Protocol_0_1.SZ_WRITE_REQUEST);
//                    request.get(data);
//                    segment.write(OId.getPage(pageId),data);
//                    /*
//                     * prepare the response.
//                     */
//                    ByteBuffer responseHeader = attachment.responseHeader;
//                    responseHeader.clear();
//                    responseHeader.putShort(Protocol_0_1.STATUS_OK);
//                    responseHeader.flip();
//                    attachment.sendResponse(key,new ByteBuffer[]{responseHeader});
//                    return true; // handled complete request.
//                }
//                default:
//                    throw new ClientError("Unknown operation code=" + op);
//                }
//                
//            }
//
//            /**
//             * <p>
//             * Return the segment fronted by this page server that logically
//             * contains the specified page.
//             * </p>
//             * <p>
//             * Note: the page may or may not exist within the segment, but that
//             * determination can only be made by attempting to access the page
//             * in the segment.
//             * </p>
//             * 
//             * @param pageId
//             *            The page identifier (including the partition and
//             *            segement components and having a zero slot component).
//             * 
//             * @return The segment that logically contains the specified page.
//             * 
//             * @exception RedirectError
//             *                if the page does not belong to a segment fronted
//             *                by this page server.
//             */
//            
//            public Segment getSegment( long pageId ) throws RedirectError {
//                
//                /*
//                 * Mask off the page and slot components.
//                 */
//                final OId segmentId = new OId( OId.maskPageAndSlot(pageId) );
//                
//                /*
//                 * Lookup the segment in a hash table.
//                 */
//                Segment segment = (Segment) segmentTable.get( segmentId );
//
//                if( segment != null ) {
//                    
//                    /*
//                     * Found.
//                     */
//                    
//                    return segment;
//                    
//                } else {
//                
//                    /*
//                     * Not found - the client will have to look elsewhere for
//                     * this segment.
//                     */
//                    
//                    log.info("Segment not found: "+segmentId);
//                    
//                    throw new RedirectError("Segment not found: "+segmentId);
//                    
//                }
//                
//            }
//            
//            /**
//             * Shutdown the responder thread (synchronous operation). This
//             * method blocks until the responder thread halts. Clients are not
//             * explicitly closed. They should detect when the channel with the
//             * server shuts down and close the socket from their end in order to
//             * release resources in the most expedient manner.
//             */
//            
//            public void shutdown() {
//
//                // mark thread for shutdown.
//                this.shutdown = true;
//
//                // wake up the thread if it is blocked on a select.
//                readWriteSelector.wakeup();
//
//                while( isAlive() ) {
//
//                    yield(); // yield so that the responder can run().
//                    
//                    System.err.print('.');
//                    
//                }
//
//                log.info("shutdown.");
//
//            }
//            
//        }
//
//        /**
//         * Shutdown the page server.
//         */
//        
//        public void shutdown() throws IOException {
//            
//            acceptor.shutdown();
//            
//            responder.shutdown();
//            
//            statistics.shutdown();
//            
//        }
//        
//        
//        /**
//         * Used to indicate that a segment is not available from this page
//         * server.
//         * 
//         * @version $Id$
//         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
//         *         Thompson </a>
//         */
//        public static class RedirectError extends IOException
//        {
//
//            public RedirectError() {}
//            
//            public RedirectError(String msg) {super(msg);}
//            
//            public RedirectError(String msg,Throwable rootCause) {
//                super(msg);
//                initCause(rootCause);
//            }
//            
//            public RedirectError(Throwable rootCause) {
//                super();
//                initCause(rootCause);
//            }
//            
//        }
//
//        /**
//         * Used to indicate errors in a request made by a client of the server.
//         * 
//         * @version $Id$
//         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
//         *         Thompson </a>
//         */
//        public static class ClientError extends IOException
//        {
//
//            public ClientError() {}
//            
//            public ClientError(String msg) {super(msg);}
//            
//            public ClientError(String msg,Throwable rootCause) {
//                super(msg);
//                initCause(rootCause);
//            }
//            
//            public ClientError(Throwable rootCause) {
//                super();
//                initCause(rootCause);
//            }
//            
//        }
//        
//    } // PageServer
//
//    /**
//     * A simulated segment containing a fixed number of fixed size pages.  The
//     * page data is stored in memory and is not persistent.
//     * 
//     * @version $Id$
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     */
//
//    public static class Segment
//    {
//
//        /**
//         * The segment identifier as a long integer. The page and slot
//         * components are ZERO. The partition and segment components are
//         * significant.
//         */
//        final private OId segmentId;
//        
//        /**
//         * The page size for the segment.
//         */
//        final private int pageSize;
//        
//        /**
//         * The #of pages in the segment.
//         */
//        final private int numPages;
//        
//        /**
//         * The contents of each page in the segment.
//         */
//        final private byte[][] pages;
//
//        /**
//         * The segment identifier as a long integer. The page and slot
//         * components are ZERO. The partition and segment components are
//         * significant.
//         */
//        public OId getSegmentId() {
//            return segmentId;
//        }
//        
//        /**
//         * The page size for the segment.
//         */
//        public int getPageSize() {
//            return pageSize;
//        }
//        
//        /**
//         * The #of pages in the segment.
//         */
//        public int getNumPages() {
//            return numPages;
//        }
//
//        /**
//         * Return a _copy_ of the page data (prevents corruption).
//         * @param pageId
//         * @return
//         */
//        public byte[] read( int pageId ) {
//            return (byte[])pages[ pageId ].clone();
//        }
//        
//        /**
//         * Save a _copy_ of the page data (prevents corruption).
//         * @param pageId
//         * @param data
//         */
//        public void write( int pageId, byte[] data ) {
//            if(data == null || data.length != pageSize ) {
//                throw new IllegalArgumentException();
//            }
//            pages[ pageId ] = (byte[]) data.clone();
//        }
//        
//        /**
//         * Create a transient in-memory segment.
//         * 
//         * @param segmentId
//         *            The segment identifier.
//         * @param pageSize
//         *            The page size for the segment.
//         * @param numPages
//         *            The number of pages in the segment. The page server
//         *            will maintain an in memory copy of this many pages.
//         *            The contents of those pages will be initially zeroed.
//         */
//        
//        public Segment(OId segmentId, int pageSize, int numPages ) {
//
//            if( pageSize <= 0 || numPages <= 0 ) {
//                
//                throw new IllegalArgumentException();
//                
//            }
//
//            // mask off the page and slot components.
//            this.segmentId = new OId(segmentId.maskPageAndSlot());
//            
//            this.pageSize = pageSize;
//        
//            this.numPages = numPages;
//        
//            this.pages = new byte[numPages][];
//        
//            for( int i=0; i<numPages; i++ ) {
//            
//                pages[ i ] = new byte[pageSize];
//            
//            }
//        
//        }
//        
//    } // Segment
//    
//    /**
//     * <p>
//     * Client used to test the page server.
//     * </p>
//     * <p>
//     * Note: <strong>The client is NOT thread-safe. </strong>
//     * </p>
//     * 
//     * FIXME Convert the clint to use non-blocking IO and verify support for use
//     * cases involving asynchronous operations including: (a) distributed shared
//     * page cache among clients; (b) scattering All Available Writes as part of
//     * ROWAA policy; and (c) parallelized scans of distributed data structures
//     * scattered over more than one segment. This will probably require an event
//     * reporting model in which asynchronous events are queued and then
//     * dispatched to the client in a separate thread.
//     * 
//     * FIXME Generalize client to more than one page server, including both
//     * multiple remote page servers and a local page server fronting local
//     * segments. The latter could be handled using sockets as well but the
//     * connection should be to localhost so that the traffic does not appear on
//     * the network. Segments on the localhost page server should be privledged
//     * over remote copies of the same segment for READ. There can actually be
//     * multiple page servers running on a single host in order to exploit SMP
//     * architectures, but the benefit from multiple same host instances is not
//     * expected to be great since the server IO system is likely to be the
//     * bottlneck. However, this is a useful configuration for some testing
//     * purposes. More interesting is running multiple clients on the same host
//     * to divide up the work in a single transaction since that applies the
//     * other CPUs to a possibly compute intensive problem while focusing a
//     * single CPU on supporting the disk IO.
//     * 
//     * @version $Id$
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson
//     *         </a>
//     */
//    public static class PageClient implements IPageServer {
//
//        /**
//         * The channel over which the client is communicating to the page server.
//         */
//        final private SocketChannel channel;
//
//        /**
//         * Buffer used for formatting requests and reading responses.  It must
//         * be {@link Buffer#clear() cleared} before writing each request and before
//         * reading each response.
//         */
//        private ByteBuffer buf = ByteBuffer.allocate(Protocol_0_1.SZ_MAX_REQUEST);
//
//        /**
//         * Opens a connection to a page server on this host using the indicated
//         * port. The channel is configured for <strong>blocking </strong> I/O.
//         * The rationale here is that the client is single threaded and its
//         * operations are synchronous so the use of non-blocking I/O does not
//         * provide any advantages.  For example, a page read() operation must
//         * be satisified before the client can take any other action.
//         * 
//         * @param port
//         *            The port.
//         * 
//         * @throws IOException
//         */
//
//        public PageClient(int port) throws IOException {
//            
//            channel = SocketChannel.open();
//            
//            /*
//             * The socket address to which the client will make its connection.
//             */
//            InetSocketAddress isa = new InetSocketAddress(InetAddress
//                    .getLocalHost(), port);
//            
//            /*
//             * Connect to the page server identified by that socket address.
//             */
//            
//            channel.connect(isa);
//
//            /*
//             * Set timeout on the client socket.
//             * 
//             * @todo I am not seeing an exception even when blocked waiting to
//             * read data.
//             * 
//             * @todo make configuration parameter.
//             */
//
//            channel.socket().setSoTimeout(500);
//
//            log.info("soTimeout="+channel.socket().getSoTimeout());
//            
////            channel.configureBlocking(false);
//
//            log.info("Connected: "+isa);
//            
//        }
//        
//        public String toString() {
//            
//            return super.toString() + "{connected=" + channel.isConnected()
//                    + ", open=" + channel.isOpen() + ", registered="
//                    + channel.isRegistered() + ", connectionPending="
//                    + channel.isConnectionPending() + ", blocking="
//                    + channel.isBlocking() + "}";
//            
//        }
//
//        public void lock(long tx,OId oid, boolean readOnly,long timeout) {
//            throw new UnsupportedOperationException();
//        }
//        
//        public void write(long tx, OId pageId, byte[] data ) throws IOException {
//            /*
//             * @todo param check; change byte[] param to ByteBuffer for efficiency.
//             * 
//             * @todo use scattering write with pre-existing buffer for request
//             * header less the operator. If the client socket is single threaded
//             * (and it has to be, right), then we can allocate the buffer to be
//             * large enough to hold the operator as well.
//             * 
//             * @todo modify interface to pass in ByteBuffer for [data] so that
//             * it can be passed through by a scattering write thereby reducing
//             * data copying and memory allocation. If we can use another buffer
//             * for the other parameters, or a fixed size request header for all
//             * parameters other than the data, then we can eliminate most of the
//             * allocation.
//             * 
//             * @todo Ok. The gathering write accepts an array of buffers. We can
//             * write all of them. We set the limit on buffers that have variable
//             * length content so that only as much data as is relevant is
//             * actually available for writing. Before writing we need to reset
//             * (or set) the buffer position and the limit as necessary. The
//             * write() is synchronous, so the client is single threaded and life
//             * is easy.
//             * 
//             * [Some types of channels, depending upon their state, may write
//             * only some of the bytes or possibly none at all. A socket channel
//             * in non-blocking mode, for example, cannot write any more bytes
//             * than are free in the socket's output buffer. ]
//             * 
//             * [This method may be invoked at any time. If another thread has
//             * already initiated a write operation upon this channel, however,
//             * then an invocation of this method will block until the first
//             * operation is complete.]
//             * 
//             * @todo ^^^ Verify that a non-blocking client writing on a socket
//             * (a) can do partial writes so that we have to iterate until all
//             * buffers have been written; and (b) either does or does not permit
//             * interleaving of other writes by other threads. Given that a
//             * partial write can occur, I can't see how a multi-threaded
//             * non-blocking _client_ could prevent interleaving of the writes on
//             * the channel. I am not sure what use cases exist for concurrent
//             * writes by a client given that there can be at most one writer on
//             * any given channel. The most obvious one would be that a single
//             * client could write on multiple channels each backed by a
//             * different _segment_. That would make it possible to scatter
//             * segment updates with high concurrency and also suggests that
//             * there would be one page server per segment. It sounds like the
//             * 2-phase commit logic is moving into the client as well as the
//             * logic to connect to the appropriate segment servers. This seems
//             * easier in a way with only two drawbacks: (1) it consumes one
//             * socket per client per segment on both the client and the server
//             * to which the client is connected for that segment; and (2) page
//             * cache and transaction coordination among clients needs some more
//             * thought.
//             * 
//             * @todo Since the client is single threaded, make sure that we can
//             * piggyback pages when writing or reading and that we can flag that
//             * a commit should follow a write or a commit or abort should follow
//             * a read. If we introduce an object (row) update protocol, then do
//             * that to.
//             */
//            final short op = Protocol_0_1.OP_WRITE;
//            buf.clear();
//            buf.putInt(Protocol_0_1.MAGIC);
//            buf.putShort(Protocol_0_1.VERSION);
//            buf.putShort( op );
//            buf.putLong(tx);
//            buf.putLong(pageId.toLong());
//            buf.putInt(data.length);
//            buf.put( data );
//            buf.flip();
//            long nwritten = 0L;
//
//            final int limit = buf.limit();
//            while( nwritten < limit ) {
//                nwritten += channel.write(buf);
//                log.info("wrote "+nwritten+" of "+limit+" bytes.");
//            }
//            
//            handleResponse( op, channel );
//            
//        }
//        
//        public byte[] read(long tx, OId pageId ) throws IOException {
//            final short op = Protocol_0_1.OP_READ;
//            buf.clear();
//            buf.putInt(Protocol_0_1.MAGIC);
//            buf.putShort(Protocol_0_1.VERSION);
//            buf.putShort(op);
//            buf.putLong(tx);
//            buf.putLong(pageId.toLong());
//            buf.flip();
//            long nwritten = 0L;
//
//            final int limit = buf.limit();
//            while( nwritten < limit ) {
//                nwritten += channel.write(buf);
//                log.info("wrote "+nwritten+" of "+limit+" bytes.");
//            }
//            
//            return ( byte[]) handleResponse( op, channel );
//            
//        }
//
//        public void prepare(long tx) {
//            throw new UnsupportedOperationException();
//        }
//
//        public void commit(long tx, boolean releaseLocks, boolean syncToDisk) {
//            throw new UnsupportedOperationException();
//        }
//
//        public void abort(long tx) {
//            throw new UnsupportedOperationException();
//        }
//
//        /**
//         * Read the response from the server. Any error conditions or redirects
//         * are logged at appropriate levels. A normal response is handled
//         * according to the semantics of the operation.
//         * 
//         * @param op
//         *            The operation that was requested.
//         * 
//         * @param channel
//         *            The channel on which we are communicating with the server.
//         * 
//         * @return An object appropriate to the request containing the response
//         *         data. For example, a byte[] for a READ operation. This is
//         *         null for operations that do not return data.
//         * 
//         * @throws IOException
//         *             If there is a problem reading the response.
//         * 
//         * @todo Write REDIRECT tests and implement support for that feature in
//         *       the client.
//         */
//        
//        protected Object handleResponse(short op, SocketChannel channel)
//                throws IOException
//        {
//            
//            /*
//             * The response header is a 16-bit signed status code. If this is an
//             * error condition, then the response body is followed by an ASCII
//             * status message and we interpret everything up to a double newline
//             * as ASCII test. Otherwise any operation specific data then follows
//             * the response header.
//             */
//            
//            buf.clear();
//            
//            int bytesRead = 0;
//            
//            while( bytesRead < 2 ) {
//                
//                // FIXME timeout, -1 returned if stream closed, etc.
//                int nbytes = channel.read(buf);
//                
//                if( nbytes == -1 ) {
//                    
//                    throw new IOException("stream closed.");
//                    
//                }
//                
//                bytesRead += nbytes;
//                
//                log.debug("read "+nbytes+" : "+bytesRead+" of "+2+" required.");
//
//            }
//            
//            // read the status code.
//            int statusCode = buf.getShort(0);
//            
//            log.debug("statusCode="+statusCode+", op="+op);
//            
//            if( Protocol_0_1.isOk( statusCode ) ) {
//                /* 
//                 * @todo extract per operation response.
//                 */
//                switch( op ) {
//                
//                case Protocol_0_1.OP_WRITE: {
//                    /*
//                     * No more data.
//                     */
//                    return null;
//                }
//                
//                case Protocol_0_1.OP_READ: {
//
//                    /*
//                     * read until we get the [nbytes] field. This is probably
//                     * already in the buffer.
//                     * 
//                     * FIXME handle timeout, -1 return, etc.
//                     */
//                    final int headerSize = Protocol_0_1.SZ_STATUS + Protocol_0_1.SZ_INT;
//                    int requiredBytesRead = headerSize; 
//                    int i = 0; // FIXME remove limit.
//                    while( bytesRead < requiredBytesRead && i++ < 4 ) {
//                        
//                        log.debug( "buf.pos="+buf.position()+", buf.limit="+buf.limit()+", buf.capacity="+buf.capacity() );
//                        
//                        int nbytes = channel.read(buf);
//                        
//                        if( nbytes == -1 ) {
//                            
//                            throw new IOException("stream closed.");
//                            
//                        }
//                        
//                        bytesRead += nbytes;                        
//                        
//                        log.debug("read "+nbytes+" : "+bytesRead+" of "+requiredBytesRead+" required.");
//                        
//                    }
//                    int pageSize = buf.getInt(2);
//                    if( pageSize < 0 || pageSize > 65536 ) {
//                        throw new IOException("pageSize="+pageSize);
//                    }
//                    log.debug("pageSize="+pageSize);
//
//                    /*
//                     * read a page of data.  some of the data may already
//                     * be in the buffer.
//                     * 
//                     * FIXME handle timeout, -1 return, etc.
//                     */
//                    requiredBytesRead += pageSize;
//                    while( bytesRead < requiredBytesRead ) {
//                        
//                        int nbytes = channel.read(buf);
//                        
//                        if( nbytes == -1 ) {
//                            
//                            throw new IOException("stream closed.");
//                            
//                        }
//                        
//                        bytesRead += nbytes;
//                        
//                        log.debug("read "+nbytes+" : "+bytesRead+" of "+requiredBytesRead+" required.");
//                        
//                    }
//                    
////                    buf.flip();
//                    buf.position(headerSize);
//                    byte[] data = new byte[pageSize];
//                    buf.get(data);
//                    return data;
//                }
//                
//                default: 
//
//                    throw new UnsupportedOperationException("op=" + op);
//
//                }
//                
//            } else if( Protocol_0_1.isRedirect( statusCode ) ) {
//                
//                /*
//                 * @todo handle redirect. will server provide possible redirect
//                 * or do we always go to the load balancer? I think that there
//                 * should be a possibility for server redirects in the protocol
//                 * even if we wind up using load balancer redirects. If the
//                 * server redirects to a specific page server, then that
//                 * information will probably wind up in the status line.
//                 * 
//                 * For now, extract message and throw exception.
//                 */
//                
//                String msg = readStatusLine( channel );
//
//                throw new IOException("Redirect: status=" + statusCode
//                        + ", message=" + msg);
//                
//            } else if( Protocol_0_1.isClientError( statusCode ) ) {
//                /*
//                 * Extract error message and throw exception.
//                 */
//                String msg = readStatusLine( channel );
//                throw new IOException("ClientError: status=" + statusCode
//                        + ", message=" + msg);
//            } else if( Protocol_0_1.isClientError( statusCode ) ) {
//                /*
//                 * Extract error message and throw exception.
//                 */
//                String msg = readStatusLine( channel );
//                throw new IOException("ServerError: status=" + statusCode
//                        + ", message=" + msg);
//            } else {
//                throw new IOException("Unknown statusCode="+statusCode); 
//            }
//            
//        }
//
//        /**
//         * <p>
//         * Read the status line from the response.
//         * </p>
//         * <p>
//         * The buffer must be flipped for reading before this method is called.
//         * This method creates a buffer into which we will read the message and
//         * decodes the current buffer contents. If the last characters decoded
//         * are a double newline then we are done. Otherwise we keep reading and
//         * appending. If a double newline shows up at any position other than
//         * the end of a read then it is an error since there should not be any
//         * more data waiting to be read after the error message in the response.
//         * </p>
//         * 
//         * @param channel
//         *            The channel on which we are communicating with the server.
//         * 
//         * @return The text of the error message.
//         * 
//         * @throws IOException
//         *             If there is a problem reading the error message.
//         * 
//         * @todo Use StringBuilder in Java 5?
//         * 
//         * @todo Reuse error message buffer across calls, just reseting it
//         *       between calls.
//         * 
//         * @todo Return the buffer to the caller to minimize copying.
//         * 
//         * @todo write tests in which we send very long messages so that we run
//         *       at least one additional read in order to get the entire
//         *       message.
//         * 
//         * @todo write tests in which we send malformed messages (trailing
//         *       bytes).
//         * 
//         * @todo write tests in which we send multiple messages in response to
//         *       multiple sequential requests and verify correct handling of
//         *       decoder reset.
//         * 
//         * FIXME Rationalize whether the caller flips and positions the buffer
//         * or this method flips and positions the buffer. Either way we need to
//         * make sure that we are not reading the status code as part of the
//         * reason line. The way that it is currently written a message that is
//         * broken into multiple reads from the channel will probably not be
//         * extracted correctly since we flip() and position(2) at the top of
//         * each loop. Also, this is not suitable for a client that does
//         * non-blocking IO.
//         * 
//         * FIXME The status line is ASCII. Consider that it may be easier to
//         * scan ASCII for a nul character that terminates the reason line and
//         * thereby greatly simplify the process of extracting a string from the
//         * response.
//         * 
//         * FIXME The status line is ASCII. Consider that it would be nice to
//         * support unicode and localized messages.
//         */
//        private String readStatusLine(SocketChannel channel)
//                throws IOException {
//
//            StringBuffer sb = new StringBuffer();
//            
//            asciiDecoder.reset(); // @todo verify reset in server also.
//
//            do {
//
//                /*
//                 * Decode whatever is currently in the buffer.
//                 */
//
//                buf.flip();
//                buf.position(2);
//                
//                CharBuffer charbuf = asciiDecoder.decode(buf);
//                
//                sb.append(charbuf);
//                
//                buf.clear();
//
//                int indexOf = sb.indexOf("\n\n"); // @todo scan from last
//                                                  // position -1 (in case breaks
//                                                  // on newline).
//
//                if (indexOf != -1) {
//
//                    if ( indexOf != sb.length() - 2 ) {
//
//                        /*
//                         * The server wrote more data after the double newline
//                         * sequence.
//                         */
//                        throw new IOException(
//                                "Too much data in response message");
//
//                    }
//
//                    /*
//                     * End of the response body.
//                     * 
//                     * Note: drop off the double newline sequence.
//                     */
//
//                    return sb.substring(0,indexOf);
//
//                }
//
//                /*
//                 * Read more data from the server.
//                 * 
//                 * FIXME timeout, -1 returned if stream closed, etc.
//                 */
//                
//                int bytesRead = channel.read(buf);
//
//                buf.flip();
//
//            } while (true);
//            
//        }
//        
//        private Charset ascii = Charset.forName("US-ASCII");
//        private CharsetDecoder asciiDecoder = ascii.newDecoder();
//
//        /**
//         * Shutdown the client.
//         * 
//         * @throws IOException
//         */
//        public void shutdown() throws IOException {
//            
//            channel.close();
//            
//            log.info("");
//            
//        }
//        
//    }
//    
//    /**
//     * Class encapsulates constants used by the page server protocol (version
//     * 0.1). All requests conforming to this protocol have the following header.
//     * 
//     * <code>
//     *  magic INT {@link #MAGIC}
//     *  version SHORT {@link #VERSION}
//     *  operator SHORT <i>varies</i>
//     * </code>
//     * 
//     * A byte[] is serialized as an INT byte count followed by that many bytes.
//     * <p>
//     * 
//     * @todo convert nbytes to unsigned short and use specialized routines for
//     *       reading and writing an integer as an unsigned short (ByteBuffer
//     *       provides for reading signed short only, so what we need is a
//     *       conversion from signed short to unsigned integer).
//     * 
//     * @todo consider having each connection be scoped to a transaction, in
//     *       which case the connection protocol would also send the txId and it
//     *       could be factored out.
//     * 
//     * @todo add read operation designed to obtain a set of pages, e.g., when
//     *       some pages have been invalidated in the cache?
//     * 
//     * @todo Add write operation to send a set of rows using a scattered write
//     *       to support writes of only dirty rows on pages. Since pages are
//     *       melded by the segment, this will work out fine (the segment also
//     *       melds the free space map in the root page and linking the pages).
//     *       The buffers would be <pageId,slotId,nbytes>[], byte[], byte[], ...
//     *       byte[]. The first buffer would identify the specific rows to be
//     *       written using a 32-bit unsigned integer coding the pageId and
//     *       slotId of the row and a 16-bit unsigned integer giving the #of
//     *       bytes in the row. The remaining buffers would contain the data for
//     *       each row.
//     * 
//     * @todo docment the error response header: <statusCode,statusLine>
//     * 
//     * @todo define shutdown protocol.
//     * 
//     * @version $Id$
//     * @author thompsonbry
//     */
//    public final static class Protocol_0_1 {
//
//        /**
//         * This is version 0.1 of the protocol. The version is two bytes. The
//         * high byte is the major version. The low byte is the minor version.
//         */
//        public static final transient short VERSION = 0x0001;
//        
//        public static final transient int OP_LOCK    = 0;
//        
//        /**
//         * Read page operation
//         * 
//         * @param txId LONG
//         * @param pageId LONG
//         * @return byte[] (INT nbytes, followed by the byte values).
//         */
//        public static final transient short OP_READ    = (short)1;
//        
//        /**
//         * Write page operation
//         * 
//         * @param txId LONG
//         * @param pageId LONG
//         * @param byte[] (INT nbytes, followed by the byte values).
//         */                        
//        public static final transient short OP_WRITE   = (short)2;
//        
//        public static final transient short OP_PREPARE = (short)3;
//        
//        
//        public static final transient short OP_COMMIT  = (short)4;
//        
//        
//        public static final transient short OP_ABORT   = (short)5;
//
//        public static final transient int SZ_BYTE  = 1;
//        public static final transient int SZ_SHORT = 2;
//        public static final transient int SZ_INT   = 4;
//        public static final transient int SZ_LONG  = 8;
//
//        public static final transient int MAGIC      = 0xdac13b42;
//        public static final transient int SZ_MAGIC   = SZ_INT;
//        public static final transient int SZ_VERSION = SZ_SHORT;
//        public static final transient int SZ_OP      = SZ_SHORT;
//        public static final transient int SZ_STATUS  = SZ_SHORT;
//                
//        /**
//         * The size of the common header for all protocol operations.
//         */
//        public static final transient int SZ_HEADER  = SZ_MAGIC + SZ_VERSION + SZ_OP;
//        
//        /**
//         * The size of the WRITE operation including all fixed length
//         * parameters. The last field is the #of bytes that must follow and
//         * which provide the data to be written.
//         */
//        public static final transient int SZ_WRITE_REQUEST
//            	= SZ_HEADER
//            	+ SZ_LONG // txId
//                + SZ_LONG // pageId 
//                + SZ_INT  // #bytes
//                ;
//
//        /**
//         * The size of the READ operation. There are no variable length
//         * parameters.
//         */
//        public static final transient int SZ_READ_REQUEST
//            	= SZ_HEADER
//            	+ SZ_LONG // txId
//                + SZ_LONG // pageId 
//                ;
//
//
//        /**
//         * The maximum request size. The largest request is a write operation
//         * writing the maximum page size.
//         */
//        public static final transient int SZ_MAX_REQUEST
//            = SZ_HEADER // header <magic, version, OP_WRITE>
//            + SZ_LONG // txId
//            + SZ_LONG // pageId
//            + SZ_INT // #bytes
//            + 2<<16 // bytes
//            ;
//
//        /*
//         * Status codes are blocked just like the HTTP status codes.
//         */
////        public static final transient short STATUS_INFO = 100;
//        public static final transient short STATUS_OK = 200;
//        public static final transient short STATUS_REDIRECT = 300;
//        public static final transient short STATUS_CLIENT_ERROR = 400;
//        public static final transient short STATUS_SERVER_ERROR = 500;
//        
//        public static final boolean isOk( int statusCode ) {
//            return statusCode >= 200 && statusCode < 300; 
//        }
//        
//        public static final boolean isRedirect( int statusCode ) {
//            return statusCode >= 300 && statusCode < 400; 
//        }
//        
//        public static final boolean isClientError( int statusCode ) {
//            return statusCode >= 400 && statusCode < 500; 
//        }
//        
//        public static final boolean isServerError( int statusCode ) {
//            return statusCode >= 500 && statusCode < 600; 
//        }
//        
//        /*
//         * @todo Develop relative and absolute buf operations for extracting
//         * unsigned 16-bit (as int) and unsigned 32-bit (as long). The promotion
//         * in data type size is required since we need Java only supports signed
//         * value. This can be used to write [nbytes] as a [short] rather than an
//         * [int].
//         */
////        /**
////         * Converts a 4 byte array of unsigned bytes to an long
////         * @param b an array of 4 unsigned bytes
////         * @return a long representing the unsigned int
////         */
////        public static final long unsignedIntToLong(byte[] b) 
////        {
////            long l = 0;
////            l |= b[0] & 0xFF;
////            l <<= 8;
////            l |= b[1] & 0xFF;
////            l <<= 8;
////            l |= b[2] & 0xFF;
////            l <<= 8;
////            l |= b[3] & 0xFF;
////            return l;
////        }
////            
////        /**
////         * Converts a two byte array to an integer
////         * @param b a byte array of length 2
////         * @return an int representing the unsigned short
////         */
////        public static final int unsignedShortToInt(byte[] b) 
////        {
////            int i = 0;
////            i |= b[0] & 0xFF;
////            i <<= 8;
////            i |= b[1] & 0xFF;
////            return i;
////        }
//
////        public static final transient Operation params[] = new Operation[] {
////                
////        };
////        
////        public final static class Operation {
////        
////            public final String name;
////            public final Parameter[] args;
////            public final Parameter ret;
////            
////            public Operation(String name,Parameter[] args,Parameter ret) {
////                
////            }
////            
////        };
////        
////        public final static class Parameter {
////            
////            public final String name;
////            public final int type;
////            
////            public Parameter( String name, int type, )
////            
////        };
//        
//    }

}
