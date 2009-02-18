/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Feb 18, 2009
 */

package com.bigdata.service;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.SocketChannel;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.Adler32;
import java.util.zip.CheckedInputStream;

import org.apache.log4j.Logger;

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.rawstore.Bytes;
import com.bigdata.util.concurrent.DaemonThreadFactory;
import com.bigdata.util.concurrent.ShutdownHelper;
import com.ibm.icu.impl.ByteBuffer;

/**
 * A class which permits resources (files) identified by a {@link UUID} to be
 * read by a remote service. This class runs one thread to accept connections
 * and thread pool to send data.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class ResourceService {

    protected static final Logger log = Logger.getLogger(ResourceService.class);

    protected static final boolean INFO = log.isInfoEnabled();

    protected static final boolean DEBUG = log.isDebugEnabled();

    /**
     * The port on which the service is accepting connections.
     */
    public final int port;

    /**
     * The server socket.
     */
    private final ServerSocket ss;

    /**
     * <code>true</code> once running and until closed.
     */
    private volatile boolean open = false;

    /**
     * Performance counters for the {@link ResourceService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo could also monitor the accept and request thread pools. The latter
     *       is the more interesting from a workload perspective.
     */
    public class Counters {
     
        /**
         * #of requests.
         */
        public long requestCount;

        /**
         * #of requests which are denied.
         */
        public long denyCount;

        /**
         * #of requests for which the resource was not found.
         */
        public long notFoundCount;

        /**
         * #of requests which end in an internal error.
         */
        public long internalErrorCount;

        /**
         * #of errors for responses where we attempt to write the file on the
         * socket.
         */
        public long writeErrorCount;

        /**
         * #of responses where we attempt to write the file on the socket.
         */
        public long nwrites;

        /**
         * #of bytes in the files sent.
         */
        public long bytesWritten;

        /**
         * The largest response written so far.
         */
        public long maxWriteSize;

        /**
         * A lock used to make updates to {@link #maxWriteSize} atomic.
         */
        final private Object maxWriteSizeLock = new Object();
        
        /**
         * #of nanoseconds sending files (this will double count time for files
         * that are served concurrently).
         */
        public long elapsedWriteNanos;
        
        synchronized public CounterSet getCounters() {

            if (root == null) {

                root = new CounterSet();

                /*
                 * #of requests and their status outcome counters.
                 */
                {
                    final CounterSet tmp = root.makePath("status");

                    tmp.addCounter("Request Count", new Instrument<Long>() {
                        public void sample() {
                            setValue(requestCount);
                        }
                    });

                    tmp.addCounter("Deny Count", new Instrument<Long>() {
                        public void sample() {
                            setValue(denyCount);
                        }
                    });

                    tmp.addCounter("Not Found Count", new Instrument<Long>() {
                        public void sample() {
                            setValue(notFoundCount);
                        }
                    });

                    tmp.addCounter("Internal Error Count",
                            new Instrument<Long>() {
                                public void sample() {
                                    setValue(internalErrorCount);
                                }
                            });
                    
                }
                
                /*
                 * writes (A write is a response where we try to write the file
                 * on the socket).
                 */
                {

                    final CounterSet tmp = root.makePath("writes");
                    tmp.addCounter("nwrites", new Instrument<Long>() {
                        public void sample() {
                            setValue(nwrites);
                        }
                    });

                    tmp.addCounter("bytesWritten", new Instrument<Long>() {
                        public void sample() {
                            setValue(bytesWritten);
                        }
                    });

                    tmp.addCounter("writeSecs", new Instrument<Double>() {
                        public void sample() {
                            final double writeSecs = (elapsedWriteNanos / 1000000000.);
                            setValue(writeSecs);
                        }
                    });

                    tmp.addCounter("bytesWrittenPerSec",
                            new Instrument<Double>() {
                                public void sample() {
                                    final double writeSecs = (elapsedWriteNanos / 1000000000.);
                                    final double bytesWrittenPerSec = (writeSecs == 0L ? 0d
                                            : (bytesWritten / writeSecs));
                                    setValue(bytesWrittenPerSec);
                                }
                            });

                    tmp.addCounter("maxWriteSize", new Instrument<Long>() {
                        public void sample() {
                            setValue(maxWriteSize);
                        }
                    });

                }

            }

            return root;

        }
        private CounterSet root = null;
        
    }

    /**
     * Performance counters for this service.
     */
    public final Counters counters = new Counters();
    
    /**
     * Start the service on any open port using a cached thread pool to handle
     * requests.
     * 
     * @throws IOException
     */
    public ResourceService() throws IOException {

        this(0/* port */, 0/* requestServicePoolSize */);

    }

    /**
     * 
     * @param port
     *            The port on which to start the service or ZERO (0) to use any
     *            open port.
     * @throws IOException
     */
    public ResourceService(int port) throws IOException {

        this(port, 0/* requestServicePoolSize */);

    }

    /**
     * 
     * @param port
     *            The port on which to start the service or ZERO (0) to use any
     *            open port.
     * @param requestServicePoolSize
     *            The size of the thread pool that will handle requests. When
     *            ZERO (0) a cached thread pool will be used with no specific
     *            size limit.
     * 
     * @throws IOException
     */
    public ResourceService(final int port, final int requestServicePoolSize)
            throws IOException {

        if (port != 0) {

            /*
             * Use the specified port.
             */
            this.port = port;

            ss = new ServerSocket(this.port);

        } else {

            /*
             * Use any open port.
             */
            ss = new ServerSocket(0);

            this.port = ss.getLocalPort();

        }

        if (INFO)
            log.info("Running on port=" + this.port);

        if (requestServicePoolSize == 0) {

            requestService = (ThreadPoolExecutor) Executors
                    .newCachedThreadPool(new DaemonThreadFactory(getClass()
                            .getName()
                            + ".requestService"));

        } else {

            requestService = (ThreadPoolExecutor) Executors.newFixedThreadPool(
                    requestServicePoolSize, new DaemonThreadFactory(getClass()
                            .getName()
                            + ".requestService"));

        }

        // Begin accepting connections.
        acceptService.submit(new AcceptTask());

    }

    public void awaitRunning(final long timeout, final TimeUnit unit)
            throws InterruptedException, TimeoutException {

        lock.lock();
        try {

            if (!open) {

                running.await(timeout, unit);

            }

            if (open)
                return;

            throw new TimeoutException();

        } finally {
            lock.unlock();
        }
        
    }
    
    private final Lock lock = new ReentrantLock();
    private final Condition running = lock.newCondition();
    
    private class AcceptTask implements Runnable {

        public void run() {

            lock.lock();
            try {
        
                open = true;
                
                running.signal();
                
            } finally {
                
                lock.unlock();
                
            }

            try {

                while (open) {

                    /*
                     * Hand off request to a pool of worker threads.
                     * 
                     * Note: The Future of this task is ignored. If there is a
                     * problem a message is logged, the client socket is closed,
                     * the execution of the task is aborted.
                     */

                    requestService.submit(new RequestTask(ss.accept()));

                }


            } catch (IOException ex) {

                if (!open) {

                    if (INFO)
                        log.info("closed.");

                    return;

                }

                log.error(ex);

            }

        }

    }
    
    /**
     * Runs a single thread which accepts connections.
     */
    private final ExecutorService acceptService = Executors
            .newSingleThreadExecutor(new DaemonThreadFactory(getClass()
                    .getName()
                    + ".acceptService"));

    /**
     * Runs a pool of threads for handling requests.
     */
    private final ExecutorService requestService;

    public boolean isOpen() {

        return open;

    }

    synchronized public void shutdown() {

        if (!isOpen()) {

            log.warn("Not running");

        }

        if (INFO)
            log.info("");

        /*
         * Immediate shutdown of the accept thread. No new requests will be
         * accepted or will start.
         */
        acceptService.shutdownNow();

        try {

            new ShutdownHelper(requestService, 10/* logTimeout */,
                    TimeUnit.SECONDS) {

                public void logTimeout() {

                    log.warn("Awaiting request service termination: elapsed="
                            + TimeUnit.NANOSECONDS.toMillis(elapsed()) + "ms");

                }

            };

        } catch (InterruptedException ex) {

            log.warn("Interrupted awaiting request service termination.");

        }

        // Note: Runnable will terminate when open == false.
        open = false;

        try {

            ss.close();

        } catch (IOException e) {

            log.warn(e);

        }

    }

    synchronized public void shutdownNow() {

        if (!isOpen()) {

            log.warn("Not running");

        }

        if (INFO)
            log.info("");

        acceptService.shutdownNow();

        requestService.shutdownNow();

        // Note: Runnable will terminate when open == false.
        open = false;

        try {

            ss.close();

        } catch (IOException e) {

            log.warn(e);

        }

    }

    /**
     * Known status codes.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public enum StatusEnum {
       
        OK(0),
        DENY(1),
        NOT_FOUND(2),
        INTERNAL_ERROR(3),
        ;
        
        private final byte b;
        
        private StatusEnum(final int b) {
            
            this.b = (byte)b;
            
        }
        
        public byte get() {
            
            return b;
            
        }
        
        public static StatusEnum valueOf(final byte b) {

            switch (b) {
            case 0:
                return OK;
            case 1:
                return NOT_FOUND;
            case 2:
                return INTERNAL_ERROR;
            default:
                throw new IllegalArgumentException("Invalid byte: " + b);
            }
            
        }
        
    }

    /**
     * Handles a request and is run (by the caller) on a worker thread pool.
     * <p>
     * The request consists of the resource {@link UUID} to be read.
     * <p>
     * The response consists for the following fields:
     * <dl>
     * <dt>status</dt>
     * <dd>A single byte. 0 is Ok. Anything else is an error. The defined
     * status codes are:
     * <dl>
     * <dt>0</dt>
     * <dd>Ok. The file will be sent.</dd>
     * <dt>1</dt>
     * <dd>Resource not found (there is no managed resource for that UUID).</dd>
     * <dt></dt>
     * <dd></dd>
     * <dt></dt>
     * <dd></dd>
     * </dl>
     * The rest of the fields only apply for normal operation. </dd>
     * <dt>length</dt>
     * <dd>A 64-bit integer in network byte order specifying the length of the
     * file.</dd>
     * <dt>data</dt>
     * <dd>the data bytes.</dd>
     * <dt>checksum</dt>
     * <dd>A 64-bit long in network byte order specifying the {@link Adler32}
     * checksum of the data bytes as computed by the service. The receiver
     * should compute the checksum on its end and verify that they agree.</dd>
     * </dl>
     * If anything goes wrong then the socket will be asynchronously closed by
     * the service.
     */
    private class RequestTask implements Runnable
    {
        
        /**
         * The client socket.
         */
        final private Socket s;
        
        /**
         * Set true once we sent the status code and any associated data in the
         * header.
         * <p>
         * Note: Once we send the status and the header (file length) we flush
         * the output stream and are committed to the response. At that point we
         * can no longer send an error code instead. All we can do is log the
         * error and close the client socket.
         */
        private boolean sentStatus = false;
        
        /**
         * 
         * @param s
         *            The client socket.
         */
        public RequestTask(final Socket s) {

            this.s = s;
            
            counters.requestCount++;

        }

        public void run()
        {
            
            if (INFO)
                log.info("localPort=" + s.getLocalPort());

            InputStream is = null;
            try {

                is = s.getInputStream();

                // the buffer size is limited to the size of a UUID.
                final DataInputStream in = new DataInputStream(
                        new BufferedInputStream(is, Bytes.SIZEOF_UUID));

                final long mostSigBits = in.readLong();
                final long leastSigBits = in.readLong();
                final UUID uuid = new UUID(mostSigBits, leastSigBits);
                
                if(INFO)
                    log.info("Requested: uuid="+uuid);
                
                final File file = getResource(uuid);
                
                if(file == null) {

                    sendError(StatusEnum.NOT_FOUND);

                    return;
                    
                }

                final long length = file.length();

                if (INFO)
                    log.info("Sending " + file + ", length=" + length
                            + ", uuid=" + uuid);
                
                // Open the file to be sent.
                final FileInputStream fis;
                try {

                    fis = new FileInputStream(file);
                    
                    /*
                     * Seek a shared lock on the file. This will prevent it from
                     * being deleted while we are sending its data and it will
                     * also prevent us from sending a file on which someone else
                     * has a write lock. If we can't get a shared lock then no
                     * worries.
                     */
                    final FileLock fileLock = fis.getChannel().tryLock(0,
                            Long.MAX_VALUE, true/* shared */);

                    if (fileLock == null) {

                        throw new IOException("Resource is locked: " + file);

                    }
                    
                    if(!fileLock.isShared()) {
                        
                        /*
                         * Do NOT hold the file lock if it is exclusive (shared
                         * lock requests convert to exclusive lock requests on
                         * some platforms). We do not want to prevent others
                         * from accessing this resource, especially not the
                         * StoreManager itself.
                         */
                        
                        fileLock.release();
                        
                    }
                    
                } catch (IOException ex) {

                    log.error(ex, ex);
                    
                    sendError(StatusEnum.INTERNAL_ERROR);
                    
                    return;
                    
                }
                
                try {

                    // Send the file.
                    sendResource(uuid, file, length, fis);
                    
                    counters.nwrites++;

                } catch (Exception ex) {
                    
                    counters.writeErrorCount++;
                    
                    // could be client death here.
                    log.warn(ex, ex);
                    
                    return;
                    
                } finally {
                    
                    try {

                        fis.close();
                        
                    } catch (Throwable t) {
                        
                        /*ignore*/
                        
                    }
                    
                }
                
            } catch (SentErrorException ex) {
            
                /*
                 * This exception is thrown by sendError(). We don't have to do
                 * anything since an error response was already sent.
                 */

            } catch (Throwable t) {
                
                /*
                 * Something unexpected. If possible we will send an error
                 * response. Otherwise we just close the client socket.
                 */
                
                try {
                
                    log.error(t, t);
                    
                    sendError(StatusEnum.INTERNAL_ERROR);
                    
                    return;
                    
                } catch (Throwable t2) {
                    
                    // ignore.
                    
                }
                
            } finally {
                
                if (is != null) {
                
                    try {
                        // close the request input stream.
                        is.close();
                    
                    } catch (IOException ex) {
                        /* ignore */
                    
                    } finally {
                    
                        is = null;
                        
                    }

                }
                
                try {
                
                    // close the client socket.
                    s.close();
                    
                } catch (IOException ex) {
                    
                    /* ignore */
                    
                }
                
            }

        }

        /**
         * Send an error response.
         * 
         * @param e
         *            The error code.
         * @throws SentErrorException
         *             normally.
         * @throws IOException
         *             if we can't write on the client socket.
         */
        private void sendError(final StatusEnum e) throws SentErrorException,
                IOException {

            assert e != null;
            assert e != StatusEnum.OK;

            switch (e) {
            case OK:
                throw new AssertionError();
            case DENY:
                counters.denyCount++;
                break;
            case NOT_FOUND:
                counters.notFoundCount++;
                break;
            case INTERNAL_ERROR:
                counters.internalErrorCount++;
            default:
                throw new AssertionError();
            }
            
            if (!sentStatus) {

                final OutputStream os = s.getOutputStream();

                try {

                    os.write(new byte[] { e.get() });

                    os.flush();
                    
                } finally {
                    
                    sentStatus = true;

                    os.close();
                    
                }
                
            }
                
            throw new SentErrorException();
            
        }

        /**
         * Sends given resource to the socket.
         * 
         * @param uuid
         *            The {@link UUID} which identifies the resource (from the
         *            request).
         * @param file
         *            The {@link File} which is to be sent.
         * @param length
         *            The length of that file in bytes.
         * @param is
         *            The {@link InputStream} reading on that file.
         * @throws IOException
         * 
         * @todo use NIO reads on {@link FileChannel} with direct
         *       {@link ByteBuffer} and transfer to {@link SocketChannel}. This
         *       might make it more difficult to compute the checksum so who
         *       knows if it is worth it. Note that this approach will also add
         *       {@link InterruptedException} to our throws list.
         */
        private void sendResource(final UUID uuid, final File file,
                final long length, final FileInputStream is) throws IOException {

            assert uuid != null;
            assert file != null;
            assert length >= 0;
            assert is != null;
            assert !sentStatus;

            long bytesWritten = 0L;
            final long begin = System.nanoTime();
            final OutputStream os = s.getOutputStream();
            try {

                // send the header.
                {

                    final DataOutputStream dos = new DataOutputStream(os);
                    
                    // status byte.
                    dos.write(new byte[] { StatusEnum.OK.get() });
                    
                    // file length.
                    dos.writeLong(length);
                    
                    // flush the data output stream.
                    dos.flush();
                    
                    bytesWritten += 1 + Bytes.SIZEOF_LONG;

                    // Note: Don't have to flush here but can't sendError anymore.
                    // os.flush();
                    
                    // presume status has been sent.
                    sentStatus = true;
                    
                }
                
                // send the data
                final long checksum;
                {

                    // the size for the writes on the socket.
                    final int BUFSIZE = Bytes.kilobyte32 * 2;
                    
                    // input stream computes checksum as it reads the data.
                    final CheckedInputStream cis = new CheckedInputStream(
                            // buffered input stream for reading from the file.
                            new BufferedInputStream(is),
                            // checksum impl.
                            new Adler32());
                    
                    final byte[] buff = new byte[BUFSIZE];
                    
                    while (true) {

                        // #of bytes read into the buffer.
                        final int read = cis.read(buff, 0, BUFSIZE);
                        
                        if (read <= 0)
                            break;
                        
                        // write on the socket.
                        os.write(buff, 0, read);

                        bytesWritten += read;
                        
                    }

                    // the checksum of the bytes read from the file.
                    checksum = cis.getChecksum().getValue();
                    
                }

                // write the checksum on the socket.
                {

                    final DataOutputStream dos = new DataOutputStream(os);
                    
                    // checksum.
                    dos.writeLong(checksum);

                    bytesWritten += Bytes.SIZEOF_LONG;
                    
                    // flush the data output stream.
                    dos.flush();
   
                }                

                // all done.
                os.flush();

                if (INFO)
                    log.info("Sent: uuid=" + uuid + ", file=" + file
                            + ", length=" + length + ", checksum=" + checksum);
                
            } finally {

                try {
                
                    os.close();
                    
                } catch (Throwable t) {
                    
                    // Ignore.
                    
                }
                
                counters.bytesWritten += bytesWritten;
                
                counters.elapsedWriteNanos += System.nanoTime() - begin;
                
                synchronized(counters.maxWriteSizeLock) {
                
                    counters.maxWriteSize = Math.max(counters.maxWriteSize,
                            bytesWritten);
                    
                }
                
            }
            
        }

    }

    /**
     * An instance of this exception is thrown internally if an error response
     * is sent. The exception is trapped and ignored. The purpose of the
     * exception is to recognize that the error response has already been
     * handled.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private static class SentErrorException extends RuntimeException {

        /**
         * 
         */
        private static final long serialVersionUID = 0L;
        
    }
    
    /**
     * Return file identified by the UUID.
     * 
     * @param uuid
     * 
     * @return The file -or- <code>null</code> if there is no file associated
     *         with that {@link UUID}.
     * 
     * @throws Exception
     *             if the resource may not be served.
     */
    abstract protected File getResource(final UUID uuid) throws Exception;
    
    /**
     * Client for a {@link ResourceService} reads a single resource from the
     * specified service, writing it into the local file system.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo should use a configurable socket factory whose configuration is
     *       shared by the {@link ResourceService}. This would allow SSL
     *       connections, etc.
     */
    public static class ReadResourceTask implements Callable<File> {

        protected static final Logger log = Logger.getLogger(ReadResourceTask.class);

        protected static final boolean INFO = log.isInfoEnabled();

        protected static final boolean DEBUG = log.isDebugEnabled();

        final InetAddress addr;

        final int port;

        final UUID uuid;

        final File file;

        /**
         * @param addr
         *            The address of the service from which the resource will be
         *            read.
         * @param port
         *            The port at which to connect to the service from which the
         *            resource will be read.
         * @param uuid
         *            The UUID which identifies the desired resource.
         * @param file
         *            The local file on which the received data will be written.
         *            The file may exist but if it exists then it must be empty.
         */
        public ReadResourceTask(final InetAddress addr, final int port,
                final UUID uuid, final File file) {

            if (addr == null)
                throw new IllegalArgumentException();
            if (port <= 0)
                throw new IllegalArgumentException();
            if (uuid == null)
                throw new IllegalArgumentException();
            if (file == null)
                throw new IllegalArgumentException();

            this.addr = addr;
            this.port = port;
            this.uuid = uuid;
            this.file = file;

        }

        /**
         * Return the {@link File} on which the resource was written.
         * 
         * @throws IOException
         *             if the file exists and is not empty.
         * @throws IOException
         *             if another process has a lock on the file.
         */
        public File call() throws Exception {

            if (INFO)
                log.info("uuid=" + uuid + ", localFile=" + file);
            
            Socket s = null;
            
            final FileOutputStream os = new FileOutputStream(file);

            try {

                final FileLock fileLock = os.getChannel().tryLock();

                if (fileLock == null) {

                    throw new IOException("File is already locked: " + file);

                }

                if (os.getChannel().size() != 0L) {

                    throw new IOException("File not empty: " + file);

                }

                s = new Socket(addr, port);
                
                // send the UUID of the resource that we want.
                {
                    
                    final DataOutputStream dos = new DataOutputStream(s
                            .getOutputStream());

                    dos.writeLong(uuid.getMostSignificantBits());
                    dos.writeLong(uuid.getLeastSignificantBits());

                    // flush the request.
                    dos.flush();
                    
                }
                
                // buffered input stream for reading from the socket.
                final InputStream is = new BufferedInputStream(s
                        .getInputStream());
                
                // read the response header.
                final long length;
                {
                    final DataInputStream dis = new DataInputStream(is);

                    final StatusEnum e = StatusEnum.valueOf(dis.readByte());

                    switch (e) {
                    case OK:
                        length = dis.readLong();
                        break;
                    default:
                        throw new IOException(e + " : uuid=" + uuid);
                    }
                }
                
                // read the data.
                final long checksum;
                {

                    // the size for the reads on the socket.
                    final int BUFSIZE = Bytes.kilobyte32 * 2;
                    
                    // input stream computes checksum as it reads the data.
                    final CheckedInputStream cis = new CheckedInputStream(is,
                            new Adler32());
                    
                    final BufferedOutputStream bos = new BufferedOutputStream(os);
                    
                    final byte[] buff = new byte[BUFSIZE];

                    long nread = 0L;
                    
                    while (nread < length) {

                        final long remaining = length - nread;

                        final int len = remaining > BUFSIZE ? BUFSIZE
                                : (int) remaining;

                        // #of bytes read into the buffer.
                        final int read = cis.read(buff, 0, len);
                        
                        if (read <= 0) {

                            // we have run out of data early.
                            throw new IOException("EOF? #read=" + nread
                                    + ", length=" + length + ", uuid=" + uuid);
                            
                        }
                        
                        nread += read;

                        if (nread > length) {

                            // we have too much data.
                            throw new IOException("EOF? #read=" + nread
                                    + ", length=" + length + ", uuid=" + uuid);

                        }
                        
                        // write on the file.
                        bos.write(buff, 0, read);
                        
                    }

                    // flush buffered writes to the file.
                    bos.flush();
                    
                    // the checksum of the bytes read from the socket.
                    checksum = cis.getChecksum().getValue();

                    if(INFO) {
                        
                        log.info("read " + nread + " bytes, checksum="
                                + checksum + ", uuid=" + uuid);
                        
                    }
                    
                }
                
                // read checksum from the socket and verify.
                {

                    final DataInputStream dis = new DataInputStream(is);

                    final long expected = dis.readLong();

                    if (expected != checksum) {

                        throw new IOException("checksum error: uuid=" + uuid);

                    }

                }

            } finally {

                // close the socket (if open).
                try {
                    if (s != null)
                        s.close();
                } catch (Throwable t) {/* ignore */
                }

                // release the file lock (if acquired).
                try {
                    os.close();
                } catch (Throwable t) {/* ignore */
                }

            }

            return file;

        }

    }

}
