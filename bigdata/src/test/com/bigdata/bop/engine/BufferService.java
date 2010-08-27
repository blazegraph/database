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

package com.bigdata.bop.engine;

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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
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

import com.bigdata.bop.BOp;
import com.bigdata.btree.IndexSegmentStore;
import com.bigdata.counters.CAT;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.io.ByteBufferInputStream;
import com.bigdata.io.ByteBufferOutputStream;
import com.bigdata.rawstore.Bytes;
import com.bigdata.service.ResourceService;
import com.bigdata.util.concurrent.DaemonThreadFactory;
import com.bigdata.util.concurrent.ShutdownHelper;

/**
 * A class which permits buffers identified by a {@link UUID} to be read by a
 * remote service. This class runs one thread to accept connections and thread
 * pool to send data.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo This class needs to be reconciled with the {@link ResourceService} in
 *       order to facilitate the exchange of files as well as buffers as part of
 *       {@link BOp} evaluation. This class was derived from the
 *       {@link ResourceService} in the HA branch. The NIO pieces of this class
 *       should be based on the work in the HA branch on the write replication
 *       pipeline. The main structure of this class is based on the
 *       {@link ResourceService} since it allows concurrent connections, which
 *       the write pipeline does not support (because it is designed
 *       specifically for a node to node streaming replication protocol),
 *       however the class as originally implemented does not allow us to keep
 *       connections open.
 * 
 * @todo If we had an {@link Adler32} native implementation then we could
 *       checksum the nio {@link ByteBuffer}s without copying the data into the
 *       Java heap. The {@link Adler32} implementation is based on zlib, which
 *       is an acceptable license. So we might create a JNI component for that.
 *       Also, there may be direct JNI calls available into the underlying zlib
 *       library from Java.
 *       <p>
 *       This could be created in a fairly straightforward manner by bundling
 *       zlib, bundling a Make file for our JNI components, and using
 *       <code>GetDirectByteBufferAddress</code> to access the data on the C
 *       heap. See <a
 *       href="http://download-llnw.oracle.com/javase/1.5.0/docs/guide/jni/spec
 *       /functions.html#nio_support">JNI Spec</a>, <a
 *       href="http://java.sun.com/products/jdk/faq/jnifaq.html">JNI FAQ</a>,
 *       and <a href="http://www.zlib.net/">ZLib project</a>.
 * 
 * @todo The change log for this class is as follows:
 *       <p>
 *       - hold open connections in order to avoid problems with socket
 *       reconnects;
 *       <p>
 *       - exchange buffers using NIO transfers rather than sending files using
 *       standard file IO and streams.
 *       <p>
 *       - done. make the counter updates thread safe using {@link CAT}s.
 *       <p>
 *       - added generics to the base class used to fetch files or buffers.
 *       <p>
 *       - added a {@link ResourceTypeEnum} byte in the request protocol (breaks
 *       compatibility for earlier versions of this service).
 */
abstract class BufferService {

	protected static final Logger log = Logger.getLogger(BufferService.class);

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
	 * Performance counters for the {@link BufferService}.
	 * 
	 * @todo could also monitor the accept and request thread pools. The latter
	 *       is the more interesting from a workload perspective.
	 */
	static public class Counters {

		/**
		 * #of requests.
		 */
		public final CAT requestCount = new CAT();

		/**
		 * #of requests which are denied.
		 */
		public final CAT denyCount = new CAT();

		/**
		 * #of requests for which the resource was not found.
		 */
		public final CAT notFoundCount = new CAT();

		/**
		 * #of requests which end in an internal error.
		 */
		public final CAT internalErrorCount = new CAT();

        /**
         * #of errors for responses where we attempt to write the requested data
         * on the socket.
         */
		public final CAT writeErrorCount = new CAT();

		/**
		 * #of responses where we attempt to write the data on the socket.
		 */
		public final CAT nwrites = new CAT();

		/**
		 * #of data bytes sent.
		 */
		public final CAT bytesWritten = new CAT();

		/**
		 * The largest response written so far.
		 */
		public long maxWriteSize;

		/**
		 * A lock used to make updates to {@link #maxWriteSize} atomic.
		 */
		final private Object maxWriteSizeLock = new Object();

		/**
		 * #of nanoseconds sending data (this will double count time for data
		 * that are served concurrently to different receivers).
		 */
		public final CAT elapsedWriteNanos = new CAT();

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
							setValue(requestCount.get());
						}
					});

					tmp.addCounter("Deny Count", new Instrument<Long>() {
						public void sample() {
							setValue(denyCount.get());
						}
					});

					tmp.addCounter("Not Found Count", new Instrument<Long>() {
						public void sample() {
							setValue(notFoundCount.get());
						}
					});

					tmp.addCounter("Internal Error Count", new Instrument<Long>() {
						public void sample() {
							setValue(internalErrorCount.get());
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
							setValue(nwrites.get());
						}
					});

					tmp.addCounter("bytesWritten", new Instrument<Long>() {
						public void sample() {
							setValue(bytesWritten.get());
						}
					});

					tmp.addCounter("writeSecs", new Instrument<Double>() {
						public void sample() {
							final double writeSecs = (elapsedWriteNanos.get() / 1000000000.);
							setValue(writeSecs);
						}
					});

					tmp.addCounter("bytesWrittenPerSec", new Instrument<Double>() {
						public void sample() {
							final double writeSecs = (elapsedWriteNanos.get() / 1000000000.);
							final double bytesWrittenPerSec = (writeSecs == 0L ? 0d : (bytesWritten.get() / writeSecs));
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
	public BufferService() throws IOException {

		this(0/* port */, 0/* requestServicePoolSize */);

	}

	/**
	 * 
	 * @param port
	 *            The port on which to start the service or ZERO (0) to use any
	 *            open port.
	 * @throws IOException
	 */
	public BufferService(final int port) throws IOException {

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
	public BufferService(final int port, final int requestServicePoolSize) throws IOException {

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

		if (log.isInfoEnabled())
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

	/**
	 * Overridden to ensure that the service is always shutdown.
	 */
	@Override
	protected void finalize() throws Throwable {
	    
        shutdownNow();
        
        super.finalize();

	}
	
    /**
     * Wait until the service is running.
     * 
     * @param timeout
     *            The timeout.
     * @param unit
     *            The unit for the timeout.
     * @throws InterruptedException
     *             if interrupted while waiting.
     * @throws TimeoutException
     *             if the timeout expired before the service was running.
     */
    public void awaitRunning(final long timeout, final TimeUnit unit)
            throws InterruptedException, TimeoutException {

        // the start time in nanoseconds.
        final long begin = System.nanoTime();

        // the timeout in nanoseconds.
        final long nanos = unit.toNanos(timeout);
        
        // the time remaining in nanoseconds.
        long remaining = nanos;

        if (lock.tryLock(remaining, TimeUnit.NANOSECONDS)) {

            try {

                while (!open) {

                    remaining = nanos - (System.nanoTime() - begin);

                    if (!running.await(remaining, TimeUnit.NANOSECONDS)) {

                        // timeout
                        break;

                    }

                }

                if (open)
                    return;

            } finally {

                lock.unlock();
                
            }

        }

        throw new TimeoutException();

	}

	private final Lock lock = new ReentrantLock();
	private final Condition running = lock.newCondition();

    /**
     * Class handles the accept of new connections. Only a single instance of
     * this task will be executed over the life of the service.
     */
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
                     * 
                     * @todo this thread per accepted connection pattern does
                     * not allow us to keep the connections open for a given
                     * node.
                     */

					requestService.submit(new RequestTask(ss.accept()));

				}

			} catch (IOException ex) {

				if (!open) {

					if (log.isInfoEnabled())
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

    /**
     * @todo Is there a possible lock ordering problem with synchronized
     *       {@link #shutdown()} and {@link #shutdownNow()} methods and the
     *       internal {@link #lock}?
     */
	synchronized public void shutdown() {

		if (!isOpen()) {

			log.warn("Not running");

		}

		if (log.isInfoEnabled())
			log.info("");

		/*
		 * Immediate shutdown of the accept thread. No new requests will be
		 * accepted or will start.
		 */
		acceptService.shutdownNow();

		try {

			new ShutdownHelper(requestService, 10/* logTimeout */, TimeUnit.SECONDS) {

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

		if (log.isInfoEnabled())
			log.info("");

		// cancel the single AcceptTask.
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
	 */
	public enum StatusEnum {

		OK(0), DENY(1), NOT_FOUND(2), INTERNAL_ERROR(3), ;

		private final byte b;

		private StatusEnum(final int b) {

			this.b = (byte) b;

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
     * Type safe enumeration of the kinds of resources which can be served.
     */
    public static enum ResourceTypeEnum {
        FILE(0), BUFFER(1);
        private final byte b;

        private ResourceTypeEnum(final int b) {
            this.b = (byte) b;
        }
        
        public byte get() {
            return b;
        }
        
        public static ResourceTypeEnum valueOf(final byte b) {

            switch (b) {
            case 0:
                return FILE;
            case 1:
                return BUFFER;
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
	 * <dd>A single byte. 0 is Ok. Anything else is an error. The defined status
	 * codes are:
	 * <dl>
	 * <dt>0</dt>
	 * <dd>Ok. The data will be sent.</dd>
	 * <dt>1</dt>
	 * <dd>Resource not found (there is no managed resource for that UUID).</dd>
	 * <dt></dt>
	 * <dd></dd>
	 * <dt></dt>
	 * <dd></dd>
	 * </dl>
	 * The rest of the fields only apply for normal operation.</dd>
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
	private class RequestTask implements Runnable {

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

			counters.requestCount.increment();

		}

		/**
		 * Note: {@link OverlappingFileLockException}s can arise when there are
		 * concurrent requests to obtain a shared lock on the same file.
		 * Personally, I think that this is a bug since the lock requests are
		 * shared and should be processed without deadlock. However, the code
		 * handles this case by proceeding without the lock - exactly as it
		 * would handle the case where a shared lock was not available. This is
		 * still somewhat fragile since it someone does not test the
		 * {@link FileLock} and was in fact granted an exclusive lock when they
		 * requested a shared lock then this code will be unwilling to send the
		 * resource. There are two ways to make that work out - either we DO NOT
		 * use {@link FileLock} for read-only files (index segments) or we
		 * ALWAYS discard the {@link FileLock} if it is not shared when we
		 * requested a shared lock and proceed without a lock. For this reason,
		 * the behavior of this class and {@link IndexSegmentStore} MUST match.
		 * 
		 * @see IndexSegmentStore
		 * @see http://blogs.sun.com/DaveB/entry/new_improved_in_java_se1
		 * @see http://forums.sun.com/thread.jspa?threadID=5324314.
		 */
		public void run() {

			if (log.isInfoEnabled())
				log.info("localPort=" + s.getLocalPort());

			InputStream is = null;
			try {

				is = s.getInputStream();

                // the buffer size is limited to the size of a UUID.
                final DataInputStream in = new DataInputStream(
                        new BufferedInputStream(is, Bytes.SIZEOF_UUID));

                final ResourceTypeEnum resourceType = ResourceTypeEnum.valueOf(in.readByte());
                final long mostSigBits = in.readLong();
				final long leastSigBits = in.readLong();
				final UUID uuid = new UUID(mostSigBits, leastSigBits);

				if (log.isInfoEnabled())
					log.info("Requested: uuid=" + uuid);

                switch (resourceType) {
                case FILE: {
                    final File file = getResource(uuid);
                    if (file == null) {
                        sendError(StatusEnum.NOT_FOUND);
                        return;
                    }
                    sendFile(uuid, file);
                    break;
                }
                case BUFFER: {
                    final ByteBuffer buffer = getBuffer(uuid);
                    if (buffer == null) {
                        sendError(StatusEnum.NOT_FOUND);
                        return;
                    }
                    sendBuffer(uuid, buffer);
                    break;
                }
                default:
                    sendError(StatusEnum.NOT_FOUND);
                    return;
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

					log.error("Unknown error: " + t, t);

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
		private void sendError(final StatusEnum e) throws SentErrorException, IOException {

			assert e != null;
			assert e != StatusEnum.OK;

			switch (e) {
			case OK:
				throw new AssertionError();
			case DENY:
				counters.denyCount.increment();
				break;
			case NOT_FOUND:
				counters.notFoundCount.increment();
				break;
			case INTERNAL_ERROR:
				counters.internalErrorCount.increment();
				break;
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
         * Prepare and send a file.
         * 
         * @param uuid
         *            The identifier for the file resource.
         * @param file
         *            The corresponding file in the file system.
         *            
         * @throws SentErrorException
         * @throws IOException
         */
        private final void sendFile(final UUID uuid, final File file)
                throws SentErrorException, IOException {
            
            final long length = file.length();

            if (log.isInfoEnabled())
                log.info("Sending " + file + ", length=" + length + ", uuid=" + uuid);

            // Open the file to be sent.
            final FileInputStream fis;
            try {

                fis = new FileInputStream(file);

            } catch (IOException ex) {

                log.error("Sending " + file + ", length=" + length + ", uuid=" + uuid, ex);

                sendError(StatusEnum.INTERNAL_ERROR);

                return;

            }

            // try block used to ensure that we close [fis] in finally{}.
            final FileLock fileLock;
            try {

                /*
                 * Seek a shared lock on the file. This will prevent it from
                 * being deleted while we are sending its data and it will also
                 * prevent us from sending a file on which someone else has a
                 * write lock. If we can't get a shared lock then no worries.
                 * 
                 * Note: The FileLock will be released when we close the
                 * FileChannel on which it depends.
                 */
                try {

                    fileLock = fis.getChannel().tryLock(0, Long.MAX_VALUE, true/* shared */);

                    if (fileLock == null) {

                        throw new IOException("Resource is locked: " + file);

                    }

                    if (!fileLock.isShared()) {

                        /*
                         * Do NOT hold the file lock if it is exclusive
                         * (shared lock requests convert to exclusive lock
                         * requests on some platforms). We do not want to
                         * prevent others from accessing this resource,
                         * especially not the StoreManager itself.
                         */

                        fileLock.release();

                    }

                } catch (OverlappingFileLockException ex) {

                    /*
                     * Note: OverlappingFileLockException can be thrown when
                     * there are concurrent requests to obtain the same
                     * shared lock. I consider this a JDK bug. It should be
                     * possible to service both requests without deadlock.
                     */

                    if (log.isInfoEnabled())
                        log.info("Will proceed without lock: file=" + file + " : " + ex);

                } catch (IOException ex) {

                    log.error("Sending " + file + ", length=" + length + ", uuid=" + uuid, ex);

                    sendError(StatusEnum.INTERNAL_ERROR);

                    return;

                }

                // Send the file.
                sendResource(uuid, file, length, fis);

                counters.nwrites.increment();

            } catch (Exception ex) {

                counters.writeErrorCount.increment();

                // could be client death here.
                log.warn(ex, ex);

                return;

            } finally {

                try {

                    /*
                     * Close the FileChannel.
                     * 
                     * Note: This will release the FileLock if one was acquired.
                     */

                    fis.close();

                } catch (Throwable t) {

                    /* ignore */

                }

            }

		}

        /**
         * Prepare and send a {@link ByteBuffer}. The bytes from the
         * {@link ByteBuffer#position()} to the {@link ByteBuffer#limit()} will
         * be send. The position and limit are not modified.
         * 
         * @param uuid
         *            The identifier for the buffer resource.
         * @param buffer
         *            The corresponding buffer object.
         * 
         * @throws SentErrorException
         * @throws IOException
         */
        private final void sendBuffer(final UUID uuid, final ByteBuffer buffer)
                throws SentErrorException, IOException {
            
            /*
             * Get a read-only view to avoid problems with concurrent
             * modification of the buffer's position, limit, etc.
             */
            final ByteBuffer b = buffer.asReadOnlyBuffer();
            
            // The #of bytes to be transferred.
            final int remaining = buffer.remaining();

            if (log.isInfoEnabled())
                log.info("Sending " + b + ", length=" + remaining + ", uuid=" + uuid);

            // Open input stream reading from the buffer to be sent.
            final InputStream fis = new ByteBufferInputStream(b);

            // try block used to ensure that we close [fis] in finally{}.
            try {

                // Send the file.
                sendResource(uuid, b, (long) remaining, fis);

                counters.nwrites.increment();

            } catch (Exception ex) {

                counters.writeErrorCount.increment();

                // could be client death here.
                log.warn(ex, ex);

                return;

            } finally {

                try {

                    fis.close();

                } catch (Throwable t) {

                    /* ignore */

                }

            }

        }

        /**
         * Sends given resource to the socket.
         * 
         * @param uuid
         *            The {@link UUID} which identifies the resource (from the
         *            request).
         * @param resource
         *            The {@link File} or {@link ByteBuffer} which is to be sent
         *            (logging purposes only).
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
		private void sendResource(final UUID uuid, final Object resource, final long length, final InputStream is)
				throws IOException {

			assert uuid != null;
			assert resource != null;
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

					// Note: Don't have to flush here but can't sendError
					// anymore.
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

				if (log.isInfoEnabled())
					log.info("Sent: uuid=" + uuid + ", resource=" + resource + ", length=" + length + ", checksum=" + checksum
							+ ", elapsed=" + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin) + "ms");

			} finally {

				try {

					os.close();

				} catch (Throwable t) {

					// Ignore.

				}

				counters.bytesWritten.add(bytesWritten);

				counters.elapsedWriteNanos.add(System.nanoTime() - begin);

                synchronized (counters.maxWriteSizeLock) {

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
	 */
	private static class SentErrorException extends RuntimeException {

		/**
         * 
         */
		private static final long serialVersionUID = 0L;

	}

    /**
     * Return file identified by the {@link UUID}.
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
     * Return {@link ByteBuffer} identified by the {@link UUID}.
     * 
     * @param uuid
     * 
     * @return The {@link ByteBuffer} -or- <code>null</code> if there is no
     *         buffer associated with that {@link UUID}.
     * 
     * @throws Exception
     *             if the resource may not be served.
     */
    abstract protected ByteBuffer getBuffer(final UUID uuid) throws Exception;

    /**
     * Client for a {@link BufferService} reads a single resource from the
     * specified service, writing it into the local file system.
     * 
     * @param <S>
     *            The generic type of the resource identifier.
     * @param <T>
     *            The generic type of the resource object.
     * 
     * @todo should use a configurable socket factory whose configuration is
     *       shared by the {@link BufferService}. This would allow SSL
     *       connections, etc.
     */
	public static abstract class FetchResourceTask<S,T> implements Callable<T> {
		
		final InetAddress addr;

		final int port;
		
		FetchResourceTask(final InetAddress addr, final int port) {
			if (addr == null)
				throw new IllegalArgumentException();
			if (port <= 0)
				throw new IllegalArgumentException();

			this.addr = addr;
			this.port = port;
		}
		
		/**
		 * The logical identifier for the resource.
		 */
		abstract S logId();

		/**
		 * The resource.
		 */
		abstract T logResource();

		/*
		 * @todo use NIO transfers for buffers and files.
		 */
        public void transfer(final InputStream is, final OutputStream os)
                throws Exception {

			final long begin = System.nanoTime();

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
					throw new IOException(e.toString() + ", id:" + logId());
				}
			}

			// read the data.
			long nread = 0L;
			final long checksum;
			{

				// the size for the reads on the socket.
				final int BUFSIZE = Bytes.kilobyte32 * 2;

				// input stream computes checksum as it reads the data.
				final CheckedInputStream cis = new CheckedInputStream(is, new Adler32());

				final BufferedOutputStream bos = new BufferedOutputStream(os);

				final byte[] buff = new byte[BUFSIZE];

				while (nread < length) {

					final long remaining = length - nread;

					final int len = remaining > BUFSIZE ? BUFSIZE : (int) remaining;

					// #of bytes read into the buffer.
                    final int read = cis.read(buff, 0, len);

                    if (read <= 0) {

                        // we have run out of data early.
                        throw new IOException("EOF? #read=" + nread
                                + ", length=" + length + ",id=" + logId());

                    }

                    nread += read;

                    if (nread > length) {

                        // we have too much data.
                        throw new IOException("EOF? #read=" + nread
                                + ", length=" + length + ",id=" + logId());

                    }

                    // write on the file.
                    bos.write(buff, 0, read);

                }

				// flush buffered writes to the file.
				bos.flush();

				// the checksum of the bytes read from the socket.
				checksum = cis.getChecksum().getValue();

			}

			// read checksum from the socket and verify.
			{

				final DataInputStream dis = new DataInputStream(is);

				final long expected = dis.readLong();

				if (expected != checksum) {

					throw new IOException("checksum error: id=" + logId());

				}

			}

            if (log.isInfoEnabled()) {

                log.info("read "
                        + nread
                        + " bytes, resource="
                        + logResource()
                        + ", checksum="
                        + checksum
                        + ", id="
                        + logId()
                        + ", elapsed="
                        + TimeUnit.NANOSECONDS.toMillis(System.nanoTime()
                                - begin) + "ms");

			}

		}

	}

    /**
     * Task sends a request for a file's data and then receives the data onto a
     * local file.
     * 
     * @todo Receive file's data using NIO to transfer from a
     *       {@link FileChannel} to the caller using a {@link SocketChannel}.
     */
	public static class ReadResourceTask extends FetchResourceTask<UUID,File> {

		protected static final Logger log = Logger.getLogger(ReadResourceTask.class);

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
            super(addr, port);

            if (uuid == null)
                throw new IllegalArgumentException();
            if (file == null)
                throw new IllegalArgumentException();

            this.uuid = uuid;
            this.file = file;

        }

        public UUID logId() {
			return uuid;
		}

		public File logResource() {
			return file;
		}

        /**
         * Return the {@link File} on which the resource was written. If the
         * operation fails, then the caller is responsible deciding whether or
         * not the {@link File} specified to the constructor needs to be
         * deleted.
         * 
         * @throws IOException
         *             if the file exists and is not empty.
         * @throws IOException
         *             if another process has a lock on the file.
         */
		public File call() throws Exception {
			Socket s = null;
			final FileOutputStream os = new FileOutputStream(file);
			try {
		
			    if (log.isInfoEnabled())
					log.info("uuid=" + uuid + ", localFile=" + file);

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

					final DataOutputStream dos = new DataOutputStream(s.getOutputStream());

                    dos.writeByte(ResourceTypeEnum.FILE.get());
					dos.writeLong(uuid.getMostSignificantBits());
					dos.writeLong(uuid.getLeastSignificantBits());

					// flush the request.
					dos.flush();

				}

				// buffered input stream for reading from the socket.
				final InputStream is = new BufferedInputStream(s.getInputStream());

				transfer(is, os);

				return file;
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
        }

    }

    /**
     * Class sends a request for a remote {@link ByteBuffer} and then receives
     * the data into a local {@link ByteBuffer}.
     * 
     * @todo Use NIO, not streams.
     * @todo Checksum the buffer and validate the checksum at the receiver.
     */
    public static class ReadBufferTask extends
            FetchResourceTask<UUID, ByteBuffer> {

        protected static final Logger log = Logger
                .getLogger(ReadResourceTask.class);

        final UUID id;

        final ByteBuffer outbuf;

        /**
         * @param addr
         *            The address of the service from which the resource will be
         *            read.
         * @param port
         *            The port at which to connect to the service from which the
         *            resource will be read.
         * @param id
         *            The id that identifies the source ByteBuffer.
         * @param outbuf
         *            The {@link ByteBuffer} to which the data from the remote
         *            source {@link ByteBuffer} will be transferred.
         */
        public ReadBufferTask(final InetAddress addr, final int port,
                final UUID id, final ByteBuffer outbuf) {
            
            super(addr, port);

            if (id == null)
                throw new IllegalArgumentException();
            if (outbuf == null)
                throw new IllegalArgumentException();

            this.id = id;
            this.outbuf = outbuf;

		}

		public UUID logId() {
			return id;
		}

		public ByteBuffer logResource() {
			return outbuf;
		}

        /**
         * Issue a request to the remote service for the {@link ByteBuffer} on
         * that service which was identified to the constructor and then accept
         * the data into the local {@link ByteBuffer} provided to the
         * constructor.
         * 
         * @return A mutable view onto the received data within the caller's
         *         {@link ByteBuffer}. The data read from the remote service are
         *         available for reading from {@link ByteBuffer#position()} to
         *         {@link ByteBuffer#limit()} in the returned buffer.
         * 
         * @throws Exception
         *             if something goes wrong (buffer does not exist, IO error,
         *             overflow in the local buffer when receiving the data from
         *             the remote service, etc.).
         */
		public ByteBuffer call() throws Exception {
			Socket s = null;
			// duplicate to avoid side effects on the caller's position and limit.
			final ByteBuffer b = outbuf.duplicate();
			final OutputStream os = new ByteBufferOutputStream(b);
			try {
				s = new Socket(addr, port);

				// send the UUID of the resource that we want.
				{

					final DataOutputStream dos = new DataOutputStream(s.getOutputStream());

					dos.writeByte(ResourceTypeEnum.BUFFER.get());
                    dos.writeLong(id.getMostSignificantBits());
                    dos.writeLong(id.getLeastSignificantBits());

					// flush the request.
					dos.flush();

				}

				// buffered input stream for reading from the socket.
				final InputStream is = new BufferedInputStream(s.getInputStream());

				transfer(is, os);

				// flip the view to prepare it for reading.
				b.flip();
				
				// return the writable view.
				return b;
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
		}

	} // class ReadBufferTask
    
}
