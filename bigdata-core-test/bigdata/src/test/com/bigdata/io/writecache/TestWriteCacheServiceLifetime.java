package com.bigdata.io.writecache;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.bigdata.ha.HAPipelineGlue;
import com.bigdata.io.ChecksumUtility;
import com.bigdata.io.FileChannelUtility;
import com.bigdata.io.IBufferAccess;
import com.bigdata.io.IReopenChannel;
import com.bigdata.io.TestCase3;
import com.bigdata.io.writecache.TestWORMWriteCacheService.MyMockQuorumMember;
import com.bigdata.io.writecache.WriteCache.FileChannelScatteredWriteCache;
import com.bigdata.io.writecache.WriteCache.FileChannelWriteCache;
import com.bigdata.quorum.MockQuorumFixture;
import com.bigdata.quorum.MockQuorumFixture.MockQuorum;
import com.bigdata.quorum.QuorumActor;
import com.bigdata.util.DaemonThreadFactory;

/**
 * These tests exercise the lifetime semantics of a WriteCacheService.
 * 
 * Specifically they stress the initialization and shutdown of services, and
 * concurrent interactions.
 * 
 * @author Martyn Cutcher
 * 
 */
public class TestWriteCacheServiceLifetime extends TestCase3 {

	public TestWriteCacheServiceLifetime() {
	}

	public TestWriteCacheServiceLifetime(String name) {
		super(name);
	}

	public void testSimpleStartStopRWService() throws InterruptedException, IOException {
		ServiceConfig config = getService(true);
		try {
			// do test
			ByteBuffer data = getRandomData(2048);
			final long addr1 = 2048;
			config.service.write(addr1, data.asReadOnlyBuffer(),
            ChecksumUtility.threadChk.get().checksum(data));
		} finally {
			config.close();
		}
	}

	/**
	 * The first genuine lifetime test, sets 20 concurrent writer tasks
	 * to output to the service and a final task to close the service
	 * concurrently.
	 */
	public void testSimpleStartStopRWService2() throws InterruptedException, IOException {
		final ServiceConfig config = getService(true);
		try {
			// do test
			final int dataSize = 8096;
			final ByteBuffer data = getRandomData(dataSize);
			final AtomicLong addr = new AtomicLong(dataSize);
			final int chk = ChecksumUtility.threadChk.get().checksum(data);
			
			final int nclients = 60;
			final int nwrites = 20000;
			
	        final ExecutorService executorService = Executors.newFixedThreadPool(
	        		nclients, DaemonThreadFactory.defaultThreadFactory());

	        final Collection<Callable<Long>> tasks = new HashSet<Callable<Long>>();
	        for (int i = 0; i < nclients; i++) {
	        	tasks.add(new Callable<Long>() {
					public Long call() throws Exception {
						try {
							for (int i = 0; i < nwrites; i++) {
								config.service.write(addr.addAndGet(dataSize), data.asReadOnlyBuffer(), chk);
								config.service.write(addr.addAndGet(dataSize), data.asReadOnlyBuffer(), chk);
								Thread.currentThread().sleep(20); // give WriteTask chance to catch up
							}
						} catch (Throwable t) {
							t.printStackTrace();
						}

						return null;
					}
	        	});
			}
	        tasks.add(new Callable<Long>() {
				public Long call() throws Exception {
					Thread.currentThread().sleep(5000);
					config.service.close();

					return null;
				}
        	});
	        
	        executorService.invokeAll(tasks, 20, TimeUnit.SECONDS);

		} finally {
			config.close();
		}
	}

	static class ServiceConfig {
		WriteCacheService service;
		ReopenFileChannel opener;
		MockQuorumFixture fixture;
		MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>> quorum;

		void close() {
			if (service != null)
				service.close();
			if (opener != null) {
				opener.destroy();
			}
			quorum.terminate();
			fixture.terminate();

		}
	}

	ServiceConfig getService(final boolean rw) throws InterruptedException, IOException {
		// No write pipeline.
		ServiceConfig config = new ServiceConfig();
		final int k = 1;
		final long lastCommitTime = 0L;
		config.fixture = new MockQuorumFixture();
		final String logicalServiceId = "logicalService_" + getName();
		config.quorum = new MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>>(k, config.fixture);
		File file = null;

		config.fixture.start();
		config.quorum.start(new MyMockQuorumMember<HAPipelineGlue>(config.fixture, logicalServiceId));

		final QuorumActor<?, ?> actor = config.quorum.getActor();

		actor.memberAdd();
		config.fixture.awaitDeque();

		actor.pipelineAdd();
		config.fixture.awaitDeque();

		actor.castVote(lastCommitTime);
		config.fixture.awaitDeque();

		file = File.createTempFile(getName(), ".rw.tmp");

		config.opener = new ReopenFileChannel(file, "rw");

		final long fileExtent = config.opener.reopenChannel().size();

        final boolean prefixWrites = rw;
        final int compactionThreshold = 30;
        final int hotCacheThreshold = 1;
        config.service = new WriteCacheService(5/* nbuffers */,
                2/* minCleanListSize */, 0/* readCacheSize*/, prefixWrites, compactionThreshold,
                0/*hotCacheSize*/, hotCacheThreshold,
                true/* useChecksums */, fileExtent, config.opener,
                config.quorum, null) {

            /**
             * The scattered write cache supports compaction.
             */
            @Override
            protected final boolean canCompact() {
                return rw;
            }

            @Override
            public WriteCache newWriteCache(final IBufferAccess buf,
                    final boolean useChecksum, final boolean bufferHasData,
                    final IReopenChannel<? extends Channel> opener,
                    final long fileExtent)
                    throws InterruptedException {

                if (!rw) {
                	return new FileChannelWriteCache(0/* baseOffset */,
                            buf, useChecksum, false,
                            bufferHasData,
                            (IReopenChannel<FileChannel>) opener,
                            fileExtent);
                } else {
                    return new FileChannelScatteredWriteCache(buf, useChecksum,
                            false, bufferHasData,
                            (IReopenChannel<FileChannel>) opener, fileExtent,
                            null);
                }

            }
		};

		return config;
	}

	/**
	 * Simple implementation for a {@link RandomAccessFile} with hook for
	 * deleting the test file.
	 */
	private static class ReopenFileChannel implements IReopenChannel<FileChannel> {

		final private File file;

		private final String mode;

		private volatile RandomAccessFile raf;

		public ReopenFileChannel(final File file, final String mode) throws IOException {

			this.file = file;

			this.mode = mode;

			reopenChannel();

		}

		public String toString() {

			return file.toString();

		}

		/**
		 * Hook used by the unit tests to destroy their test files.
		 */
		public void destroy() {
			try {
				raf.close();
			} catch (IOException e) {
				log.error(e, e);
			}
			if (!file.delete())
				log.warn("Could not delete file: " + file);
		}

		/**
		 * Read some data out of the file.
		 * 
		 * @param off
		 *            The offset of the record.
		 * @param nbytes
		 *            The #of bytes to be read.
		 * @return The record.
		 */
		public ByteBuffer read(final long off, final int nbytes) throws IOException {

			final ByteBuffer tmp = ByteBuffer.allocate(nbytes);

			FileChannelUtility.readAll(this, tmp, off);

			// flip for reading.
			tmp.flip();

			return tmp;

		}

		synchronized public FileChannel reopenChannel() throws IOException {

			if (raf != null && raf.getChannel().isOpen()) {

				/*
				 * The channel is still open. If you are allowing concurrent
				 * reads on the channel, then this could indicate that two
				 * readers each found the channel closed and that one was able
				 * to re-open the channel before the other such that the channel
				 * was open again by the time the 2nd reader got here.
				 */

				return raf.getChannel();

			}

			// open the file.
			this.raf = new RandomAccessFile(file, mode);

			if (log.isInfoEnabled())
				log.info("(Re-)opened file: " + file);

			return raf.getChannel();

		}

	};
	
    protected final Random r = new Random();

    /**
     * Returns random data that will fit in N bytes. N is chosen randomly in
     * 1:256.
     * 
     * @return A new {@link ByteBuffer} wrapping a new <code>byte[]</code> of
     *         random length and having random contents.
     */
    public ByteBuffer getRandomData() {

        final int nbytes = r.nextInt(256) + 1;

        return getRandomData(nbytes);

    }

    /**
     * Returns random data that will fit in <i>nbytes</i>.
     * 
     * @return A new {@link ByteBuffer} wrapping a new <code>byte[]</code>
     *         having random contents.
     */
    public ByteBuffer getRandomData(final int nbytes) {

        final byte[] bytes = new byte[nbytes];

        r.nextBytes(bytes);

        return ByteBuffer.wrap(bytes);

    }

}
