/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
package com.bigdata.io.writecache;

import java.util.concurrent.TimeUnit;

import com.bigdata.counters.CAT;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.counters.OneShotInstrument;
import com.bigdata.io.writecache.WriteCache.ReadCache;
import com.bigdata.io.writecache.WriteCacheService.WriteTask;
import com.bigdata.util.Bytes;

/**
 * Performance counters for the {@link WriteCacheService}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class WriteCacheServiceCounters extends WriteCacheCounters implements
        IWriteCacheServiceCounters {

    /** #of configured buffers (immutable). */
    public final int nbuffers;

    /**
     * The configured dirty list threshold before evicting to disk (immutable).
     */
    public final int dirtyListThreshold;

    /**
     * The threshold of reclaimable space at which we will attempt to coalesce
     * records in cache buffers.
     */
    public final int compactingThreshold;

    /**
     * #of dirty buffers (instantaneous).
     * <p>
     * Note: This is set by the {@link WriteTask} thread and by
     * {@link WriteCacheService#reset()}. It is volatile so it is visible from a
     * thread which looks at the counters and for correct publication from
     * reset().
     */
    public volatile int ndirty;

    /**
     * #of clean buffers (instantaneous).
     * <p>
     * Note: This is set by the {@link WriteTask} thread and by
     * {@link WriteCacheService#reset()}. It is volatile so it is visible from a
     * thread which looks at the counters and for correct publication from
     * reset().
     */
    public volatile int nclean;

    /**
     * The maximum #of dirty buffers observed by the {@link WriteTask} (its
     * maximum observed backlog). This is only set by the {@link WriteTask}
     * thread, but it is volatile so it is visible from a thread which looks at
     * the counters.
     */
    public volatile int maxdirty;

    /**
     * #of times the {@link WriteCacheService} was reset (typically to handle an
     * error condition).
     * <p>
     * Note: This is set by {@link WriteCacheService#reset()}. It is volatile so
     * it is visible from a thread which looks at the counters and for correct
     * publication from reset().
     */
    public volatile long nreset;

    /**
     * The #of {@link WriteCache} blocks sent by the leader to the first
     * downstream follower.
     */
    public volatile long nsend;

    /**
     * The #of {@link WriteCache} buffers written to the disk.
     */
    public volatile long nbufferEvictedToChannel;

    /**
     * The cumulative latency (nanoseconds) when writing a write cache buffer
     * onto the backing channel.
     * 
     * See BLZG-1589 (new latency-oriented counters)
     */
    public volatile long elapsedBufferEvictedToChannelNanos;

    /**
     * The cumulative number of records written onto the backing channel. This
     * may be used to track the number of induced write operators per second.
     * However, note that the RWStore will pad out writes to their slot size in
     * order to offer the underlying file system and disk controller an
     * opportunity to meld together multiple writes into a single IO. This is
     * particularly effective in combination with the small slots optimization.
     * 
     * See BLZG-1589 (new latency-oriented counters)
     */
    public volatile long nrecordsEvictedToChannel;
    
    /**
     * The #of {@link WriteCache} buffers that have been compacted.
     */
    public volatile long ncompact;

    /**
     * The #of record-level writes made onto the {@link WriteCacheService}.
     */
    public volatile long ncacheWrites;

    /**
     * The cumulative latency (nanoseconds) when writing into the
     * write cache.
     * 
     * See BLZG-1589 (new latency-oriented counters)
     */
    public volatile long elapsedCacheWriteNanos;

    /**
     * The requests to clear an address from the cache.
     * 
     * @see WriteCacheService#clearWrite(long, int)
     */
    public volatile long nclearAddrRequests;

    /**
     * The #of addresses actually found and cleared from the cache by the
     * {@link WriteCacheService}.
     * 
     * @see WriteCacheService#clearWrite(long, int)
     */
    public volatile long nclearAddrCleared;

    /**
     * The #of read requests that were a miss in the cache and resulted in a
     * read through to the disk where the record was NOT installed into the read
     * cache (either because there is no read cache, because the record is too
     * large for the read cache, or because the thread could not obtain a
     * {@link ReadCache} block to install the read).
     */
    public final CAT nreadNotInstalled = new CAT();
    
    public final CAT memoCacheSize = new CAT();
    
    public WriteCacheServiceCounters(final int nbuffers,
            final int dirtyListThreshold, final int compactingThreshold) {

        this.nbuffers = nbuffers;

        this.dirtyListThreshold = dirtyListThreshold;

        this.compactingThreshold = compactingThreshold;

    }

    @Override
    public CounterSet getCounters() {

        final CounterSet root = super.getCounters();

        root.addCounter(NBUFFERS, new OneShotInstrument<Integer>(nbuffers));

        root.addCounter(DIRTY_LIST_THRESHOLD, new OneShotInstrument<Integer>(
                dirtyListThreshold));

        root.addCounter(COMPACTING_THRESHOLD, new OneShotInstrument<Integer>(
                compactingThreshold));

        root.addCounter(NDIRTY, new Instrument<Integer>() {
            @Override
            public void sample() {
                setValue(ndirty);
            }
        });

        root.addCounter(MAX_DIRTY, new Instrument<Integer>() {
            @Override
            public void sample() {
                setValue(maxdirty);
            }
        });

        root.addCounter(NCLEAN, new Instrument<Integer>() {
            @Override
            public void sample() {
                setValue(nclean);
            }
        });

        root.addCounter(NRESET, new Instrument<Long>() {
            @Override
            public void sample() {
                setValue(nreset);
            }
        });

        root.addCounter(NSEND, new Instrument<Long>() {
            @Override
            public void sample() {
                setValue(nsend);
            }
        });

        root.addCounter(NBUFFER_EVICTED_TO_CHANNEL, new Instrument<Long>() {
            @Override
            public void sample() {
                setValue(nbufferEvictedToChannel);
            }
        });

        root.addCounter(ELAPSED_BUFFER_EVICTED_TO_CHANNEL_NANOS, new Instrument<Long>() {
            @Override
            public void sample() {
                setValue(elapsedBufferEvictedToChannelNanos);
            }
        });

        root.addCounter(AVERAGE_BUFFER_EVICTED_TO_CHANNEL_NANOS, new Instrument<Double>() {
            @Override
            public void sample() {
                if (nbufferEvictedToChannel > 0) {
                    final double d = elapsedBufferEvictedToChannelNanos / nbufferEvictedToChannel;
                    setValue(((long) (d * 100)) / 100d);
                }
            }
        });

        root.addCounter(NRECORDS_EVICTED_TO_CHANNEL, new Instrument<Long>() {
            @Override
            public void sample() {
                setValue(nrecordsEvictedToChannel);
            }
        });

        root.addCounter(AVERAGE_RECORD_EVICTED_TO_CHANNEL_NANOS, new Instrument<Double>() {
            @Override
            public void sample() {
                if (nrecordsEvictedToChannel > 0) {
                    /*
                     * Note: records are evicted a buffer at a time. Therefore
                     * we use the same divisor here as we do for the
                     * AVERAGE_BUFFER_EVICTED_TO_CHANNEL_NANOS.
                     */
                    final double d = elapsedBufferEvictedToChannelNanos / nrecordsEvictedToChannel;
                    setValue(((long) (d * 100)) / 100d);
                }
            }
        });

        root.addCounter(AVERAGE_RANDOM_WRITES_PER_SECOND, new Instrument<Double>() {
            @Override
            public void sample() {
                if (nrecordsEvictedToChannel > 0) {
                    /*
                     * Note: records are evicted a buffer at a time. Therefore
                     * we use the same divisor here as we do for the
                     * AVERAGE_BUFFER_EVICTED_TO_CHANNEL_NANOS.
                     */
                    // records written / second.
                    final double d = TimeUnit.NANOSECONDS.toSeconds(elapsedBufferEvictedToChannelNanos)
                            / ((double) nrecordsEvictedToChannel);
                    // two decimal places of precision.
                    final double v = ((long) (d * 100)) / 100d;
                    setValue(v);
                }
            }
        });

        root.addCounter(NCOMPACT, new Instrument<Long>() {
            @Override
            public void sample() {
                setValue(ncompact);
            }
        });

        root.addCounter(NCACHE_WRITES, new Instrument<Long>() {
            @Override
            public void sample() {
                setValue(ncacheWrites);
            }
        });

        root.addCounter(ELAPSED_CACHE_WRITES_NANOS, new Instrument<Long>() {
            @Override
            public void sample() {
                setValue(elapsedCacheWriteNanos);
            }
        });

        root.addCounter(AVERAGE_CACHE_WRITE_NANOS, new Instrument<Double>() {
            @Override
            public void sample() {
                if (ncacheWrites > 0) {
                    final double d = elapsedCacheWriteNanos / ncacheWrites;
                    setValue(((long) (d * 100)) / 100d);
                }
            }
        });

        root.addCounter(NCLEAR_ADDR_REQUESTS, new Instrument<Long>() {
            @Override
            public void sample() {
                setValue(nclearAddrRequests);
            }
        });

        root.addCounter(NCLEAR_ADDR_CLEARED, new Instrument<Long>() {
            @Override
            public void sample() {
                setValue(nclearAddrCleared);
            }
        });

        root.addCounter(MB_PER_SEC, new Instrument<Double>() {
            @Override
            public void sample() {
                final double mbPerSec = (((double) bytesWritten)
                        / Bytes.megabyte32 / (TimeUnit.NANOSECONDS
                        .toSeconds(elapsedWriteNanos)));
                setValue(((long) (mbPerSec * 100)) / 100d);

            }
        });

        /*
         * Read Cache.
         */
        
        root.addCounter(NREAD_NOT_INSTALLED, new Instrument<Long>() {
            @Override
            public void sample() {
                setValue(nreadNotInstalled.get());
            }
        });

        root.addCounter(MEMO_CACHE_SIZE, new Instrument<Long>() {
            @Override
            public void sample() {
                setValue(memoCacheSize.get());
            }
        });

        return root;

    }

} // class WriteCacheServiceCounters
