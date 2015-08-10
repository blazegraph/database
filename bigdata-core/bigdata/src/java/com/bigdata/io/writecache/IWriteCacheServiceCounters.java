/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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

import com.bigdata.io.writecache.WriteCache.ReadCache;
import com.bigdata.io.writecache.WriteCacheService.WriteTask;
import com.bigdata.util.concurrent.Memoizer;

/**
 * Interface declaring the counters exposed by the {@link WriteCacheService} .
 */
public interface IWriteCacheServiceCounters {

    /**
     * The configured number of {@link WriteCache} buffers.
     */
    String NBUFFERS = "nbuffers";

    /**
     * The configured dirty list threshold before evicting to disk (immutable).
     */
    String DIRTY_LIST_THRESHOLD = "dirtyListThreshold";

    /**
     * The threshold of reclaimable space at which we will attempt to coalesce
     * records in cache buffers.
     */
    String COMPACTING_THRESHOLD = "compactingThreshold";

    /**
     * #of dirty buffers (instantaneous).
     * <p>
     * Note: This is set by the {@link WriteTask} thread and by
     * {@link WriteCacheService#reset()}. It is volatile so it is visible from a
     * thread which looks at the counters and for correct publication from
     * reset().
     */
    String NDIRTY = "ndirty";

    /**
     * The maximum #of dirty buffers observed by the {@link WriteTask} (its
     * maximum observed backlog). This is only set by the {@link WriteTask}
     * thread, but it is volatile so it is visible from a thread which looks at
     * the counters.
     */
    String MAX_DIRTY = "maxDirty";

    /**
     * The #of {@link WriteCache} buffers on the clean list at the moment in
     * time when the measurement was taken.
     */
    String NCLEAN = "nclean";

    /**
     * #of times the {@link WriteCacheService} was reset (typically to handle an
     * error condition).
     * <p>
     * Note: This is set by {@link WriteCacheService#reset()}. It is volatile so
     * it is visible from a thread which looks at the counters and for correct
     * publication from reset().
     */
    String NRESET = "nreset";
    /**
     * The #of {@link WriteCache} blocks sent by the leader to the first
     * downstream follower.
     */
    String NSEND = "nsend";

    /**
     * The #of {@link WriteCache} buffers evicted to the backing channel.
     * <p>
     * Note: This always reports buffers written to the channel, not records
     * written on the channel, even if there are many records in the buffer and
     * we are using gathered writes on the channel.
     */
    String NBUFFER_EVICTED_TO_CHANNEL = "nbufferEvictToChannel";

    /**
     * The #of {@link WriteCache} buffers that have been compacted.
     */
    String NCOMPACT = "ncompact";

    /**
     * The #of record-level writes made onto the {@link WriteCacheService}.
     */
    String NCACHE_WRITES = "ncacheWrites";

    /**
     * The requests to clear an address from the cache.
     * 
     * @see WriteCacheService#clearWrite(long, int)
     */
    String NCLEAR_ADDR_REQUESTS = "nclearAddrRequests";

    /**
     * The #of addresses actually found and cleared from the cache by the
     * {@link WriteCacheService}.
     * 
     * @see WriteCacheService#clearWrite(long, int)
     */
    String NCLEAR_ADDR_CLEARED = "nclear";

    /**
     * The #of megabytes per second written onto the backing channel.
     */
    String MB_PER_SEC = "mbPerSec";

    /*
     * ReadCache counters.
     */
    
    /**
     * The #of read requests that were a miss in the cache and resulted in a
     * read through to the disk where the record was NOT installed into the read
     * cache (either because there is no read cache, because the record is too
     * large for the read cache, or because the thread could not obtain a
     * {@link ReadCache} block to install the read).
     */
    String NREAD_NOT_INSTALLED = "nreadNotInstalled";

    /**
     * The current size of the {@link Memoizer}'s internal cache that is used to
     * serialize reads against a given byte offset on the backing file.
     */
    String MEMO_CACHE_SIZE = "memoCacheSize";

} // interface IWriteCacheCounters
