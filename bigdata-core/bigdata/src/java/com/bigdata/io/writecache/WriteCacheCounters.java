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

import com.bigdata.counters.CAT;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;

/**
 * Performance counters for the {@link WriteCache}.
 * <p>
 * Note: thread-safety is required for: {@link #nhit} and {@link #nmiss}. The
 * rest should be Ok without additional synchronization, CAS operators, etc
 * (mainly because they are updated while holding a lock).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class WriteCacheCounters implements IWriteCacheCounters {

    /*
     * read on the cache.
     */

    /**
     * #of read requests that are satisfied by the write cache.
     */
    public final CAT nhit = new CAT();

    /**
     * The #of read requests that are not satisfied by the write cache.
     */
    public final CAT nmiss = new CAT();

    /*
     * write on the cache.
     */

    /**
     * #of records accepted for eventual write onto the backing channel.
     */
    public long naccept;

    /**
     * #of bytes accepted for eventual write onto the backing channel.
     */
    public long bytesAccepted;

    /*
     * write on the channel.
     */

    // /**
    // * #of times {@link IWriteCache#flush(boolean)} was called.
    // */
    // public long nflush;

    /**
     * #of writes on the backing channel. Note that some write cache
     * implementations do ordered writes and will therefore do one write per
     * record while others do append only and therefore do one write per write
     * cache flush. Note that in both cases we may have to redo a write if the
     * backing channel was concurrently closed, so the value here can diverge
     * from the #of accepted records and the #of requested flushes.
     */
    public long nchannelWrite;

    /**
     * #of bytes written onto the backing channel.
     */
    public long bytesWritten;

    /**
     * Total elapsed time writing onto the backing channel.
     */
    public long elapsedWriteNanos;

    public CounterSet getCounters() {

        final CounterSet root = new CounterSet();

        /*
         * read on the cache.
         */

        root.addCounter(NHIT, new Instrument<Long>() {
            public void sample() {
                setValue(nhit.get());
            }
        });

        root.addCounter(NMISS, new Instrument<Long>() {
            public void sample() {
                setValue(nmiss.get());
            }
        });

        root.addCounter(HIT_RATE, new Instrument<Double>() {
            public void sample() {
                final long nhit = WriteCacheCounters.this.nhit.get();
                final long ntests = nhit + nmiss.get();
                setValue(ntests == 0L ? 0d : (double) nhit / ntests);
            }
        });

        /*
         * write on the cache.
         */

        // #of records accepted by the write cache.
        root.addCounter(NACCEPT, new Instrument<Long>() {
            public void sample() {
                setValue(naccept);
            }
        });

        // #of bytes in records accepted by the write cache.
        root.addCounter(BYTES_ACCEPTED, new Instrument<Long>() {
            public void sample() {
                setValue(bytesAccepted);
            }
        });

        /*
         * write on the channel.
         */

        // // #of times this write cache was flushed to the backing channel.
        // root.addCounter(NFLUSHED, new Instrument<Long>() {
        // public void sample() {
        // setValue(nflush);
        // }
        // });

        // #of writes onto the backing channel.
        root.addCounter(NCHANNEL_WRITE, new Instrument<Long>() {
            public void sample() {
                setValue(nchannelWrite);
            }
        });

        // #of bytes written onto the backing channel.
        root.addCounter(BYTES_WRITTEN, new Instrument<Long>() {
            public void sample() {
                setValue(bytesWritten);
            }
        });

        // average bytes per write (will under report if we must retry
        // writes).
        root.addCounter(BYTES_PER_WRITE, new Instrument<Double>() {
            public void sample() {
                final double bytesPerWrite = (nchannelWrite == 0 ? 0d
                        : (bytesWritten / (double) nchannelWrite));
                setValue(bytesPerWrite);
            }
        });

        // elapsed time writing on the backing channel.
        root.addCounter(WRITE_SECS, new Instrument<Double>() {
            public void sample() {
                setValue(elapsedWriteNanos / 1000000000.);
            }
        });
        
        return root;

    } // getCounters()

    public String toString() {

        return getCounters().toString();

    }

} // class WriteCacheCounters
