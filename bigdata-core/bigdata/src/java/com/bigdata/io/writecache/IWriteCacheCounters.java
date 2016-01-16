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

/**
 * Interface declaring the counters exposed by the {@link WriteCache}.
 */
public interface IWriteCacheCounters {

    /**
     * The #of read requests that were satisfied by the cache.
     */
    String NHIT = "nhit";
    /**
     * The #of read requests that were not satisfied by the cache.
     */
    String NMISS = "nmiss";
    /**
     * The effective hit rate for the cache.
     */
    String HIT_RATE = "hitRate";
    /**
     * The #of records that were accepted by the cache.
     */
    String NACCEPT = "naccept";
    /**
     * The #of bytes in the records that were accepted by the cache.
     */
    String BYTES_ACCEPTED = "bytesAccepted";
    // /**
    // * The #of times this write cache was flushed to the backing channel.
    // */
    // String NFLUSHED = "nflushed";
    /**
     * The #of writes onto the backing channel - this is either
     * {@link WriteCache} buffer instances or individual records in those
     * {@link WriteCache} buffers depending on whether the {@link WriteCache}
     * supports gathered writes (for the WORM, it is the #of {@link WriteCache}
     * instances written, for the RW, it is the #of records written).
     */
    String NCHANNEL_WRITE = "nchannelWrite";
    /**
     * The #of bytes written onto the backing channel.
     */
    String BYTES_WRITTEN = "bytesWritten";
    /**
     * The average bytes per write (will under-report if we must retry writes).
     */
    String BYTES_PER_WRITE = "bytesPerWrite";
    /**
     * The elapsed time (in seconds) writing on the backing channel.
     */
    String WRITE_SECS = "writeSecs";

} // interface IWriteCacheCounters
