/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
package com.bigdata.btree;

import java.text.NumberFormat;

/**
 * A helper class that collects statistics on an {@link AbstractBTree}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo add nano timers and track storage used by the index. The goal is to
 *       know how much of the time of the server is consumed by the index, what
 *       percentage of the store is dedicated to the index, how expensive it is
 *       to do some scan-based operations (merge down, delete of transactional
 *       isolated persistent index), and evaluate the buffer strategy by
 *       comparing accesses with IOs.
 * 
 * @todo expose more of the counters using property access methods.
 */
public class Counters {

    private final AbstractBTree btree;
    
    public Counters(AbstractBTree btree) {
        
        assert btree != null;
        
        this.btree = btree;
        
    }
    
    int nfinds = 0; // #of keys looked up in the tree by lookup(key).
    int nbloomRejects = 0; // #of keys rejected by the bloom filter in lookup(key).
    int ninserts = 0;
    int nremoves = 0;
    int nindexOf = 0;
    int ngetKey = 0;
    int ngetValue = 0;
    int rootsSplit = 0;
    int rootsJoined = 0;
    int nodesSplit = 0;
    int nodesJoined = 0;
    int leavesSplit = 0;
    int leavesJoined = 0; // @todo also merge vs redistribution of keys on remove (and insert if b*-tree)
    int nodesCopyOnWrite = 0;
    int leavesCopyOnWrite = 0;
    int nodesRead = 0;
    int leavesRead = 0;
    int nodesWritten = 0;
    int leavesWritten = 0;
    long bytesRead = 0L;
    long bytesWritten = 0L;
    /*
     * Note: The nano time fields are for nodes+leaves.
     */
    long serializeTimeNanos = 0;
    long deserializeTimeNanos = 0;
    long writeTimeNanos = 0;
    long readTimeNanos = 0;
    
    /**
     * The #of nodes written on the backing store.
     */
    final public int getNodesWritten() {
        
        return nodesWritten;
        
    }
    
    /**
     * The #of leaves written on the backing store.
     */
    final public int getLeavesWritten() {
        
        return leavesWritten;
        
    }
    
    /**
     * The number of bytes read from the backing store.
     */
    final public long getBytesRead() {
        
        return bytesRead;
        
    }
    
    /**
     * The number of bytes written onto the backing store.
     */
    final public long getBytesWritten() {
        
        return bytesWritten;
        
    }

    public String toString() {

        /*
         * @todo find/insert/remove times?
         * 
         * @todo range iterator times? (must aggregate across hasNext() and next())
         * 
         * @todo key search times?
         * 
         * @todo data copy times?
         */
        
        /*
         * store read/write times.
         */
        final double readTimeSecs = (readTimeNanos / 1000000000.);

        final String bytesReadPerSec = (readTimeSecs == 0L ? "N/A" : ""
                + commaFormat.format(bytesRead / readTimeSecs));

        final double writeTimeSecs = (writeTimeNanos / 1000000000.);

        final String bytesWrittenPerSec = (writeTimeSecs == 0. ? "N/A"
                : ""+ commaFormat.format(bytesWritten
                                / writeTimeSecs));
        
        /*
         * node/leaf (de-)serialization times.
         */
        final double serializeTimeSecs = (serializeTimeNanos / 1000000000.);

        final String serializePerSec = (serializeTimeSecs == 0L ? "N/A" : ""
                + commaFormat.format((nodesWritten+leavesWritten)/ serializeTimeSecs));

        final double deserializeTimeSecs = (deserializeTimeNanos / 1000000000.);

        final String deserializePerSec = (deserializeTimeSecs == 0. ? "N/A"
                : ""+ commaFormat.format((nodesRead+leavesRead)
                                / deserializeTimeSecs));

        return 
        "\n#find="+nfinds+
        ", #bloomRejects="+nbloomRejects+
        ", #insert="+ninserts+
        ", #remove="+nremoves+
        ", #indexOf="+nindexOf+
        ", #getKey="+ngetKey+
        ", #getValue="+ngetValue+
        "\n#roots split="+rootsSplit+
        ", #roots joined="+rootsJoined+
        ", #nodes split="+nodesSplit+
        ", #nodes joined="+nodesJoined+
        ", #leaves split="+leavesSplit+
        ", #leaves joined="+leavesJoined+
        ", #nodes copyOnWrite="+nodesCopyOnWrite+
        ", #leaves copyOnWrite="+leavesCopyOnWrite+
        "\nread ("
                + nodesRead + " nodes, " + leavesRead + " leaves, "
                + commaFormat.format(bytesRead) + " bytes" + ", readSeconds="
                + secondsFormat.format(readTimeSecs) + ", bytes/sec="
                + bytesReadPerSec + ", deserializeSeconds="
                + secondsFormat.format(deserializeTimeSecs)
                + ", deserialized/sec=" + deserializePerSec + ")" +
        "\nwrote ("
                + nodesWritten + " nodes, " + leavesWritten + " leaves, "
                + commaFormat.format(bytesWritten) + " bytes"
                + ", writeSeconds=" + secondsFormat.format(writeTimeSecs)
                + ", bytes/sec=" + bytesWrittenPerSec + ", serializeSeconds="
                + secondsFormat.format(serializeTimeSecs) + ", serialized/sec="
                + serializePerSec + ")" +
        "\n----"
        ;
        
    }

    static private final NumberFormat commaFormat = NumberFormat.getInstance();
    static private final NumberFormat percentFormat = NumberFormat.getPercentInstance();
    static private final NumberFormat secondsFormat = NumberFormat.getInstance();
    
    static
    {

        commaFormat.setGroupingUsed(true);
        commaFormat.setMaximumFractionDigits(0);
        
        secondsFormat.setMinimumFractionDigits(3);
        secondsFormat.setMaximumFractionDigits(3);
        
    }

}
