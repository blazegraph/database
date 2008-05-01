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

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.ICounterSet;
import com.bigdata.counters.Instrument;

/**
 * A helper class that collects statistics on an {@link AbstractBTree}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Counters {

    public Counters() {
        
    }

    /**
     * Copy constructor.
     * 
     * @param c
     */
    public Counters(Counters c) {
        
        add(c);
        
    }
    
    /**
     * Adds the values from another {@link Counters} object to this one.
     * 
     * @param o
     */
    public void add(Counters o) {
        
        // IKeySearch
        nfinds += o.nfinds;
        nbloomRejects += o.nbloomRejects;
        ninserts += o.ninserts;
        nremoves += o.nremoves;
        // ILinearList
        nindexOf += o.nindexOf; // Note: also does key search.
        ngetKey  += o.ngetKey;
        ngetValue += o.ngetValue;
        // IRangeQuery
        nrangeCount += o.nrangeCount;
        nrangeIterator += o.nrangeIterator;
        // Structural mutation.
        rootsSplit += o.rootsSplit;
        rootsJoined += o.rootsJoined;
        nodesSplit += o.nodesSplit;
        nodesJoined += o.nodesJoined;
        leavesSplit += o.leavesSplit;
        leavesJoined += o.leavesJoined;
        nodesCopyOnWrite += o.nodesCopyOnWrite;
        leavesCopyOnWrite += o.leavesCopyOnWrite;
        // IO
        nodesRead += o.nodesRead;
        leavesRead += o.leavesRead;
        nodesWritten += o.nodesWritten;
        leavesWritten += o.leavesWritten;
        bytesRead += o.bytesRead;
        bytesWritten += o.bytesWritten;
        
        serializeTimeNanos += o.serializeTimeNanos;
        deserializeTimeNanos += o.deserializeTimeNanos;
        writeTimeNanos += o.writeTimeNanos;
        readTimeNanos += o.readTimeNanos;
        
    }
    
    // IKeySearch
    int nfinds = 0; // #of keys looked up in the tree by lookup(key).
    int nbloomRejects = 0; // #of keys rejected by the bloom filter in lookup(key).
    int ninserts = 0;
    int nremoves = 0;
    // ILinearList
    int nindexOf = 0;
    int ngetKey = 0;
    int ngetValue = 0;
    // IRangeQuery
    int nrangeCount = 0;
    int nrangeIterator = 0;
    // Structural change.
    int rootsSplit = 0;
    int rootsJoined = 0;
    int nodesSplit = 0;
    int nodesJoined = 0;
    int leavesSplit = 0;
    int leavesJoined = 0;
    int nodesCopyOnWrite = 0;
    int leavesCopyOnWrite = 0;
    // IO
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
     * Return a score whose increasing value is correlated with the amount of
     * activity on an index as reflected in these {@link Counters}.
     * <p>
     * The raw score is the (de)-serialization time plus the read/write time.
     * Time was choosen since it is a common unit and since it reflects the
     * combination of CPU time, memory time (for allocations and garbage
     * collection - the latter can be quite significant), and the disk wait
     * time. The other main component of time is key search, but that is not
     * instrumented right now.
     * <p>
     * (de-)serialization is basically a CPU activity and drives memory to the
     * extent that allocations are made. At present, de-serialization is much
     * slower than serialization, primarily because a lot of object creation
     * occurs during de-serialization. Changing to a raw record format for nodes
     * and leaves would likely reduce the de-serialization costs significantly.
     * <p>
     * The read/write time is strongly dominated by actual DISK IO and by
     * garbage collection time (garbage collection can cause threads to be
     * suspended at any time). For deep B+Trees, DISK READ time dominates DISK
     * WRITE time since increasing numbers of random reads are required to
     * materialize any given leaf.
     * 
     * @return The computed score.
     */
    public double computeRawScore() {
        
        return //
            (serializeTimeNanos + deserializeTimeNanos) + //
            (writeTimeNanos + readTimeNanos)//
            ;
        
    }
    
    /**
     * Normalizes a raw score in the context of totals for some data service.
     * 
     * @param rawScore
     *            The raw score.
     * @param totalRawScore
     *            The raw score computed from the totals.
     *            
     * @return The normalized score.
     */
    static public double normalize(double rawScore, double totalRawScore ) {
        
        if(totalRawScore == 0d) {
            
            return 0d;
            
        }
        
        return rawScore / totalRawScore;
        
    }
    
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

    /**
     * Return a {@link CounterSet} reporting on the various counters tracked in
     * the instance fields of this class.
     */
    synchronized ICounterSet getCounters() {
        
        if(counterSet == null) {
            
            counterSet = new CounterSet();

            /*
             * ISimpleBTree
             *
             * @todo instrument key search time.
             */
            {
                final CounterSet tmp = counterSet.makePath("keySearch");

                tmp.addCounter("#find", new Instrument<Integer>() {
                    protected void sample() {
                        setValue(nfinds);
                    }
                });

                tmp.addCounter("#bloomReject", new Instrument<Integer>() {
                    protected void sample() {
                        setValue(nbloomRejects);
                    }
                });

                tmp.addCounter("#insert", new Instrument<Integer>() {
                    protected void sample() {
                        setValue(ninserts);
                    }
                });

                tmp.addCounter("#nremove", new Instrument<Integer>() {
                    protected void sample() {
                        setValue(nremoves);
                    }
                });
            }
            
            /*
             * ILinearList API.
             */
            {
                
                final CounterSet tmp = counterSet.makePath("linearList");

                tmp.addCounter("#indexOf", new Instrument<Integer>() {
                    protected void sample() {
                        setValue(nindexOf);
                    }
                });

                tmp.addCounter("#getKey", new Instrument<Integer>() {
                    protected void sample() {
                        setValue(ngetKey);
                    }
                });

                tmp.addCounter("#getValue", new Instrument<Integer>() {
                    protected void sample() {
                        setValue(ngetValue);
                    }
                });
                
            }
            
            /*
             * IRangeQuery
             * 
             * FIXME instrument the IRangeQuery API for times (must aggregate
             * across hasNext() and next()). Note that rangeCount and rangeCopy
             * both depend on rangeIterator so that is the only one for which we
             * really need timing data. The iterator should report the
             * cumulative service time when it is finalized.
             */
            {
                
                final CounterSet tmp = counterSet.makePath("rangeQuery");

                tmp.addCounter("#rangeCount", new Instrument<Integer>() {
                    protected void sample() {
                        setValue(nrangeCount);
                    }
                });
                
                tmp.addCounter("#rangeIterator", new Instrument<Integer>() {
                    protected void sample() {
                        setValue(nrangeIterator);
                    }
                });
                
            }
            
            /*
             * Structural mutation statistics.
             * 
             * @todo also merge vs redistribution of keys on remove (and insert
             * if b*-tree)
             */
            {
               
                final CounterSet tmp = counterSet.makePath("structure");
                
                tmp.addCounter("#rootSplit", new Instrument<Integer>() {
                    protected void sample() {
                        setValue(rootsSplit);
                    }
                });

                tmp.addCounter("#rootJoined", new Instrument<Integer>() {
                    protected void sample() {
                        setValue(rootsJoined);
                    }
                });

                tmp.addCounter("#nodeSplit", new Instrument<Integer>() {
                    protected void sample() {
                        setValue(nodesSplit);
                    }
                });

                tmp.addCounter("#nodeJoined", new Instrument<Integer>() {
                    protected void sample() {
                        setValue(nodesJoined);
                    }
                });

                tmp.addCounter("#leafSplit", new Instrument<Integer>() {
                    protected void sample() {
                        setValue(leavesSplit);
                    }
                });

                tmp.addCounter("#leafJoined", new Instrument<Integer>() {
                    protected void sample() {
                        setValue(leavesJoined);
                    }
                });

                tmp.addCounter("#leafCopyOnWrite", new Instrument<Integer>() {
                    protected void sample() {
                        setValue(leavesCopyOnWrite);
                    }
                });

                tmp.addCounter("#nodeCopyOnWrite", new Instrument<Integer>() {
                    protected void sample() {
                        setValue(nodesCopyOnWrite);
                    }
                });

            }
         
            /*
             * IO stats.
             * 
             * FIXME track the serialization times explicitly and separate them
             * from the writes on the store.
             * 
             * @todo add nano timers and track storage used by the index. The
             * goal is to know how much of the time of the server is consumed by
             * the index, what percentage of the store is dedicated to the
             * index, how expensive it is to do some scan-based operations
             * (merge down, delete of transactional isolated persistent index),
             * and evaluate the buffer strategy by comparing accesses with IOs.
             */
            {
                final CounterSet tmp = counterSet.makePath("IO");

                tmp.addCounter("leafWriteCount", new Instrument<Integer>() {
                    protected void sample() {
                        setValue(leavesWritten);
                    }
                });

                tmp.addCounter("nodeWriteCount", new Instrument<Integer>() {
                    protected void sample() {
                        setValue(nodesWritten);
                    }
                });

                // FIXME and per-sec also.
                tmp.addCounter("bytesWritten", new Instrument<Long>() {
                    protected void sample() {
                        setValue(bytesWritten);
                    }
                });

                tmp.addCounter("bytesRead", new Instrument<Long>() {
                    protected void sample() {
                        setValue(bytesRead);
                    }
                });

            }

        }
        
        return counterSet;
        
    }
    private CounterSet counterSet;
    
    public String toString() {

        // FIXME uncomment this - I am just looking for callers.
        if(true) throw new UnsupportedOperationException();
        
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
        "\n"
        ;
        
    }

    static private final NumberFormat commaFormat = NumberFormat.getInstance();
//    static private final NumberFormat percentFormat = NumberFormat.getPercentInstance();
    static private final NumberFormat secondsFormat = NumberFormat.getInstance();
    
    static
    {

        commaFormat.setGroupingUsed(true);
        commaFormat.setMaximumFractionDigits(0);
        
        secondsFormat.setMinimumFractionDigits(3);
        secondsFormat.setMaximumFractionDigits(3);
        
    }

}
