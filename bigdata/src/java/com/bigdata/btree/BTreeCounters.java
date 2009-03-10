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

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.ICounterSet;
import com.bigdata.counters.Instrument;

/**
 * A helper class that collects statistics on an {@link AbstractBTree}.
 * <p>
 * Note: This class DOES NOT have a hard reference to the {@link AbstractBTree}.
 * Holding an instance of this class WILL NOT force the {@link AbstractBTree} to
 * remain strongly reachable.
 * 
 * @todo counters need to be atomic if we want to avoid the possibility of
 *       concurrent <code>x++</code> operations failing to correctly increment
 *       <code>x</code> for each request. I have not seen any problems
 *       resulting from this.
 * 
 * @todo {@link #add(BTreeCounters)}, {@link #subtract(BTreeCounters)} and the
 *       copy ctor are not atomic. I have not seen any problems resulting from
 *       this.
 * 
 * @todo instrument the key search time
 * 
 * @todo instrument bloom filter time when the bloom filter is used (this should
 *       be done separately on the bloom filter object).
 * 
 * @todo At present, deserialization is much slower than serialization,
 *       primarily because of object creation. Changing to a raw record format
 *       for nodes and leaves would likely reduce the deserialization costs
 *       significantly. The time could also be reduced by lazy decompression of
 *       the values and lazy deserialization of the values such that tuple scans
 *       for keys only do not require value deserialization.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
final public class BTreeCounters {

    public BTreeCounters() {
        
    }

    /**
     * Equal iff they are the same instance.
     */
    public boolean equals(Object o) {
        
        return this == o;
        
    }
    
    /**
     * Copy constructor.
     * 
     * @param c
     */
    public BTreeCounters(final BTreeCounters c) {
        
        add(c);
        
    }
    
//    /**
//     * Returns a "mark". The mark is a copy of the counters taken at the moment
//     * requested. You can compute the delta between the last mark or any mark
//     * using {@link #subtract(BTreeCounters)}.
//     */
//    public BTreeCounters mark() {
//       
//        final BTreeCounters mark = new BTreeCounters();
//        
//        mark.add(this);
//        
//        return mark;
//        
//    }

    /**
     * Adds the values from another {@link BTreeCounters} object to this one.
     * 
     * @param o
     */
    public void add(final BTreeCounters o) {
        
        if (o == null)
            throw new IllegalArgumentException();
        
        // IKeySearch
        nfinds += o.nfinds;
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
        headSplit += o.headSplit;
        tailSplit += o.tailSplit;
        nodesCopyOnWrite += o.nodesCopyOnWrite;
        leavesCopyOnWrite += o.leavesCopyOnWrite;
        // tuple mutation.
        ntupleInsertValue += o.ntupleInsertValue;
        ntupleInsertDelete += o.ntupleInsertDelete;
        ntupleUpdateValue += o.ntupleUpdateValue;
        ntupleUpdateDelete += o.ntupleUpdateDelete;
        ntupleRemove += o.ntupleRemove;
        // IO
        nodesRead += o.nodesRead;
        leavesRead += o.leavesRead;
        nodesWritten += o.nodesWritten;
        leavesWritten += o.leavesWritten;
        bytesRead += o.bytesRead;
        bytesWritten += o.bytesWritten;
        
        serializeNanos += o.serializeNanos;
        deserializeNanos += o.deserializeNanos;
        writeNanos += o.writeNanos;
        readNanos += o.readNanos;
        
    }
    
    /**
     * Subtracts the given counters from the current counters, returning a new
     * counter object containing their difference.
     * 
     * @param o
     */
    public BTreeCounters subtract(final BTreeCounters o) {
        
        if (o == null)
            throw new IllegalArgumentException();
        
        // make a copy of the current counters.
        final BTreeCounters t = new BTreeCounters(this);
        
        /*
         * Subtract out the given counters.
         */
        
        // IKeySearch
        t.nfinds -= o.nfinds;
        t.ninserts -= o.ninserts;
        t.nremoves -= o.nremoves;
        // ILinearList
        t.nindexOf -= o.nindexOf; // Note: also does key search.
        t.ngetKey  -= o.ngetKey;
        t.ngetValue -= o.ngetValue;
        // IRangeQuery
        t.nrangeCount -= o.nrangeCount;
        t.nrangeIterator -= o.nrangeIterator;
        // Structural mutation.
        t.rootsSplit -= o.rootsSplit;
        t.rootsJoined -= o.rootsJoined;
        t.nodesSplit -= o.nodesSplit;
        t.nodesJoined -= o.nodesJoined;
        t.leavesSplit -= o.leavesSplit;
        t.leavesJoined -= o.leavesJoined;
        t.headSplit -= o.headSplit;
        t.tailSplit -= o.tailSplit;
        t.nodesCopyOnWrite -= o.nodesCopyOnWrite;
        t.leavesCopyOnWrite -= o.leavesCopyOnWrite;
        // tuple mutation.
        t.ntupleInsertValue -= o.ntupleInsertValue;
        t.ntupleInsertDelete -= o.ntupleInsertDelete;
        t.ntupleUpdateValue -= o.ntupleUpdateValue;
        t.ntupleUpdateDelete -= o.ntupleUpdateDelete;
        t.ntupleRemove -= o.ntupleRemove;
        // IO
        t.nodesRead -= o.nodesRead;
        t.leavesRead -= o.leavesRead;
        t.nodesWritten -= o.nodesWritten;
        t.leavesWritten -= o.leavesWritten;
        t.bytesRead -= o.bytesRead;
        t.bytesWritten -= o.bytesWritten;
        
        t.serializeNanos -= o.serializeNanos;
        t.deserializeNanos -= o.deserializeNanos;
        t.writeNanos -= o.writeNanos;
        t.readNanos -= o.readNanos;
        
        return t;
        
    }
    
    /**
     * #of keys looked up in the tree by contains/lookup(key) (does not count
     * those rejected by the bloom filter before they are tested against the
     * B+Tree).
     */
    public long nfinds = 0;
    public long ninserts = 0;
    public long nremoves = 0;
    // ILinearList
    public long nindexOf = 0;
    public long ngetKey = 0;
    public long ngetValue = 0;
    // IRangeQuery
    public long nrangeCount = 0;
    public long nrangeIterator = 0;
    // Structural change.
    public int rootsSplit = 0;
    public int rootsJoined = 0;
    public int nodesSplit = 0;
    public int nodesJoined = 0;
    public int leavesSplit = 0;
    public int leavesJoined = 0;
    public int tailSplit = 0;
    public int headSplit = 0;
    public int nodesCopyOnWrite = 0;
    public int leavesCopyOnWrite = 0;
    
    /*
     * Tuple counters.
     * 
     * Note: The tuple level counters can be used to determine the degree of
     * insert, vs update, vs delete activity on a B+Tree. How you count
     * "deletes" depends on whether or not delete markers are in use - when they
     * are in use deletes are in fact updates (or inserts if the key was not
     * pre-existing). When delete markers are not in use, deletes actually
     * remove the tuple.
     * 
     * Note: You CAN NOT determine the actual net change in the #of tuples in
     * the B+Tree that is part of a fused view using these counters since the
     * same tuple may have been inserted, deleted and updated and may or may not
     * exist in the other sources of the fused view. However, these counters MAY
     * be used as an indication that a compacting merge might reduce the #of
     * tuples in the view (e.g., a lot of insertDelete or updateDelete activity)
     * and that might lead you to choose to perform a compacting merge before
     * deciding whether or not to split an index partition.
     */
    
    /**
     * #of non-deleted tuples that were inserted into the B+Tree (rather than
     * updating the value for an existing tuple).
     */
    public long ntupleInsertValue = 0;

    /**
     * #of deleted tuples that were inserted into the B+Tree (rather than
     * deleting the value for an existing tuple). Note that delete markers MAY
     * be written into a B+Tree when there is no tuple for that key in the
     * BTree. Those cases arise when the B+Tree is used to isolate a
     * transaction's write set and are counted here.
     */
    public long ntupleInsertDelete = 0;
    /**
     * #of pre-existing tuples whose value was updated to a non-deleted value
     * (includes update of a deleted tuple to a non-deleted tuple by overwrite
     * of the tuple).
     */
    public long ntupleUpdateValue = 0;
    /**
     * #of pre-existing un-deleted tuples whose delete marker was set (we don't
     * count re-deletes of an already deleted tuple).
     */
    public long ntupleUpdateDelete = 0;
    /**
     * #of pre-existing tuples that were removed from the B+Tree (only non-zero
     * when the B+Tree does not support delete markers).
     */
    public long ntupleRemove = 0;
    
    // IO
    public int nodesRead = 0;
    public int leavesRead = 0;
    public int nodesWritten = 0;
    public int leavesWritten = 0;
    public long bytesRead = 0L;
    public long bytesWritten = 0L;
    /*
     * Note: The nano time fields are for nodes+leaves.
     */
    public long serializeNanos = 0;
    public long deserializeNanos = 0;
    public long writeNanos = 0;
    public long readNanos = 0;
    
    /**
     * Return a score whose increasing value is correlated with the amount of
     * read/write activity on an index as reflected in these
     * {@link BTreeCounters}.
     * <p>
     * The score is the serialization / deserialization time plus the read /
     * write time. Time was choosen since it is a common unit and since it
     * reflects the combination of CPU time, memory time (for allocations and
     * garbage collection - the latter can be quite significant), and the disk
     * wait time. The other main component of time is key search, but that is
     * not instrumented right now.
     * <p>
     * Serialization and deserialization are basically a CPU activity and drive
     * memory to the extent that allocations are made, especially during
     * deserialization.
     * <p>
     * The read/write time is strongly dominated by actual DISK IO and by
     * garbage collection time (garbage collection can cause threads to be
     * suspended at any time). For deep B+Trees, DISK READ time dominates DISK
     * WRITE time since increasing numbers of random reads are required to
     * materialize any given leaf.
     * 
     * @return The computed score.
     */
    public double computeRawReadWriteScore() {
        
        return //
            (serializeNanos + deserializeNanos) + //
            (writeNanos + readNanos)//
            ;
        
    }

    /**
     * Return a score whose increasing value is correlated with the amount of
     * read activity on an index as reflected in these {@link BTreeCounters}.
     * The score is {@link #deserializeNanos} + {@link #readNanos}.
     * 
     * @see #computeRawReadWriteScore()
     */
    public double computeRawReadScore() {
        
        return deserializeNanos + readNanos;
        
    }

    /**
     * Return a score whose increasing value is correlated with the amount of
     * write activity on an index as reflected in these {@link BTreeCounters}.
     * The score is {@link #serializeNanos} plus {@link #writeNanos}.
     * 
     * @see #computeRawReadWriteScore()
     */
    public double computeRawWriteScore() {
        
        return serializeNanos + writeNanos;
        
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
    static public double normalize(final double rawScore,
            final double totalRawScore) {
        
        if (totalRawScore == 0d) {

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
//    synchronized 
    public ICounterSet getCounters() {
        
//        if(counterSet == null) {
            
        final CounterSet counterSet = new CounterSet();

            /*
             * ISimpleBTree
             *
             * @todo instrument key search time.
             */
            {
                
                final CounterSet tmp = counterSet.makePath("keySearch");

                tmp.addCounter("#find", new Instrument<Long>() {
                    protected void sample() {
                        setValue(nfinds);
                    }
                });

                tmp.addCounter("#insert", new Instrument<Long>() {
                    protected void sample() {
                        setValue(ninserts);
                    }
                });

                tmp.addCounter("#nremove", new Instrument<Long>() {
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

                tmp.addCounter("#indexOf", new Instrument<Long>() {
                    protected void sample() {
                        setValue(nindexOf);
                    }
                });

                tmp.addCounter("#getKey", new Instrument<Long>() {
                    protected void sample() {
                        setValue(ngetKey);
                    }
                });

                tmp.addCounter("#getValue", new Instrument<Long>() {
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

                tmp.addCounter("#rangeCount", new Instrument<Long>() {
                    protected void sample() {
                        setValue(nrangeCount);
                    }
                });
                
                tmp.addCounter("#rangeIterator", new Instrument<Long>() {
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

                tmp.addCounter("#headSplit", new Instrument<Integer>() {
                    protected void sample() {
                        setValue(headSplit);
                    }
                });

                tmp.addCounter("#tailSplit", new Instrument<Integer>() {
                    protected void sample() {
                        setValue(tailSplit);
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
             * Tuple stats.
             */
            {
                
                final CounterSet tmp = counterSet.makePath("tuples");

                tmp.addCounter("#insertValue", new Instrument<Long>() {
                    protected void sample() {
                        setValue(ntupleInsertValue);
                    }
                });
                tmp.addCounter("#insertDelete", new Instrument<Long>() {
                    protected void sample() {
                        setValue(ntupleInsertDelete);
                    }
                });

                tmp.addCounter("#updateValue", new Instrument<Long>() {
                    protected void sample() {
                        setValue(ntupleUpdateValue);
                    }
                });

                tmp.addCounter("#updateDelete", new Instrument<Long>() {
                    protected void sample() {
                        setValue(ntupleUpdateDelete);
                    }
                });

                tmp.addCounter("#tupleRemove", new Instrument<Long>() {
                    protected void sample() {
                        setValue(ntupleRemove);
                    }
                });
            }

            /*
             * IO stats.
             */
            {

                final CounterSet tmp = counterSet.makePath("IO");

                /*
                 * nodes/leaves read/written
                 */

                tmp.addCounter("leafReadCount", new Instrument<Integer>() {
                    protected void sample() {
                        setValue(leavesRead);
                    }
                });

                tmp.addCounter("nodeReadCount", new Instrument<Integer>() {
                    protected void sample() {
                        setValue(nodesRead);
                    }
                });

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

                /*
                 * store reads (bytes)
                 */
                tmp.addCounter("bytesRead", new Instrument<Long>() {
                    protected void sample() {
                        setValue(bytesRead);
                    }
                });

                tmp.addCounter("readSecs", new Instrument<Double>() {
                    public void sample() {
                        final double elapsedReadSecs = (readNanos / 1000000000.);
                        setValue(elapsedReadSecs);
                    }
                });

                tmp.addCounter("bytesReadPerSec",
                        new Instrument<Double>() {
                            public void sample() {
                                final double readSecs = (readNanos / 1000000000.);
                                final double bytesReadPerSec = (readSecs == 0L ? 0d
                                        : (bytesRead / readSecs));
                                setValue(bytesReadPerSec);
                            }
                        });

                /*
                 * store writes (bytes)
                 */
                tmp.addCounter("bytesWritten", new Instrument<Long>() {
                    protected void sample() {
                        setValue(bytesWritten);
                    }
                });

                tmp.addCounter("writeSecs", new Instrument<Double>() {
                    public void sample() {
                        final double writeSecs = (writeNanos / 1000000000.);
                        setValue(writeSecs);
                    }
                });

                tmp.addCounter("bytesWrittenPerSec",
                        new Instrument<Double>() {
                            public void sample() {
                                final double writeSecs = (writeNanos/ 1000000000.);
                                final double bytesWrittenPerSec = (writeSecs == 0L ? 0d
                                        : (bytesWritten / writeSecs));
                                setValue(bytesWrittenPerSec);
                            }
                        });
                
                /*
                 * node/leaf (de-)serialization times.
                 */
                
                tmp.addCounter("serializeSecs", new Instrument<Double>() {
                    public void sample() {
                        final double serializeSecs = (serializeNanos / 1000000000.);
                        setValue(serializeSecs);
                    }
                });

                tmp.addCounter("serializePerSec",
                        new Instrument<Double>() {
                            public void sample() {
                                final double serializeSecs = (serializeNanos / 1000000000.);
                                final double serializePerSec = (serializeSecs== 0L ? 0d
                                        : ((nodesRead+leavesRead) / serializeSecs));
                                setValue(serializePerSec);
                            }
                        });

                tmp.addCounter("deserializeSecs", new Instrument<Double>() {
                    public void sample() {
                        final double deserializeSecs = (deserializeNanos / 1000000000.);
                        setValue(deserializeSecs);
                    }
                });

                tmp.addCounter("deserializePerSec",
                        new Instrument<Double>() {
                            public void sample() {
                                final double deserializeSecs = (deserializeNanos / 1000000000.);
                                final double deserializePerSec = (deserializeSecs== 0L ? 0d
                                        : ((nodesRead+leavesRead) / deserializeSecs));
                                setValue(deserializePerSec);
                            }
                        });
                
            }

//        }
        
        return counterSet;
        
    }
//    private CounterSet counterSet;
    
    public String toString() {

        // in XML.
        return getCounters().asXML(null/* filter */);
        
    }

}
