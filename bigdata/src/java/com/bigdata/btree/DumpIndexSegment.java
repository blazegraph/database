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
 * Created on May 15, 2008
 */

package com.bigdata.btree;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.btree.IndexSegment.ImmutableNodeFactory.ImmutableLeaf;
import com.bigdata.journal.DumpJournal;
import com.bigdata.rawstore.IRawStore;

/**
 * Utility to examine the context of an {@link IndexSegmentStore}.
 *
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DumpIndexSegment {

    protected static Logger log = Logger.getLogger(DumpIndexSegment.class);
    
    public static void usage() {
     
        System.err.println("usage: " + DumpIndexSegment.class.getSimpleName()
                + " [options] " + " file(s)");
        
        // @todo declare the options that the class understands.
        
        System.err.println("options:");

        System.err.println(" -d level: set the logger level");
        
    }

    /**
     * Dump one or more {@link IndexSegment}s.
     * 
     * @param args
     *            usage <code>[file|-d level]+</code>, where <i>file</i> is
     *            the name of a n {@link IndexSegmentStore} file and
     *            <code>level is the name of the {@link Level} to be used
     *            for the {@link AbstractBTree#dumpLog}</code>
     *            
     * @throws IOException 
     */
    public static void main(String[] args) throws IOException {
        
        if(args.length==0) {
         
            usage();
            
            System.exit(1);
            
        }

        for (int i = 0; i < args.length; i++) {

            final String arg = args[i];

            if (arg.startsWith("-")) {

                if( arg.equals("-d")) {
                    
                    final Level level = Level.toLevel(args[++i]);
                    
                    System.out.println("Setting log level: "+level);
                    
                    // turn up the dumpLog level so that we can see the output.
                    AbstractBTree.dumpLog.setLevel(level);
                    
                } else {
                    
                    System.err.println("Unknown option: "+arg);
                    
                    System.exit( 1 );
                    
                }
                
            } else {

                final File file = new File(arg);

                if (!file.exists()) {

                    System.err.println("No such file: " + file);

                    continue;
                    
                }

                dumpIndexSegment(file);

            }

        }

    }

    static void dumpIndexSegment(final File file) throws IOException {

        /*
         * Note: These options also require you to turn up the logging level in
         * order to see the output. However, when true they will apply a variety
         * of validation tests to the nodes and leaves regardless of whether
         * their state is written onto the console.
         */
        boolean dumpNodeState = true; // @todo command line option
        boolean dumpLeafState = true;// @todo command line option

//        Properties properties = new Properties();
//
//        properties.setProperty(IndexSegmentStore.Options.SEGMENT_FILE,
//                file.getPath());

        final IndexSegmentStore store = new IndexSegmentStore(file);

        // dump the checkpoint record, index metadata record, etc.
        dumpHeaders(store);

        AbstractNode root = store.loadIndexSegment().getRoot();

        // dump the node state.
        if (root instanceof Node) {

            writeBanner("dump nodes");

            // dump the nodes (not the leaves).
            dumpNodes(store, (Node) root, dumpNodeState);

        }

        // dump the leaves using a fast reverse scan.
        boolean fastReverseScan = true;// @todo command line option
        if (fastReverseScan) {

            writeBanner("dump leaves using fast reverse scan");

            dumpLeavesReverseScan(store, dumpLeafState);

        }

        // dump the leaves using a fast forward scan.
        boolean fastForwardScan = true;// @todo command line option
        if (fastForwardScan) {

            writeBanner("dump leaves using fast forward scan");

            dumpLeavesForwardScan(store, dumpLeafState);

        }

        // dump the index contents
        boolean entryScan = true;// @todo command line option.
        boolean showTuples = false;// @todo command line option.
        if (entryScan) {

            writeBanner("dump keys and values using iterator");

            DumpJournal.dumpIndex(store.loadIndexSegment(),showTuples);

        }

    }

    static void dumpHeaders(final IndexSegmentStore store) throws IOException {

        System.out.println("file        : " + store.getFile());

        System.out.println("checkpoint  : " + store.getCheckpoint().toString());

        System.out.println("metadata    : " + store.getIndexMetadata().toString());
        
        System.out.println("bloomFilter : "
                + (store.getCheckpoint().addrBloom != IRawStore.NULL ? store
                        .readBloomFilter().toString() : "N/A"));
        
    }
    
    /**
     * Dumps nodes (but not leaves) using a low-level approach.
     * 
     * @param store
     * 
     * @param node
     */
    static void dumpNodes(final IndexSegmentStore store, final Node node,
            final boolean dumpNodeState) {

        if(dumpNodeState)
            node.dump(System.out);
        
        final int nkeys = node.getKeyCount();
        
        for (int i = 0; i <= nkeys; i++) {

            final long addr = node.getChildAddr(i);

            if (store.getAddressManager().isNodeAddr(addr)) {

                final Node child;
                if (true) {

                    // normal read following the node hierarchy, using cache, etc.
                    child = (Node) node.getChild(i);

                } else {

                    // lower level read 
                    final ByteBuffer data = store.read(addr);

                    // note: does NOT set the parent reference on the Node!
                    child = (Node) node.btree.nodeSer.getNode(node.btree, addr,
                            data);

                }

                // recursive dump
                dumpNodes(store, child, dumpNodeState);

            }
            
        }
        
    }

    /**
     * Low-level routine descends the left-most path from the root and returns
     * the address of the left-most leaf.
     * 
     * @param store 
     * @param addr
     * @return
     */
    static long getFirstLeafAddr(IndexSegmentStore store, long addr) {
        
        if(store.getAddressManager().isNodeAddr(addr)) {
         
            // lower level read 
            ByteBuffer data = store.read(addr);

            final AbstractBTree btree = store.loadIndexSegment(); 
            
            // note: does NOT set the parent reference on the read Node!
            Node child = (Node) btree.nodeSer.getNode(btree, addr, data);
            
            // left most child
            return getFirstLeafAddr(store, child.getChildAddr(0));
            
        }

        // found the left most leaf.
        return addr;
        
    }
    
    /**
     * Low-level routine descends the left-most path from the root and returns
     * the address of the left-most leaf.
     * 
     * @param store 
     * @param addr
     * @return
     */
    static long getLastLeafAddr(final IndexSegmentStore store, final long addr) {

        if(store.getAddressManager().isNodeAddr(addr)) {
         
            // lower level read 
            final ByteBuffer data = store.read(addr);

            final AbstractBTree btree = store.loadIndexSegment(); 
            
            // note: does NOT set the parent reference on the read Node!
            final Node child = (Node) btree.nodeSer.getNode(btree, addr, data);
            
            // right most child
            return getLastLeafAddr(store, child.getChildAddr(child.getKeyCount()));
            
        }

        // found the right most leaf.
        return addr;
        
    }
    
    /**
     * Dump leaves by direct record scan from first leaf offset until end of
     * leaves region.
     * <p>
     * Note: While this could be rewritten for cleaner code to use
     * {@link IndexSegment#leafIterator(boolean)} but it would make it harder to
     * spot problems in the data.
     * 
     * @param store
     */
    static void dumpLeavesReverseScan(IndexSegmentStore store,boolean dumpLeafState) {
        
        final long begin = System.currentTimeMillis();
        
        final AbstractBTree btree = store.loadIndexSegment(); 
        
        // first the address of the first leaf in a right-to-left scan (always defined).
        long addr = store.getCheckpoint().addrLastLeaf;
        
        System.out.println("lastLeafAddr="+store.toString(addr));

        {
            
            final long addr2 = getLastLeafAddr(store,
                    store.getCheckpoint().addrRoot);
            
            if (addr != addr2) {

                log.error("Last leaf address is inconsistent? checkpoint reports: "
                                + addr
                                + " ("
                                + store.toString(addr)
                                + ")"
                                + ", but node hierarchy reports "
                                + addr2
                                + " ("
                                + store.toString(addr2) + ")");
                
            }
            
        }

        int nscanned = 0;
        
        while (true) {

            if(!store.getAddressManager().isLeafAddr(addr)) {
                
                log.error("Not a leaf address: "+store.toString(addr)+" : aborting scan");

                // abort scan.
                break;
                
            }

            // lower level read 
            ByteBuffer data = store.read(addr);

            // note: does NOT set the parent reference on the Leaf!
            Leaf leaf = (Leaf) btree.nodeSer.getLeaf(btree, addr, data);

            if(dumpLeafState) leaf.dump(System.out);
            
            nscanned++;
            
            final long priorAddr = ((ImmutableLeaf)leaf).priorAddr;
            
            if (priorAddr == -1L) {

                log.error("Expecting the prior address to be known - aborting scan: current addr="
                                + addr+" ("+store.toString(addr)+")");
                
                // abort scan.
                break;

            }
            
            if(priorAddr == 0L) {
            
                if (nscanned != store.getCheckpoint().nleaves) {

                    log.error("Scanned "
                                    + nscanned
                                    + " leaves, but checkpoint record indicates that there are "
                                    + store.getCheckpoint().nleaves + " leaves");
                    
                }
                
                // Done (normal completion).
                break;
                
            }
            
            // go to the previous leaf in the key order.
            addr = priorAddr;
            
        }

        final long elapsed = System.currentTimeMillis() - begin;
        
        System.out.println("Visited "+nscanned+" leaves using fast reverse scan in "+elapsed+" ms");
        
    }

    /**
     * Dump leaves by direct record scan from first leaf offset until end of
     * leaves region.
     * <p>
     * Note: While this could be rewritten for cleaner code to use
     * {@link IndexSegment#leafIterator(boolean)} but it would make it harder to
     * spot problems in the data.
     * 
     * @param store
     */
    static void dumpLeavesForwardScan(IndexSegmentStore store,boolean dumpLeafState) {
        
        final long begin = System.currentTimeMillis();
        
        final AbstractBTree btree = store.loadIndexSegment(); 

        // first the address of the first leaf in a left-to-right scan (always defined).
        long addr = store.getCheckpoint().addrFirstLeaf;
        
        {
            
            final long addr2 = getFirstLeafAddr(store,
                    store.getCheckpoint().addrRoot);
            
            if (addr != addr2) {

                log.error("First leaf address is inconsistent? checkpoint reports: "
                        + addr
                        + " ("
                        + store.toString(addr)
                        + ")"
                        + ", but node hierarchy reports "
                        + addr2
                        + " ("
                        + store.toString(addr2) + ")");
                
            }
            
        }
        
        System.out.println("firstLeafAddr="+store.toString(addr));

        int nscanned = 0;
        
        while (true) {

            if(!store.getAddressManager().isLeafAddr(addr)) {
                
                log.error("Not a leaf address: "+store.toString(addr)+" : aborting scan");

                // abort scan.
                break;
                
            }
            
            // lower level read 
            ByteBuffer data = store.read(addr);

            // note: does NOT set the parent reference on the Leaf!
            Leaf leaf = (Leaf) btree.nodeSer.getLeaf(btree, addr, data);

            if(dumpLeafState) leaf.dump(System.out);
            
            nscanned++;
            
            final long nextAddr = ((ImmutableLeaf)leaf).nextAddr;
            
            if (nextAddr == -1L) {

                log.error("Expecting the next address to be known - aborting scan: current addr="
                        + addr+" ("+store.toString(addr)+")");
                
                // abort scan.
                break;

            }
            
            if(nextAddr == 0L) {
                
                if (nscanned != store.getCheckpoint().nleaves) {

                    log.error("Scanned "
                                    + nscanned
                                    + " leaves, but checkpoint record indicates that there are "
                                    + store.getCheckpoint().nleaves + " leaves");
                    
                }

                // Done (normal completion).
                break;
                
            }
            
            addr = nextAddr;
                        
        }


        final long elapsed = System.currentTimeMillis() - begin;
        
        System.out.println("Visited "+nscanned+" leaves using fast forward scan in "+elapsed+" ms");

    }

    static void writeBanner(String s) {
    
        System.out.println(bar);
        System.out.println("=== "+s);
        System.out.println(bar);
        
    }
    
    static final String bar = "============================================================";
    
}
