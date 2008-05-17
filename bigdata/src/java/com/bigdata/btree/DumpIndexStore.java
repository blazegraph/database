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
import java.nio.ByteBuffer;
import java.util.Properties;

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
public class DumpIndexStore {

    protected static Logger log = Logger.getLogger(DumpIndexStore.class);
    
    public static void usage() {
     
        System.err.println("usage: " + DumpIndexStore.class.getSimpleName()
                + "[options] " + " file(s)");
        
//        System.err.println("options:");
//
//        System.err.println(" -i: dump Index");
        
    }
    
    public static void main(String[] args) {
        
        if(args.length==0) {
         
            usage();
            
            System.exit(1);
            
        }

        for (String fileStr : args) {

            File file = new File(fileStr);

            if (!file.exists()) {

                System.err.println("No such file: " + fileStr);

                System.exit(1);

            }

            Properties properties = new Properties();

            properties.setProperty(IndexSegmentStore.Options.SEGMENT_FILE,
                    fileStr);

            IndexSegmentStore store = new IndexSegmentStore(properties);

            // dump the checkpoint record, index metadata record, etc.
            dump(store);
            
            AbstractNode root = store.loadIndexSegment().getRoot(); 
            
            // turn up the dumpLog level so that we can see the output.
            BTree.dumpLog.setLevel(Level.DEBUG);

            if(root instanceof Node) {

                writeBanner("dump nodes");

                // dump the nodes (not the leaves).
                dumpNodes(store,(Node)root);
                
            }
            
            // dump the leaves using a fast reverse scan. @todo command line option
            if(false) {

                writeBanner("dump leaves using fast reverse scan");

                dumpLeavesReverseScan(store);
                
            }

            // dump the leaves using a fast forward scan. @todo command line option
            if(false) {

                writeBanner("dump leaves using fast forward scan");

                dumpLeavesForwardScan(store);
                
            }
        
            // dump the index contents : @todo command line option.
            if(true) {
                
                writeBanner("dump keys and values using iterator");

                DumpJournal.dumpIndex(store.loadIndexSegment());
                
            }
            
        }

    }

    static void dump(IndexSegmentStore store) {

        System.err.println("file        : " + store.getFile());

        System.err.println("checkpoint  : " + store.getCheckpoint().toString());

        System.err.println("metadata    : " + store.getIndexMetadata().toString());
        
        System.err.println("bloomFilter : "
                + (store.getCheckpoint().addrBloom != IRawStore.NULL ? store
                        .getBloomFilter().toString() : "N/A"));
        
    }
    
    /**
     * Dumps nodes (but not leaves) using a low-level approach.
     * 
     * @param store
     * 
     * @param node
     */
    static void dumpNodes(IndexSegmentStore store, Node node) {

        node.dump(System.err);
        
        for (int i = 0; i <= node.nkeys; i++) {

            long addr = node.childAddr[i];

            if (store.getAddressManager().isNodeAddr(addr)) {

                final Node child;
                if (true) {

                    // normal read following the node hierarchy, using cache, etc.
                    child = (Node) node.getChild(i);

                } else {

                    // lower level read 
                    ByteBuffer data = store.read(addr);

                    // note: does NOT set the parent reference on the Node!
                    child = (Node) node.btree.nodeSer.getNode(node.btree, addr,
                            data);

                }

                // recursive dump
                dumpNodes(store, child);

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
            return getFirstLeafAddr(store, child.childAddr[0]);
            
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
    static long getLastLeafAddr(IndexSegmentStore store, long addr) {
        
        if(store.getAddressManager().isNodeAddr(addr)) {
         
            // lower level read 
            ByteBuffer data = store.read(addr);

            final AbstractBTree btree = store.loadIndexSegment(); 
            
            // note: does NOT set the parent reference on the read Node!
            Node child = (Node) btree.nodeSer.getNode(btree, addr, data);
            
            // right most child
            return getLastLeafAddr(store, child.childAddr[child.nkeys]);
            
        }

        // found the right most leaf.
        return addr;
        
    }
    
    /**
     * Dump leaves by direct record scan from first leaf offset until end of
     * leaves region.
     * 
     * FIXME refactor for fast forward/reverse scan in {@link IndexSegment}.
     * 
     * @param store
     */
    static void dumpLeavesReverseScan(IndexSegmentStore store) {
        
        final AbstractBTree btree = store.loadIndexSegment(); 
        
        // first the address of the first leaf in a right-to-left scan (always defined).
        long addr = store.getCheckpoint().addrLastLeaf;
        
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
                
                return;
                
            }

            // lower level read 
            ByteBuffer data = store.read(addr);

            // note: does NOT set the parent reference on the Leaf!
            Leaf leaf = (Leaf) btree.nodeSer.getLeaf(btree, addr, data);

            leaf.dump(System.err);
            
            nscanned++;
            
            final long priorAddr = ((ImmutableLeaf)leaf).priorAddr;
            
            if (priorAddr == -1L) {

                log.error("Expecting the prior address to be known - aborting scan: current addr="
                                + addr+" ("+store.toString(addr)+")");
                
                // abort scan.
                return;

            }
            
            if(priorAddr == 0L) {
            
                if (nscanned != store.getCheckpoint().nleaves) {

                    log.error("Scanned "
                                    + nscanned
                                    + " leaves, but checkpoint record indicates that there are "
                                    + store.getCheckpoint().nleaves + " leaves");
                    
                }
                
                // Done.
                return;
                
            }
            
            // go to the previous leaf in the key order.
            addr = priorAddr;
            
        }

    }

    /**
     * Dump leaves by direct record scan from first leaf offset until end of
     * leaves region.
     * 
     * FIXME refactor for fast forward/reverse scan in {@link IndexSegment}.
     * 
     * @param store
     */
    static void dumpLeavesForwardScan(IndexSegmentStore store) {
        
        final AbstractBTree btree = store.loadIndexSegment(); 

        // first the address of the first leaf in a left-to-right scan (always defined).
        long addr = store.getCheckpoint().addrFirstLeaf;
        
        { // @todo same test for the reverse scan.
            
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
        
        System.err.println("firstLeafAddr="+store.toString(addr));

        int nscanned = 0;
        
        while (true) {

            if(!store.getAddressManager().isLeafAddr(addr)) {
                
                log.error("Not a leaf address: "+store.toString(addr)+" : aborting scan");

                // abort scan.
                
                return;
                
            }
            
            // lower level read 
            ByteBuffer data = store.read(addr);

            // note: does NOT set the parent reference on the Leaf!
            Leaf leaf = (Leaf) btree.nodeSer.getLeaf(btree, addr, data);

            leaf.dump(System.err);
            
            nscanned++;
            
            final long nextAddr = ((ImmutableLeaf)leaf).nextAddr;
            
            if (nextAddr == -1L) {

                log.error("Expecting the next address to be known - aborting scan: current addr="
                        + addr+" ("+store.toString(addr)+")");
                
                // abort scan.
                return;

            }
            
            if(nextAddr == 0L) {
                
                if (nscanned != store.getCheckpoint().nleaves) {

                    log.error("Scanned "
                                    + nscanned
                                    + " leaves, but checkpoint record indicates that there are "
                                    + store.getCheckpoint().nleaves + " leaves");
                    
                }

                // Done.
                return;
                
            }
            
            addr = nextAddr;
                        
        }

    }

    static void writeBanner(String s) {
    
        System.out.println(bar);
        System.out.println("=== "+s);
        System.out.println(bar);
        
    }
    
    static final String bar = "============================================================";
    
}
