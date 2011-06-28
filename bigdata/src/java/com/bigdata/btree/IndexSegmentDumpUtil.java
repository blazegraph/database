package com.bigdata.btree;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;

import com.bigdata.btree.IndexSegment.ImmutableLeafCursor;
import com.bigdata.btree.IndexSegment.ImmutableNodeFactory.ImmutableLeaf;

public class IndexSegmentDumpUtil {
    /**
     * @param args
     */
    public static void main(String[] args) {
        
        try {
            
            if (args.length != 1) {
                
                printUsage();
                
                return;
                
            }
            
            File file = new File(args[0]);
            
            // File file = new File("D:\\temp\\segs\\U100000_spo_SPO_part00139_51095.seg");
            
            IndexSegmentStore store = new IndexSegmentStore(file);
            
            System.err.println("checkpoint: "+store.getCheckpoint());
            
            IndexSegment ndxSegment = store.loadIndexSegment();
            
            // wait for enter
            
            long before = Runtime.getRuntime().freeMemory();
            
            System.err.println("free memory before loading first leaf: " + before);
            
            System.err.println("take your memory dump and then press enter to load the first leaf");
            
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            
            String input = reader.readLine();
            
            ImmutableLeafCursor cursor = ndxSegment.newLeafCursor(SeekEnum.First);
            
            ImmutableLeaf leaf = cursor.first();

            long after = Runtime.getRuntime().freeMemory();
            
            long used = before - after;
            
            System.err.println("free memory after loading first leaf: " + after);
            
            System.err.println("free memory used loading first leaf: " + used);
            
            // byte counts on disk
            
            System.err.println("bytes on disk for root: " + store.getByteCount(store.getCheckpoint().addrRoot));

            System.err.println("bytes on disk for first leaf: " + store.getByteCount(store.getCheckpoint().addrFirstLeaf));
            
            System.err.println("take your memory dump and then press enter to end the program");
            
            input = reader.readLine();
            
            // hit enter
            
            // compare free memory before and after loading leaf
            
            // measuer of inflation in memory
            
            // exclude weak references, leaves and nodes have weak refs to segment
            // total retained memory for node and leaf
            
        } catch (Exception ex) {
            
            ex.printStackTrace();
            
        }
        
    }
    
    private static void printUsage() {
        
        System.err.println("You must specify an index segment store file");
        
    }
    
}
