package com.bigdata.btree;

import java.io.File;

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
            
            IndexSegmentStore store = new IndexSegmentStore(file);
            
            IndexSegment ndxSegment = store.loadIndexSegment();
            
            ImmutableLeafCursor cursor = ndxSegment.newLeafCursor(SeekEnum.First);
            
            ImmutableLeaf leaf = cursor.first();
            
            Thread.sleep(120*1000);  // gives you 2 minutes to go get the memory dump
            
        } catch (Exception ex) {
            
            ex.printStackTrace();
            
        }
        
    }
    
    private static void printUsage() {
        
        System.err.println("You must specify an index segment store file");
        
    }
    
}
