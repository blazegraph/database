package com.bigdata.resources;

import java.util.Arrays;

import com.bigdata.btree.IndexMetadata;

/**
 * The result of a {@link JoinIndexPartitionTask}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class JoinResult extends AbstractResult {

    public final long checkpointAddr;
    
    public final String[] oldnames;
    
    /**
     * 
     * @todo javadoc
     * 
     * @param name
     * @param indexMetadata
     * @param checkpointAddr
     * @param oldnames
     */
    public JoinResult(String name, IndexMetadata indexMetadata, long checkpointAddr, String[] oldnames) {
        
        super(name, indexMetadata);

        this.checkpointAddr = checkpointAddr;
        
        this.oldnames = oldnames;
        
    }
    
    public String toString() {
        
        return "JoinResult{name="+name+", sources="+Arrays.toString(oldnames)+"}";
        
    }


}