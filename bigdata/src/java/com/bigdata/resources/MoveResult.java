package com.bigdata.resources;

import java.util.UUID;

import com.bigdata.btree.IndexMetadata;

/**
 * The object returned by {@link MoveIndexPartitionTask}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MoveResult extends AbstractResult {

    final UUID targetDataServiceUUID;
    final int newPartitionId;
    
    /**
     * @param name 
     * @param indexMetadata
     * @param targetDataServiceUUID
     * @param newPartitionId
     */
    public MoveResult(String name, IndexMetadata indexMetadata,
            UUID targetDataServiceUUID, int newPartitionId) {
        
        super(name, indexMetadata);

        this.targetDataServiceUUID = targetDataServiceUUID;
        
        this.newPartitionId = newPartitionId;
        
    }
    
    public String toString() {
        
        return "MoveResult{name=" + name + ", newPartitionId="
                + newPartitionId + ", targetDataService="
                + targetDataServiceUUID + "}";
        
    }


}