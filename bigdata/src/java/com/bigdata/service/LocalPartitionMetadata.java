/*

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
package com.bigdata.service;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;

import org.CognitiveWeb.extser.LongPacker;
import org.CognitiveWeb.extser.ShortPacker;

import com.bigdata.scaleup.IResourceMetadata;

/**
 * Per-partition metadata records describing each partition of this index
 * that has been mapped onto the backing data service.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class LocalPartitionMetadata extends PartitionMetadataWithSeparatorKeys {

    /**
     * 
     */
    private static final long serialVersionUID = -7080351492885042697L;

//    /**
//     * True if there are writes on the index partition that need to be
//     * handled on overflow (set true each time there is a write on the index
//     * partition and made restart-safe iff the index is committed).
//     */
//    private boolean dirty = false;
    
    /**
     * Per-index partition counter. Counter values as reported to the
     * application are int64 values where the high int32 bits are the
     * partition identifier, thereby making the counters capable of
     * independent update without global syncronization.
     */
    private int counter;
    
    /**
     * De-serialization constructor.
     */
    public LocalPartitionMetadata() {
        
    }
    
//    public LocalPartitionMetadata(PartitionMetadataWithSeparatorKeys src) {
//
//        this(src.getPartitionId(), src.getDataServices(), src
//                .getResources(), src.getLeftSeparatorKey(), src
//                .getRightSeparatorKey());
//
//    }

    public LocalPartitionMetadata(int partitionId, UUID[] dataServices,
            IResourceMetadata[] resources, byte[] leftSeparatorKey,
            byte[] rightSeparatorKey) {

        super(partitionId, dataServices, resources, leftSeparatorKey,
                rightSeparatorKey);
        
//        dirty = false; // nothing written on the partition yet.

        counter = 0; // initialize counter to zero.
        
    }

    private static final transient short VERSION0 = 0x0;
    
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        
        super.readExternal(in);
        
        final short version = ShortPacker.unpackShort(in);
        
        if(version!=VERSION0) {
            
            throw new IOException("Unknown version: "+version);
            
        }

//        dirty = in.readBoolean();
        
        counter = (int) LongPacker.unpackLong(in);
        
    }

    public void writeExternal(ObjectOutput out) throws IOException {

        super.writeExternal(out);
        
        ShortPacker.packShort(out, VERSION0);
        
//        out.writeBoolean(dirty);
        
        LongPacker.packLong(out, counter);
        
    }

}