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

import com.bigdata.scaleup.IPartitionMetadata;
import com.bigdata.scaleup.IResourceMetadata;
import com.bigdata.scaleup.PartitionMetadata;

/**
 * An immutable object whose state describes an index partition together with
 * the left and right separator keys surrounding the index partition.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class PartitionMetadataWithSeparatorKeys extends PartitionMetadata {

    /**
     * 
     */
    private static final long serialVersionUID = -1511361004851335936L;
    
    private byte[] leftSeparatorKey;
    private byte[] rightSeparatorKey;

    /**
     * De-serialization constructor.
     */
    public PartitionMetadataWithSeparatorKeys() {
        
    }
    
    public PartitionMetadataWithSeparatorKeys(int partitionId,
            UUID[] dataServices, IResourceMetadata[] resources,
            byte[] leftSeparatorKey, byte[] rightSeparatorKey) {

        super(partitionId, dataServices, resources);

        if (leftSeparatorKey == null)
            throw new IllegalArgumentException("leftSeparatorKey");
        
        // Note: rightSeparatorKey MAY be null.
        
        this.leftSeparatorKey = leftSeparatorKey;
        
        this.rightSeparatorKey = rightSeparatorKey;
        
    }

    public PartitionMetadataWithSeparatorKeys(byte[] leftSeparatorKey,
            IPartitionMetadata src, byte[] rightSeparatorKey) {

        this(src.getPartitionId(), src.getDataServices(), src
                .getResources(), leftSeparatorKey, rightSeparatorKey);
        
    }
    
    /**
     * The separator key that defines the left edge of that index partition
     * (always defined) - this is the first key that can enter the index
     * partition. The left-most separator key for a scale-out index is
     * always an empty byte[] since that is the smallest key that may be
     * defined.
     */
    public byte[] getLeftSeparatorKey() {
       return leftSeparatorKey; 
    }
    
    /**
     * The separator key that defines the right edge of that index partition
     * or [null] iff the index partition does not have a right sibling (a
     * null has the semantics of no upper bound).
     */
    public byte[] getRightSeparatorKey() {
        return rightSeparatorKey;
    }
    
    private static final transient short VERSION0 = 0x0;
    
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        
        super.readExternal(in);
        
        final short version = ShortPacker.unpackShort(in);
        
        if(version!=VERSION0) {
            
            throw new IOException("Unknown version: "+version);
            
        }

        final int leftLen = (int) LongPacker.unpackLong(in);

        final int rightLen = (int) LongPacker.unpackLong(in);

        leftSeparatorKey = new byte[leftLen];
        
        in.read(leftSeparatorKey);
        
        if(rightLen!=0) {
            
            rightSeparatorKey = new byte[rightLen];

            in.read(rightSeparatorKey);

        } else {
            
            rightSeparatorKey = null;
            
        }
        
    }

    public void writeExternal(ObjectOutput out) throws IOException {

        super.writeExternal(out);
        
        ShortPacker.packShort(out, VERSION0);
        
        LongPacker.packLong(out, leftSeparatorKey.length);

        LongPacker.packLong(out, rightSeparatorKey == null ? 0
                : rightSeparatorKey.length);
    
        out.write(leftSeparatorKey);
        
        if(rightSeparatorKey!=null) {
            
            out.write(rightSeparatorKey);
            
        }
        
    }
    
}