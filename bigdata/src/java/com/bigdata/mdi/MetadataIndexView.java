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
 * Created on Feb 20, 2008
 */

package com.bigdata.mdi;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.DelegateIndex;
import com.bigdata.btree.ILinearList;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.io.SerializerUtil;
import com.bigdata.mdi.MetadataIndex.MetadataIndexMetadata;

/**
 * The extension semantics for the {@link IMetadataIndex} are implemented by
 * this class.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MetadataIndexView extends DelegateIndex implements IMetadataIndex {

    private final AbstractBTree delegate;
    
    public MetadataIndexView(AbstractBTree delegate) {
        
        super(delegate);
    
        this.delegate = delegate;
        
    }
    
    public IndexMetadata getScaleOutIndexMetadata() {

        return ((MetadataIndexMetadata)getIndexMetadata()).getManagedIndexMetadata();
        
    }

    public PartitionLocator get(byte[] key) {

        return (PartitionLocator) SerializerUtil.deserialize(lookup(key));

    }

    /**
     * Note: It is this method which introduces the requirement for the
     * {@link ILinearList} API for the {@link MetadataIndex}. The method is
     * used to discover the locator for the index partition within which the
     * <i>key</i> would be found.
     */
    public PartitionLocator find(byte[] key) {
        
        final int index;
        
        if (key == null) {
            
            // use the index of the last partition.
            index = delegate.getEntryCount() - 1;
            
        } else {

            // locate the index partition for that key.
            index = findIndexOf(key);
            
        }
        
        if(index == -1) {
            
            return null;
            
        }
        
        final byte[] val = delegate.valueAt(index);
        
        return (PartitionLocator) SerializerUtil.deserialize(val);
        
    }
    
    /**
     * Find the index of the partition spanning the given key.
     * 
     * @return The index of the partition spanning the given key or
     *         <code>-1</code> iff there are no partitions defined.
     * 
     * @exception IllegalStateException
     *                if there are partitions defined but no partition spans the
     *                key. In this case the {@link MetadataIndex} lacks an entry
     *                for the key <code>new byte[]{}</code>.
     */
    private int findIndexOf(byte[] key) {
        
        int pos = delegate.indexOf(key);
        
        if (pos < 0) {

            /*
             * the key lies between the partition separators and represents the
             * insert position.  we convert it to an index and subtract one to
             * get the index of the partition that spans this key.
             */
            
            pos = -(pos+1);

            if(pos == 0) {

                if(delegate.getEntryCount() != 0) {
                
                    throw new IllegalStateException(
                            "Partition not defined for empty key.");
                    
                }
                
                return -1;
                
            }
                
            pos--;

            return pos;
            
        } else {
            
            /*
             * exact hit on a partition separator, so we choose the entry with
             * that key.
             */
            
            return pos;
            
        }

    }
    
}
