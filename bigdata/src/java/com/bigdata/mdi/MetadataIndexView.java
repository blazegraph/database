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
    
    public int findIndexOf(byte[] key) {
        
        int pos = indexOf(key);
        
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
    
    public PartitionLocator find(byte[] key) {
        
        final int index = findIndexOf(key);
        
        if(index == -1) {
            
            return null;
            
        }
        
        final byte[] val = valueAt(index);
        
        return (PartitionLocator) SerializerUtil.deserialize(val);
        
    }
    
    public PartitionLocator get(byte[] key) {

        return (PartitionLocator) SerializerUtil.deserialize(lookup(key));

//        final byte[] val = lookup(key);
//
//        if (val == null) {
//
//            return null;
//            
//        }
//
//        return (PartitionMetadata) SerializerUtil.deserialize(val);
        
    }
    
    /**
     * Create or update a partition (exact match on the separator key).
     * 
     * @param key
     *            The separator key (the first key that would go into that
     *            partition).
     * @param val
     *            The parition metadata.
     * 
     * @return The previous partition metadata for that separator key or
     *         <code>null</code> if there was no partition for that separator
     *         key.
     * 
     * @exception IllegalArgumentException
     *                if the key identifies an existing partition but the
     *                partition identifers do not agree.
     */
    public PartitionLocator put(byte[] key, PartitionLocator val) {

        if (val == null) {

            throw new IllegalArgumentException();

        }
        
        final byte[] newval = SerializerUtil.serialize(val);
        
        final byte[] oldval2 = insert(key, newval);

        PartitionLocator oldval = oldval2 == null ? null
                : (PartitionLocator) SerializerUtil.deserialize(oldval2);
        
        if (oldval != null && oldval.getPartitionId() != val.getPartitionId()) {

            throw new IllegalArgumentException("Expecting: partId="
                    + oldval.getPartitionId() + ", but have partId="
                    + val.getPartitionId());

        }

        return oldval;
    
    }

    public IndexMetadata getScaleOutIndexMetadata() {

        return ((MetadataIndexMetadata)getIndexMetadata()).getManagedIndexMetadata();
        
    }

    public int[] findIndices(byte[] fromKey, byte[] toKey) {

        // index of the first partition to check.
        final int fromIndex = (fromKey == null ? 0 : findIndexOf(fromKey));

        // index of the last partition to check.
        final int toIndex = (toKey == null ? delegate.getEntryCount() - 1 : 
            findIndexOf(toKey));

        // keys are out of order.
        if (fromIndex > toIndex) {

            throw new IllegalArgumentException("fromKey > toKey");

        }

        return new int[] { fromIndex, toIndex };
        
    }


    public int indexOf(byte[] key) {

        return delegate.indexOf(key);
        
    }


    public byte[] keyAt(int index) {
        
        return delegate.keyAt(index);
        
    }


    public byte[] valueAt(int index) {

        return delegate.valueAt(index);
        
    }

}
