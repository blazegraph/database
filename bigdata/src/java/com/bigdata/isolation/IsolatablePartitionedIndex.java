/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Feb 12, 2007
 */

package com.bigdata.isolation;

import com.bigdata.objndx.IEntryIterator;
import com.bigdata.scaleup.MetadataIndex;
import com.bigdata.scaleup.PartitionedIndex;

/**
 * A partitioned index that supports transactions and deletion markers.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME support processing of delete markers. They can exist in the mutable
 * btree and in index segments that are not either a clean first eviction or a
 * full compacting merge (e.g., they can still exist in a compacting merge if
 * there are other index segments or btrees that are part of a partition but
 * are not partitipating in the compacting merge). 
 */
public class IsolatablePartitionedIndex extends PartitionedIndex implements IIsolatableIndex {

    /**
     * @param btree
     * @param mdi
     */
    public IsolatablePartitionedIndex(UnisolatedBTree btree, MetadataIndex mdi) {
        super(btree, mdi);
    }

    public boolean contains(byte[] key) {
        // TODO Auto-generated method stub
        return false;
    }

    public Object insert(Object key, Object value) {
        // TODO Auto-generated method stub
        return null;
    }

    public Object lookup(Object key) {
        // TODO Auto-generated method stub
        return null;
    }

    public int rangeCount(byte[] fromKey, byte[] toKey) {
        // TODO Auto-generated method stub
        return 0;
    }

    public IEntryIterator rangeIterator(byte[] fromKey, byte[] toKey) {
        // TODO Auto-generated method stub
        return null;
    }

    public Object remove(Object key) {
        // TODO Auto-generated method stub
        return null;
    }

    public void contains(int ntuples, byte[][] keys, boolean[] contains) {
        // TODO Auto-generated method stub

    }

    public void insert(int ntuples, byte[][] keys, Object[] values) {
        // TODO Auto-generated method stub

    }

    public void lookup(int ntuples, byte[][] keys, Object[] values) {
        // TODO Auto-generated method stub

    }

    public void remove(int ntuples, byte[][] keys, Object[] values) {
        // TODO Auto-generated method stub

    }

}
