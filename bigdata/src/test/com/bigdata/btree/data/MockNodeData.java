package com.bigdata.btree.data;

import com.bigdata.btree.INodeData;
import com.bigdata.btree.raba.IRaba;

/**
 * Mock object for {@link INodeData}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
class MockNodeData extends AbstractMockNodeData implements INodeData {

    /**
     * The #of tuples spanned by this node.
     */
    private int spannedTupleCount;

    private final long[] childAddr;

    /**
     * The #of tuples spanned by each child of this node.
     */
    private final int[] childEntryCount;

    final public int getSpannedTupleCount() {

        return spannedTupleCount;

    }

//    final public int[] getChildEntryCounts() {
//
//        return childEntryCount;
//
//    }

    final public long[] getChildAddr() {

        return childAddr;

    }
    
    final public long getChildAddr(final int index) {

        if (index < 0 || index > getKeys().size() + 1)
            throw new IndexOutOfBoundsException();
        
        return childAddr[index];
        
    }

    final public int getChildEntryCount(final int index) {

        if (index < 0 || index > getKeys().size() + 1)
            throw new IndexOutOfBoundsException();
        
        return childEntryCount[index];
        
    }

    final public int getChildCount() {

        return getKeyCount() + 1;

    }

    final public boolean isLeaf() {

        return false;

    }

    public MockNodeData(final IRaba keys,
            final int spannedTupleCount, final long[] childAddr,
            final int[] childEntryCount) {

        super(keys);

        assert spannedTupleCount >= keys.size();

        assert childAddr != null;
        
        assert childEntryCount != null;
        
        assert keys.capacity() == childAddr.length;
        
        assert keys.capacity() == childEntryCount.length;

        this.spannedTupleCount = spannedTupleCount;

        this.childAddr = childAddr;

        this.childEntryCount = childEntryCount;

    }

}
