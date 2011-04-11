package com.bigdata.btree.data;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.raba.IRaba;
import com.bigdata.io.AbstractFixedByteArrayBuffer;
import com.bigdata.rawstore.IRawStore;

/**
 * Mock object for {@link ILeafData} used for unit tests.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MockLeafData extends AbstractMockNodeData implements ILeafData {

    final private IRaba vals;

    final private boolean[] deleteMarkers;

    final private long[] versionTimestamps;
    
    final private long minVersionTimestamp, maxVersionTimestamp;

    final private boolean[] rawRecords;

    final public IRaba getValues() {

        return vals;

    }

    final public byte[] getValue(final int index) {

        return vals.get(index);

    }

    final public int getSpannedTupleCount() {

        return vals.size();

    }

    final public int getValueCount() {

        return vals.size();

    }

    final public boolean isLeaf() {

        return true;

    }

    final public boolean isReadOnly() {
        
        return true;
        
    }
    
    /**
     * No.
     */
    final public boolean isCoded() {
        
        return false;
        
    }
    
    final public AbstractFixedByteArrayBuffer data() {
        
        throw new UnsupportedOperationException();
        
    }

    final public boolean getDeleteMarker(final int index) {

        if (deleteMarkers == null)
            throw new UnsupportedOperationException();

        return deleteMarkers[index];

    }

    final public long getVersionTimestamp(final int index) {

        if (versionTimestamps == null)
            throw new UnsupportedOperationException();

        return versionTimestamps[index];

    }

    final public long getRawRecord(final int index) {

        if (rawRecords == null)
            throw new UnsupportedOperationException();

        if(!rawRecords[index])
        	return IRawStore.NULL;
        
        final byte[] b = vals.get(index);
        
        final long addr = AbstractBTree.decodeRecordAddr(b);
  
        return addr;

    }

    final public boolean hasDeleteMarkers() {

        return deleteMarkers != null;

    }

    final public boolean hasVersionTimestamps() {

        return versionTimestamps != null;

    }

	public boolean hasRawRecords() {

		return rawRecords != null;
		
    }

//    public MockLeafData(final IRaba keys, final IRaba vals) {
//
//		this(keys, vals, null/* deleteMarkers */, null/* versionTimestamps */,
//				null/* rawRecords */);
//
//    }

    public MockLeafData(final IRaba keys, final IRaba vals,
            final boolean[] deleteMarkers, final long[] versionTimestamps,
            final boolean[] rawRecords) {

        super(keys);

        assert vals != null;
        assert !vals.isKeys();
        assert vals.size() == keys.size();
        assert vals.capacity() == keys.capacity();

        if (deleteMarkers != null)
            assert deleteMarkers.length == vals.capacity();
        
        if (versionTimestamps != null)
            assert versionTimestamps.length == vals.capacity();

        if (rawRecords != null)
            assert rawRecords.length == vals.capacity();

        this.vals = vals;

        this.deleteMarkers = deleteMarkers;

        this.rawRecords = rawRecords;
        
        this.versionTimestamps = versionTimestamps;

        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        
        if (versionTimestamps != null) {

            final int nkeys = keys.size();
            
            for (int i = 0; i < nkeys; i++) {

                final long t = versionTimestamps[i];

                if (t < min)
                    min = t;
                
                if (t > max)
                    max = t;
                
            }
            
        }
        
        this.minVersionTimestamp = min;
        
        this.maxVersionTimestamp = max;
       
    }

    public boolean isDoubleLinked() {
        return false;
    }

    public long getNextAddr() {
        throw new UnsupportedOperationException();
    }

    public long getPriorAddr() {
        throw new UnsupportedOperationException();
    }

    public long getMaximumVersionTimestamp() {
        
        if(!hasVersionTimestamps())
            throw new UnsupportedOperationException();

        return maxVersionTimestamp;
        
    }

    public long getMinimumVersionTimestamp() {

        if(!hasVersionTimestamps())
            throw new UnsupportedOperationException();

        return minVersionTimestamp;
        
    }

    public String toString() {
    
        final StringBuilder sb = new StringBuilder();
        
        sb.append(super.toString());
        
        sb.append("{");
        
        DefaultLeafCoder.toString(this, sb);
        
        sb.append("}");
        
        return sb.toString();
        
    }

}
