package com.bigdata.btree.data;

import com.bigdata.btree.raba.IRaba;

/**
 * Mock object for {@link ILeafData} used for unit tests.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
class MockLeafData extends AbstractMockNodeData implements ILeafData {

    final private IRaba vals;

    final private boolean[] deleteMarkers;

    final private long[] versionTimestamps;

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

    final public boolean hasDeleteMarkers() {

        return deleteMarkers != null;

    }

    final public boolean hasVersionTimestamps() {

        return versionTimestamps != null;

    }

    public MockLeafData(final IRaba keys, final IRaba vals) {

        this(keys, vals, null/* deleteMarkers */, null/* versionTimestamps */);

    }

    public MockLeafData(final IRaba keys, final IRaba vals,
            final boolean[] deleteMarkers, final long[] versionTimestamps) {

        super(keys);

        assert vals != null;
        assert !vals.isKeys();
        assert vals.size() == keys.size();
        assert vals.capacity() == keys.capacity();

        if (deleteMarkers != null)
            assert deleteMarkers.length == vals.capacity();
        
        if (versionTimestamps != null)
            assert versionTimestamps.length == vals.capacity();

        this.vals = vals;

        this.deleteMarkers = deleteMarkers;

        this.versionTimestamps = versionTimestamps;

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

}
