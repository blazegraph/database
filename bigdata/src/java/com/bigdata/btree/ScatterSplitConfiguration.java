package com.bigdata.btree;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.CognitiveWeb.extser.LongPacker;

import com.bigdata.btree.IndexMetadata.Options;

/**
 * Configuration object for scatter split behavior for a scale-out index.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ScatterSplitConfiguration implements Externalizable {
    
    /**
     * 
     */
    private static final long serialVersionUID = -1383416502823794570L;

    /**
     * @see Options#SCATTER_SPLIT_ENABLED
     */
    private boolean enabled;

    /**
     * @see Options#SCATTER_SPLIT_PERCENT_OF_SPLIT_THRESHOLD
     */
    private double percentOfSplitThreshold;

    /**
     * @see Options#SCATTER_SPLIT_DATA_SERVICE_COUNT
     */
    private int dataServiceCount;

    /**
     * @see Options#SCATTER_SPLIT_INDEX_PARTITION_COUNT
     */
    private int indexPartitionCount;

    /**
     * @see Options#SCATTER_SPLIT_ENABLED
     */
    public boolean isEnabled() {
        
        return enabled;
        
    }

    /**
     * @see Options#SCATTER_SPLIT_DATA_SERVICE_COUNT
     */
    public int getDataServiceCount() {
        
        return dataServiceCount;
        
    }

    /**
     * @see Options#SCATTER_SPLIT_INDEX_PARTITION_COUNT
     */
    public int getIndexPartitionCount() {
       
        return indexPartitionCount;
        
    }

    /**
     * @see Options#SCATTER_SPLIT_PERCENT_OF_SPLIT_THRESHOLD
     */
    public double getPercentOfSplitThreshold() {

        return percentOfSplitThreshold;
        
    }

    public String toString() {
        
        final StringBuilder sb = new StringBuilder(getClass().getName());
        
        sb.append("{enabled="+enabled);
        
        sb.append(", percentOfSplitThreshold="+percentOfSplitThreshold);
        
        sb.append(", dataServiceCount="+dataServiceCount);
        
        sb.append(", indexPartitionCount="+indexPartitionCount);

        sb.append("}");
       
        return sb.toString();
        
    }

    /**
     * De-serialization ctor.
     */
    public ScatterSplitConfiguration() {
        
    }

    /**
     * Core impl.
     * 
     * @param enabled
     * @param percentOfSplitThreshold
     * @param dataServiceCount
     * @param indexPartitionCount
     */
    public ScatterSplitConfiguration(final boolean enabled,
            final double percentOfSplitThreshold, final int dataServiceCount,
            final int indexPartitionCount) {

        if (percentOfSplitThreshold < 0.1 || percentOfSplitThreshold > 1.0) {

            throw new IllegalArgumentException(
                    Options.SCATTER_SPLIT_PERCENT_OF_SPLIT_THRESHOLD
                            + " must be in [0.1:1.0]");

        }

        if (dataServiceCount < 0) {

            throw new IllegalArgumentException(
                    Options.SCATTER_SPLIT_DATA_SERVICE_COUNT
                            + " must be non-negative");

        }

        if (indexPartitionCount < 0) {

            throw new IllegalArgumentException(
                    Options.SCATTER_SPLIT_INDEX_PARTITION_COUNT
                            + " must be non-negative");

        }

        this.enabled = enabled;
        this.percentOfSplitThreshold = percentOfSplitThreshold;
        this.dataServiceCount = dataServiceCount;
        this.indexPartitionCount = indexPartitionCount;

    }

    private static final transient int VERSION0 = 0x0;

    public void readExternal(final ObjectInput in) throws IOException,
            ClassNotFoundException {

        final int version = (int) LongPacker.unpackLong(in);

        if (version != VERSION0)
            throw new IOException("Unknown version: " + version);

        this.enabled = in.readBoolean();
        this.percentOfSplitThreshold = in.readDouble();
        this.dataServiceCount = (int) LongPacker.unpackLong(in);
        this.indexPartitionCount = (int) LongPacker.unpackLong(in);

    }

    public void writeExternal(final ObjectOutput out) throws IOException {

        LongPacker.packLong(out, VERSION0);

        out.writeBoolean(enabled);
        out.writeDouble(percentOfSplitThreshold);
        LongPacker.packLong(out, dataServiceCount);
        LongPacker.packLong(out, indexPartitionCount);

    }

}
