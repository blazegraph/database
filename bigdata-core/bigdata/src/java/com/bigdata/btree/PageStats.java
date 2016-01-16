/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
package com.bigdata.btree;

import com.bigdata.btree.data.IAbstractNodeData;
import com.bigdata.btree.data.ILeafData;
import com.bigdata.rawstore.IRawStore;

/**
 * Class reports various summary statistics for nodes and leaves.
 */
abstract public class PageStats extends BaseIndexStats {

    /** Number of nodes/leaves visited so far. */
    public long nvisited;
    /** The #of bytes in the raw records for the nodes visited. */
    public long nodeBytes;
    /** The #of bytes in the raw records for the leaves visited. */
    public long leafBytes;
    /** The min/max bytes per node. */
    public long minNodeBytes, maxNodeBytes;
    /** The min/max bytes per leaf. */
    public long minLeafBytes, maxLeafBytes;
    /**
     * Histogram of the allocation slot sizes based on {@link #SLOT_SIZES}. The
     * indices into this array are correlated with the indices into the
     * {@link #SLOT_SIZES} array. If the allocation is larger than the maximum
     * value in {@link #SLOT_SIZES}, then it is recorded in {@link #blobs}
     * instead.
     */
    public final long[] histogram;
    /**
     * The #of allocations that are larger than the maximum slot size in
     * {@link #SLOT_SIZES}.
     */
    public long blobs;
    /** The #of errors encountered during traversal. */
    public long nerrors;
    /**
     * This map is used to report the histogram of pages based on the actual
     * byte count of the user data in the allocation when the backing slot size
     * is not directly available. Allocations
     */
    public static final int[] SLOT_SIZES = new int[] { 64, 128, 192, 320, 512,
            768, 1024, 2048, 3072, 4096, 8192 };
    /**
     * The number of raw record allocations and the byte size of those raw
     * record allocations.
     * 
     * TODO We could also use a histogram over this information (raw records
     * sizes).
     */
    public long nrawRecs = 0, rawRecBytes;

    public PageStats() {

        histogram = new long[SLOT_SIZES.length];

    }

    /**
     * Track the histogram of allocation sizes.
     * 
     * @param allocationSize
     *            The size of some allocation.
     * 
     * @see #histogram
     * @see #blobs
     * @see #SLOT_SIZES
     */
    protected void trackSlotSize(final long allocationSize) {

        for (int i = 0; i < SLOT_SIZES.length; i++) {

            if (allocationSize <= SLOT_SIZES[i]) {

                histogram[i]++;

                return;

            }

        }

        blobs++;

    }

    /** Return {@link #nodeBytes} plus {@link #leafBytes}. */
    public long getTotalBytes() {
        return nodeBytes + leafBytes + rawRecBytes;
    }

    /** The average bytes per node. */
    public long getBytesPerNode() {
        return (nnodes == 0 ? 0 : nodeBytes / nnodes);
    }

    /** The average bytes per leaf. */
    public long getBytesPerLeaf() {
        return (nleaves == 0 ? 0 : leafBytes / nleaves);
    }

    /** The average bytes per raw record. */
    public long getBytesPerRawRecord() {
        return (nrawRecs== 0 ? 0 : rawRecBytes / nrawRecs);
    }

    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append(getClass().getName());
        sb.append("{indexType=" + indexType);
        sb.append(",m=" + m);
        sb.append(",nnodes=" + nnodes);
        sb.append(",nleaves=" + nleaves);
        sb.append(",nrawRecs=" + nrawRecs);
        sb.append(",nodeBytes=" + nodeBytes);
        sb.append(",minNodeBytes=" + minNodeBytes);
        sb.append(",maxNodeBytes=" + maxNodeBytes);
        sb.append(",leafBytes=" + leafBytes);
        sb.append(",minLeafBytes=" + minLeafBytes);
        sb.append(",maxLeafBytes=" + maxLeafBytes);
        sb.append(",rawRecBytes=" + rawRecBytes);
        sb.append(",bytesPerNode=" + getBytesPerNode());
        sb.append(",bytesPerLeaf=" + getBytesPerLeaf());
        sb.append(",bytesPerRawRec=" + getBytesPerRawRecord());
        sb.append(",nerrors=" + nerrors);
        final long npages = (nleaves + nnodes);
        for (int i = 0; i < SLOT_SIZES.length; i++) {
            final long slotsThisSize = histogram[i];
            final double percentSlotSize = ((double) slotsThisSize) / npages;
            sb.append(",slot_" + SLOT_SIZES[i] + "=" + round(percentSlotSize));
        }
        {
            final double percentBlobs = ((double) blobs) / npages;
            sb.append(",blobs=" + round(percentBlobs));
        }
        sb.append(",newM=" + getRecommendedBranchingFactor());
        sb.append("}");
        return sb.toString();
    }
    
    /**
     * Round the value to 2 decimal points.
     */
    private static double round(final double d) {

        return ((int) (100 * d)) / 100d;

    }

    /**
     * Return the header row for a table.
     * 
     * @return The header row.
     */
    @Override
    public String getHeaderRow() {

        final StringBuilder sb = new StringBuilder();

        sb.append("name");
        sb.append('\t');
        sb.append("indexType");
        sb.append('\t');
        sb.append("m");
        sb.append('\t');
        sb.append("height");
        sb.append('\t');
        sb.append("nnodes");
        sb.append('\t');
        sb.append("nleaves");
        sb.append('\t');
        sb.append("nentries");
        sb.append('\t');
        sb.append("nrawRecs");
        sb.append('\t');
        sb.append("nerrors");
        sb.append('\t');
        sb.append("nodeBytes");
        sb.append('\t');
        sb.append("leafBytes");
        sb.append('\t');
        sb.append("rawRecBytes");
        sb.append('\t');
        sb.append("totalBytes");
        sb.append('\t');
        sb.append("avgNodeBytes");
        sb.append('\t');
        sb.append("avgLeafBytes");
        sb.append('\t');
        sb.append("avgRawRecBytes");
        sb.append('\t');
        sb.append("minNodeBytes");
        sb.append('\t');
        sb.append("maxNodeBytes");
        sb.append('\t');
        sb.append("minLeafBytes");
        sb.append('\t');
        sb.append("maxLeafBytes");
        
        // One column for each slot size in the histogram. The data are
        // written out as percentages.
        for (int i = 0; i < PageStats.SLOT_SIZES.length; i++) {
            sb.append('\t');
            sb.append(PageStats.SLOT_SIZES[i]);
        }
        
        // #of blob allocations.
        sb.append('\t');
        sb.append("blobs");

        // Recommended branching factor for the index.
        sb.append('\t');
        sb.append("newM");

        // Current branching factor.
        sb.append('\t');
        sb.append("curM");

        return sb.toString();
    }

    /**
     * Return a row of data for an index as aggregated by this {@link PageStats}
     * object.
     * 
     * @see #getHeaderRow()
     */
    @Override
    public String getDataRow() {

        final PageStats stats = this;

        final StringBuilder sb = new StringBuilder();

        sb.append(name);
        sb.append('\t');
        sb.append(indexType);
        sb.append('\t');
        sb.append(stats.m);
        sb.append('\t');
        sb.append(stats.height);
        sb.append('\t');
        sb.append(stats.nnodes);
        sb.append('\t');
        sb.append(stats.nleaves);
        sb.append('\t');
        sb.append(stats.ntuples);
        sb.append('\t');
        sb.append(stats.nrawRecs);
        sb.append('\t');
        sb.append(stats.nerrors);
        sb.append('\t');
        sb.append(stats.nodeBytes);
        sb.append('\t');
        sb.append(stats.leafBytes);
        sb.append('\t');
        sb.append(stats.rawRecBytes);
        sb.append('\t');
        sb.append(stats.getTotalBytes());
        sb.append('\t');
        sb.append(stats.getBytesPerNode());
        sb.append('\t');
        sb.append(stats.getBytesPerLeaf());
        sb.append('\t');
        sb.append(stats.getBytesPerRawRecord());
        sb.append('\t');
        sb.append(stats.minNodeBytes);
        sb.append('\t');
        sb.append(stats.maxNodeBytes);
        sb.append('\t');
        sb.append(stats.minLeafBytes);
        sb.append('\t');
        sb.append(stats.maxLeafBytes);
        
        final long npages = (stats.nleaves + stats.nnodes);

        for (int i = 0; i < PageStats.SLOT_SIZES.length; i++) {
            sb.append('\t');
            final long slotsThisSize = stats.histogram[i];
            final double percentSlotSize = ((double) slotsThisSize) / npages;
            sb.append(percentSlotSize);
        }
        
        sb.append('\t');
        sb.append(((double) stats.blobs) / npages);

        sb.append('\t');
        sb.append(stats.getRecommendedBranchingFactor());

        sb.append('\t');
        sb.append(stats.m);

        return sb.toString();
    }

    /**
     * This computes the recommended branching factor for the index based on an
     * examination of the current branching factor, an assumed nominal page size
     * of 8k, the min, max, and average node and leaf sizes, and the histogram
     * of the allocation sizes for the index.
     * 
     * @return
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/592">
     *      Optimize RWStore allocator sizes </a>
     * 
     *      TODO This says "branching factor", but {@link #m} is overloaded and
     *      is the branching factor for the BTree and the addressBits for the
     *      HTree. This method should be renamed (or moved into the concrete
     *      instances of this class).
     */
    abstract public int getRecommendedBranchingFactor();
    
    /**
     * Visit a node or leaf, updating the {@link PageStats}.
     * <p>
     * Note: This method MUST be extended to capture at least the initialization
     * of the {@link #ntuples}, {@link #nnodes}, {@link #nleaves}, and
     * {@link #m} fields.
     * 
     * @param ndx
     *            The index.
     * @param node
     *            A node or leaf in that index.
     */
    public void visit(final ISimpleTreeIndexAccess ndx,
            final IAbstractNodeData node) {

        final PageStats stats = this;

        if (stats.nvisited == 0) {

            stats.name = ((ICheckpointProtocol) ndx).getIndexMetadata()
                    .getName();

            stats.indexType = ((ICheckpointProtocol) ndx).getCheckpoint()
                    .getIndexType();
            
        }

        final IIdentityAccess po = (IIdentityAccess) node;

        if (po.isPersistent()) {

            /*
             * We can only report on storage for persistent nodes (those
             * associated with an address in a backing store). This test
             * works around a problem that would otherwise exist if you
             * attempt to collect the PageStats on an index that has not
             * been checkpointed.
             */
            
            final long addrSelf = po.getIdentity();

            final IRawStore store = ndx.getStore();

            final long nbytes = store.getByteCount(addrSelf);

            stats.trackSlotSize(nbytes);

            final boolean isLeaf = node.isLeaf();

            if (isLeaf) {

                stats.nleaves++;
                stats.leafBytes += nbytes;
                if (stats.minLeafBytes > nbytes || stats.minLeafBytes == 0)
                    stats.minLeafBytes = nbytes;
                if (stats.maxLeafBytes < nbytes)
                    stats.maxLeafBytes = nbytes;

                if (node instanceof ILeafData) {
                    final ILeafData data = (ILeafData) node;
                    if(data.hasRawRecords()) {
                        for (int i = 0; i < data.getKeys().size(); i++) {
                            final long rawAddr = data.getRawRecord(i);
                            if (rawAddr != IRawStore.NULL) {
                                stats.nrawRecs++;
                                stats.rawRecBytes += store
                                        .getByteCount(rawAddr);
                            }
                        }
                    }
                }

            } else {

                stats.nnodes++;
                stats.nodeBytes += nbytes;
                if (stats.minNodeBytes > nbytes || stats.minNodeBytes == 0)
                    stats.minNodeBytes = nbytes;
                if (stats.maxNodeBytes < nbytes)
                    stats.maxNodeBytes = nbytes;

            }

        } // if(isPersistent())

        stats.nvisited++;
        
    }

}
