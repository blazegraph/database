/**

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
package com.bigdata.btree;

import com.bigdata.btree.data.IAbstractNodeData;
import com.bigdata.rawstore.IRawStore;

/**
 * Class reports various summary statistics for nodes and leaves.
 */
abstract public class PageStats {

    /** Number of nodes/leaves visited so far. */
    public long nvisited;
    /** The type of index. */
    public IndexTypeEnum indexType;
    /**
     * The name associated with the index -or- <code>null</code> if the index is
     * not named.
     */
    public String name;
    /**
     * The current branching factor for the index.
     * 
     * TODO GIST: [m] is BTree specific. The [addressBits] concept is the
     * parallel for the HTree. This field should probably be moved into the
     * concrete instances of the {@link PageStats} class.
     */
    public int m;
    /** The #of entries in the index. */
    public long ntuples;
    /** The height (aka depth) of the index */
    public int height;
    /** The #of nodes visited. */
    public long nnodes;
    /** The #of leaves visited. */
    public long nleaves;
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
    /**
     * This map is used to report the histogram of pages based on the actual
     * byte count of the user data in the allocation when the backing slot size
     * is not directly available. Allocations
     */
    public static final int[] SLOT_SIZES = new int[] { 64, 128, 192, 320, 512,
            768, 1024, 2048, 3072, 4096, 8192 };

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
        return nodeBytes + leafBytes;
    }

    /** The average bytes per node. */
    public long getBytesPerNode() {
        return (nnodes == 0 ? 0 : nodeBytes / nnodes);
    }

    /** The average bytes per leaf. */
    public long getBytesPerLeaf() {
        return (nleaves == 0 ? 0 : leafBytes / nleaves);
    }

    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append(getClass().getName());
        sb.append("{indexType=" + indexType);
        sb.append(",m=" + m);
        sb.append(",nnodes=" + nnodes);
        sb.append(",nleaves=" + nleaves);
        sb.append(",nodeBytes=" + nodeBytes);
        sb.append(",minNodeBytes=" + minNodeBytes);
        sb.append(",maxNodeBytes=" + maxNodeBytes);
        sb.append(",leafBytes=" + leafBytes);
        sb.append(",minLeafBytes=" + minLeafBytes);
        sb.append(",maxLeafBytes=" + maxLeafBytes);
        sb.append(",bytesPerNode=" + getBytesPerNode());
        sb.append(",bytesPerLeaf=" + getBytesPerLeaf());
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
    public static String getHeaderRow() {

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
        sb.append("nodeBytes");
        sb.append('\t');
        sb.append("leafBytes");
        sb.append('\t');
        sb.append("totalBytes");
        sb.append('\t');
        sb.append("avgNodeBytes");
        sb.append('\t');
        sb.append("avgLeafBytes");
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
        sb.append(stats.nodeBytes);
        sb.append('\t');
        sb.append(stats.leafBytes);
        sb.append('\t');
        sb.append(stats.getTotalBytes());
        sb.append('\t');
        sb.append(stats.getBytesPerNode());
        sb.append('\t');
        sb.append(stats.getBytesPerLeaf());
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