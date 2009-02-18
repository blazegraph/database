package com.bigdata.resources;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.BTree;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.IndexSegmentBuilder;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.SegmentMetadata;

/**
 * The result of an {@link CompactingMergeTask}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BuildResult extends AbstractResult {

    /**
     * The #of sources in the view from which the {@link IndexSegment} was
     * built.
     */
    public final int sourceCount;
 
    /**
     * The sources in the view from which the {@link IndexSegment} was built.
     * <p>
     * Note: Builds may include anything from the mutable {@link BTree} on the
     * old journal to the full view of the index partition. They MAY be
     * comprised of only a subset of the full view as long as the subset is
     * formed from the 1st N sources in the full view and is taken in the same
     * order as the full view. As a degenerate case, the subset may include only
     * the data from the mutable {@link BTree} on the old journal. However, the
     * subset can also include additional sources that were merged together in
     * order to generate the new {@link IndexSegment}.
     */
    public IResourceMetadata[] sources;
    
    /**
     * <code>true</code> iff the build operation was a compacting merge of the
     * entire index partition view.
     * <p>
     * Note: A compacting merge is ONLY permitted when the entire view is
     * processed. This is necessary in order for the index partition to remain
     * consistent. A compacting merge assumes a closed world (that is, it
     * assumes that there are no deleted tuples which are not present in the set
     * of sources which it processed). A non-merge build DOES NOT make this
     * assumption and therefore WILL NOT discard deleted tuples which unless
     * they have been overwritten in more recent sources in the view.
     */
    public boolean compactingMerge;
    
    /**
     * The metadata describing the generated {@link IndexSegment}.
     */
    public final SegmentMetadata segmentMetadata;

    /**
     * The object which built the {@link IndexSegment} and which contains more
     * interesting information about the build.
     */
    public final IndexSegmentBuilder builder;

    /**
     * 
     * @param name
     *            The name under which the processed index partition was
     *            registered (this is typically different from the name of the
     *            scale-out index).
     * @param indexMetadata
     *            The index metadata object for the processed index as of the
     *            timestamp of the view from which the {@link IndexSegment} was
     *            generated.
     * @param segmentMetadata
     *            The metadata describing the generated {@link IndexSegment}.
     * @param builder
     *            Contains more interesting information about the build.
     */
    public BuildResult(final String name, final boolean compactingMerge,
            final AbstractBTree[] sources,
            final IndexMetadata indexMetadata,
            final SegmentMetadata segmentMetadata,
            final IndexSegmentBuilder builder) {

        super(name, indexMetadata);
        
        if (sources == null) {

            throw new IllegalArgumentException();
            
        }

        if (sources.length == 0) {

            throw new IllegalArgumentException();
            
        }

        for(AbstractBTree src : sources) {
            
            if (src == null)
                throw new IllegalArgumentException();
            
        }
        
        if (segmentMetadata == null) {

            throw new IllegalArgumentException();
            
        }

        if (builder == null) {

            throw new IllegalArgumentException();
            
        }

        this.sourceCount = sources.length;

        this.compactingMerge = compactingMerge;

        this.sources = new IResourceMetadata[sourceCount];

        for (int i = 0; i < sourceCount; i++) {

            this.sources[i] = sources[i].getStore().getResourceMetadata();

        }
        
        this.segmentMetadata = segmentMetadata;
        
        this.builder = builder;

    }

    public String toString() {
        
        return "BuildResult{name=" + name + ", #sources=" + sourceCount
                + ", merge=" + compactingMerge + ", #tuples(out)="
                + builder.getCheckpoint().nentries + "}";

    }

}
