package com.bigdata.resources;

import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.service.Split;

/**
 * The result of a {@link SplitIndexPartitionTask} including enough metadata
 * to identify the index partitions to be created and the index partition to
 * be deleted.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SplitResult extends AbstractResult {

    /**
     * The array of {@link Split}s that describes the new key range for
     * each new index partition created by splitting the old index
     * partition.
     */
    public final Split[] splits;
    
    /**
     * An array of the {@link BuildResult}s for each output split.
     */
    public final BuildResult[] buildResults;

    /**
     * @param name
     *            The name under which the processed index partition was
     *            registered (this is typically different from the name of
     *            the scale-out index).
     * @param indexMetadata
     *            The index metadata object for the processed index as of
     *            the timestamp of the view from which the
     *            {@link IndexSegment} was generated.
     * @param splits
     *            Note: At this point we have the history as of the
     *            lastCommitTime in N index segments. Also, since we
     *            constain the resource manager to refuse another overflow
     *            until we have handle the old journal, all new writes are
     *            on the live index.
     * @param buildResults
     *            A {@link BuildResult} for each output split.
     */
    public SplitResult(String name, IndexMetadata indexMetadata,
            Split[] splits, BuildResult[] buildResults) {

        super( name, indexMetadata);

        assert splits != null;
        
        assert buildResults != null;
        
        assert splits.length == buildResults.length;
        
        for(int i=0; i<splits.length; i++) {
            
            assert splits[i] != null;

            assert splits[i].pmd != null;

            assert splits[i].pmd instanceof LocalPartitionMetadata;

            assert buildResults[i] != null;
            
        }
        
        this.splits = splits;
        
        this.buildResults = buildResults;

    }

}