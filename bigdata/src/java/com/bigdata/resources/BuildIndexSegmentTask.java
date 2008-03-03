package com.bigdata.resources;

import java.io.File;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexSegment;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.IResourceManager;

/**
 * Task builds an {@link IndexSegment} from the fused view of an index partition
 * as of some historical timestamp. This task is typically applied after an
 * {@link IResourceManager#overflow(boolean, boolean)} in order to produce a
 * compact view of the index as of the lastCommitTime on the old journal. Note
 * that the task by itself does not update the definition of the live index,
 * merely builds an {@link IndexSegment}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BuildIndexSegmentTask extends AbstractTask {

    /**
     * 
     */
    private final ResourceManager resourceManager;

    final protected long lastCommitTime;
    
    final protected File outFile;

    final protected byte[] fromKey;

    final protected byte[] toKey;

    /**
     * 
     * @param concurrencyManager
     * @param lastCommitTime
     *            The lastCommitTime of the journal whose view of the index
     *            you wish to capture in the generated {@link IndexSegment}.
     * @param name
     *            The name of the index.
     * @param outFile
     *            The file on which the {@link IndexSegment} will be
     *            written.
     */
    public BuildIndexSegmentTask(ResourceManager resourceManager,
            IConcurrencyManager concurrencyManager, long lastCommitTime,
            String name, File outFile) {

        this(resourceManager, concurrencyManager, lastCommitTime, name, outFile,
                null/* fromKey */, null/* toKey */);

    }

    /**
     * 
     * @param concurrencyManager
     * @param lastCommitTime
     *            The lastCommitTime of the journal whose view of the index
     *            you wish to capture in the generated {@link IndexSegment}.
     * @param name
     *            The name of the index.
     * @param outFile
     *            The file on which the {@link IndexSegment} will be
     *            written.
     * @param fromKey
     *            The lowest key that will be counted (inclusive). When
     *            <code>null</code> there is no lower bound.
     * @param toKey
     *            The first key that will not be counted (exclusive). When
     *            <code>null</code> there is no upper bound.
     */
    public BuildIndexSegmentTask(ResourceManager resourceManager, IConcurrencyManager concurrencyManager,
            long lastCommitTime, String name, File outFile, byte[] fromKey, byte[] toKey) {

        super(concurrencyManager, -lastCommitTime/*historical read*/, name);

        if (resourceManager == null)
            throw new IllegalArgumentException();

        this.resourceManager = resourceManager;

        this.lastCommitTime = lastCommitTime;
        
        if (outFile == null)
            throw new IllegalArgumentException();

        this.outFile = outFile;
        
        this.fromKey = fromKey;
         
        this.toKey = toKey;

    }

    /**
     * Build an {@link IndexSegment} from an index partition.
     * 
     * @return The {@link BuildResult}.
     */
    public Object doTask() throws Exception {

        // the name under which the index partition is registered.
        final String name = getOnlyResource();
        
        // The source view.
        final IIndex src = getIndex( name );
        
        /*
         * Build the index segment.
         */
        
        return resourceManager.buildIndexSegment(name, src, outFile,
                lastCommitTime, fromKey, toKey);

    }

 }