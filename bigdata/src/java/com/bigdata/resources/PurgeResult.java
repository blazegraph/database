package com.bigdata.resources;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import com.bigdata.journal.CommitRecordIndex;
import com.bigdata.journal.ITransactionService;
import com.bigdata.service.Params;

/**
 * A class that captures the results of
 * {@link StoreManager#purgeOldResources()}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class PurgeResult implements Serializable, Params {

    /**
     * 
     */
    private static final long serialVersionUID = -8729447753321005132L;

    /**
     * The earliest commit time on any of the managed journals.
     */
    final public long firstCommitTime;
    
    /**
     * Last commit time on the live journal at the start of the operation.
     */
    final public long lastCommitTime;

    /**
     * The release time as reported by the {@link ITransactionService}.
     */
    final public long givenReleaseTime;

    /**
     * The earliest timestamp that MUST be retained for the read-historical
     * indices in the cache and {@link Long#MAX_VALUE} if there are NO
     * read-historical indices in the cache.
     */
    final public long indexRetentionTime;

    /**
     * The chosen release time.
     */
    final public long choosenReleaseTime;
    
    /**
     * The earliest commit time that was preserved by the purge operation
     * (you can still read on this commit point after the purge).
     */
    final public long commitTimeToPreserve;
    
    /**
     * The #of resources (journals and index segments) that are dependencies
     * for index views from the {@link #commitTimeToPreserve} until the
     * {@link #lastCommitTime}.
     */
    final public int resourcesInUseCount;

    /**
     * The #of journals before the purge.
     */
    final public int journalBeforeCount;

    /**
     * The #of journals after the purge.
     */
    final public int journalAfterCount;

    /**
     * The #of index segments before the purge.
     */
    final public int segmentBeforeCount;

    /**
     * The #of index segments after the purge.
     */
    final public int segmentAfterCount;
    
    /**
     * The #of bytes under management before the purge.
     */
    final public long bytesBeforeCount;
    
    /**
     * The #of bytes under management after the purge.
     */
    final public long bytesAfterCount;

    /**
     * Elapsed time in milliseconds required to scan the
     * {@link CommitRecordIndex} for each historical journal in order to
     * identify the set of resources "in use" resources.
     */
    final long elapsedScanCommitIndicesTime;

    /**
     * Elapsed time in milliseconds required to delete the resources which have
     * been identified as being no longer required.
     */
    final long elapsedDeleteResourcesTime;

    /**
     * Total elapsed time in milliseconds for the purge resources operation.
     * This includes both the {@link #elapsedScanCommitIndicesTime} and the
     * {@link #elapsedDeleteResourcesTime}.
     */
    final long elapsedPurgeResourcesTime;

    public PurgeResult(//
            final long firstCommitTime,
            final long lastCommitTime,
            final long givenReleaseTime,
            final long indexRetentionTime,
            final long choosenReleaseTime,
            final long commitTimeToPreserve,
            final int resourcesInUseCount,
            final int journalBeforeCount,
            final int journalAfterCount,
            final int segmentBeforeCount,
            final int segmentAfterCount,
            final long bytesBeforeCount,
            final long bytesAfterCount,
            final long elapsedScanCommitIndicesTime,
            final long elapsedDeleteResourcesTime,
            final long elapsedPurgeResourcesTime
            ) {

        this.firstCommitTime = firstCommitTime;
        this.lastCommitTime = lastCommitTime;
        this.givenReleaseTime = givenReleaseTime;
        this.indexRetentionTime = indexRetentionTime;
        this.choosenReleaseTime = choosenReleaseTime;
        this.commitTimeToPreserve = commitTimeToPreserve;
        this.resourcesInUseCount = resourcesInUseCount;
        this.journalBeforeCount = journalBeforeCount;
        this.journalAfterCount = journalAfterCount;
        this.segmentBeforeCount = segmentBeforeCount;
        this.segmentAfterCount = segmentAfterCount;
        this.bytesBeforeCount = bytesBeforeCount;
        this.bytesAfterCount = bytesAfterCount;
        this.elapsedScanCommitIndicesTime = elapsedScanCommitIndicesTime;
        this.elapsedDeleteResourcesTime = elapsedDeleteResourcesTime;
        this.elapsedPurgeResourcesTime = elapsedPurgeResourcesTime;
        
    }
    
    public String toString() {

        final StringBuilder sb = new StringBuilder();
        
        sb.append(getClass().getSimpleName());
        
        sb.append("{firstCommitTime=" + firstCommitTime);
        sb.append(",lastCommitTime=" + lastCommitTime);
        sb.append(",givenReleaseTime=" + givenReleaseTime);
        sb.append(",indexRetentionTime=" + indexRetentionTime);
        sb.append(",choosenReleaseTime=" + choosenReleaseTime);
        sb.append(",commitTimeToPreserve=" + commitTimeToPreserve);
        sb.append(",resourcesInUseCount=" + resourcesInUseCount);
        sb.append(",journalBeforeCount=" + journalBeforeCount);
        sb.append(",journalAfterCount=" + journalAfterCount);
        sb.append(",segmentBeforeCount=" + segmentBeforeCount);
        sb.append(",segmentAfterCount=" + segmentAfterCount);
        sb.append(",bytesBeforeCount=" + bytesBeforeCount);
        sb.append(",bytesAfterCount=" + bytesAfterCount);
        sb.append(",elapsedScanCommitIndicesTime=" + elapsedScanCommitIndicesTime);
        sb.append(",elapsedDeleteResourcesTime=" + elapsedDeleteResourcesTime);
        sb.append(",elapsedPurgeResourcesTime=" + elapsedPurgeResourcesTime);
        sb.append("}");

        return sb.toString();
            
    }

    public Map<String,Object> getParams() {

        final Map<String, Object> v = new HashMap<String, Object>();

        v.put("firstCommitTime", firstCommitTime);
        v.put("lastCommitTime", lastCommitTime);
        v.put("givenReleaseTime", givenReleaseTime);
        v.put("indexRetentionTime", indexRetentionTime);
        v.put("choosenReleaseTime", choosenReleaseTime);
        v.put("commitTimeToPreserve", commitTimeToPreserve);
        v.put("resourcesInUseCount", resourcesInUseCount);
        v.put("journalBeforeCount", journalBeforeCount);
        v.put("journalAfterCount", journalAfterCount);
        v.put("segmentBeforeCount", segmentBeforeCount);
        v.put("segmentAfterCount", segmentAfterCount);
        v.put("bytesBeforeCount", bytesBeforeCount);
        v.put("bytesAfterCount", bytesAfterCount);
        v.put("elapsedScanCommitIndicesTime", elapsedScanCommitIndicesTime);
        v.put("elapsedDeleteResourcesTime", elapsedDeleteResourcesTime);
        v.put("elapsedPurgeResourcesTime", elapsedPurgeResourcesTime);

        return v;

    }

}
