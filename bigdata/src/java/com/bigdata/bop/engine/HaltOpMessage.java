package com.bigdata.bop.engine;

import java.io.Serializable;
import java.util.UUID;

import com.bigdata.bop.BOp;

/**
 * A message sent to the {@link IQueryClient} when an operator is done executing
 * for some chunk of inputs.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class HaltOpMessage implements Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /** The identifier of the query. */
    final public UUID queryId;

    /** The identifier of the operator. */
    final public int bopId;

    /**
     * The index partition identifier against which the operator was executing.
     */
    final public int partitionId;

    /**
     * The identifier of the service on which the operator was executing.
     */
    final public UUID serviceId;

    /**
     * * The cause and <code>null</code> if the operator halted normally.
     */
    final public Throwable cause;

    /**
     * The operator identifier for the primary sink -or- <code>null</code> if
     * there is no primary sink (for example, if this is the last operator in
     * the pipeline).
     */
    final public Integer sinkId;

    /**
     * The number of the {@link IChunkMessage}s that were output for the primary
     * sink. (This information is used for the atomic termination decision.)
     * <p>
     * For a given downstream operator this is ONE (1) for scale-up. For
     * scale-out, this is one per index partition over which the intermediate
     * results were mapped.
     */
    final public int sinkMessagesOut;

    /**
     * The operator identifier for the alternative sink -or- <code>null</code>
     * if there is no alternative sink.
     */
    final public Integer altSinkId;

    /**
     * The number of the {@link IChunkMessage}s that were output for the
     * alternative sink. (This information is used for the atomic termination
     * decision.)
     * <p>
     * For a given downstream operator this is ONE (1) for scale-up. For
     * scale-out, this is one per index partition over which the intermediate
     * results were mapped. It is zero if there was no alternative sink for the
     * operator.
     */
    final public int altSinkMessagesOut;
    
    /**
     * The statistics for the execution of the bop against the partition on the
     * service.
     */
    final public BOpStats taskStats;

    /**
     * @param queryId
     *            The query identifier.
     * @param bopId
     *            The operator whose execution phase has terminated for a
     *            specific index partition and input chunk.
     * @param partitionId
     *            The index partition against which the operator was executed.
     * @param serviceId
     *            The node which executed the operator.
     * @param cause
     *            <code>null</code> unless execution halted abnormally.
     * @param sinkId
     *            The {@link BOp.Annotations#BOP_ID} of the default sink and
     *            <code>null</code> if there is no sink (for example, if this is
     *            the last operator in the pipeline).
     * @param sinkMessagesOut
     *            The number of {@link IChunkMessage} which were sent to the
     *            operator for the default sink.
     * @param altSinkId
     *            The {@link BOp.Annotations#BOP_ID} of the alternative sink and
     *            <code>null</code> if there is no alternative sink.
     * @param altSinkMessagesOut
     *            The number of {@link IChunkMessage} which were sent to the
     *            operator for the alternative sink.
     * @param taskStats
     *            The statistics for the execution of that bop on that shard and
     *            service.
     */
    public HaltOpMessage(
            //
            final UUID queryId, final int bopId, final int partitionId,
            final UUID serviceId, Throwable cause, //
            final Integer sinkId, final int sinkMessagesOut,// 
            final Integer altSinkId, final int altSinkMessagesOut,// 
            final BOpStats taskStats) {

        this.queryId = queryId;
        this.bopId = bopId;
        this.partitionId = partitionId;
        this.serviceId = serviceId;
        this.cause = cause;
        this.sinkId = sinkId;
        this.sinkMessagesOut = sinkMessagesOut;
        this.altSinkId = altSinkId;
        this.altSinkMessagesOut = altSinkMessagesOut;
        this.taskStats = taskStats;
    }

    public String toString() {
        final StringBuilder sb = new StringBuilder(getClass().getName());
        sb.append("{queryId=" + queryId);
        sb.append(",bopId=" + bopId);
        sb.append(",partitionId=" + partitionId);
        sb.append(",serviceId=" + serviceId);
        if (cause != null)
            sb.append(",cause=" + cause);
        sb.append(",sinkId=" + sinkId);
        sb.append(",sinkChunksOut=" + sinkMessagesOut);
        sb.append(",altSinkId=" + altSinkId);
        sb.append(",altSinkChunksOut=" + altSinkMessagesOut);
        sb.append(",stats=" + taskStats);
        sb.append("}");
        return sb.toString();
    }

}
