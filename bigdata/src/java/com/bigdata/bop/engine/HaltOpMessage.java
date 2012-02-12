package com.bigdata.bop.engine;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;

import com.bigdata.io.LongPacker;

/**
 * A message sent to the {@link IQueryClient} when an operator is done executing
 * for some chunk of inputs.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class HaltOpMessage implements Externalizable, IHaltOpMessage {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private UUID queryId;
    private UUID serviceId;
    private int bopId;
    private int partitionId;
    private int sinkMessagesOut;
    private int altSinkMessagesOut;
    private BOpStats stats;
    private Throwable cause;

    /**
     * De-serialization constructor.
     */
    public HaltOpMessage() {
        
    }
    
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
     * @param sinkMessagesOut
     *            The number of {@link IChunkMessage} which were sent to the
     *            operator for the default sink.
     * @param altSinkMessagesOut
     *            The number of {@link IChunkMessage} which were sent to the
     *            operator for the alternative sink.
     * @param taskStats
     *            The statistics for the execution of that bop on that shard and
     *            service.
     */
//    * @param sinkId
//    *            The {@link BOp.Annotations#BOP_ID} of the default sink and
//    *            <code>null</code> if there is no sink (for example, if this is
//    *            the last operator in the pipeline).
//    * @param altSinkId
//    *            The {@link BOp.Annotations#BOP_ID} of the alternative sink and
//    *            <code>null</code> if there is no alternative sink.
    public HaltOpMessage(
            //
            final UUID queryId, final int bopId, final int partitionId,
            final UUID serviceId, Throwable cause, //
            final int sinkMessagesOut,// 
            final int altSinkMessagesOut,// 
            final BOpStats taskStats) {

        this.queryId = queryId;
        this.bopId = bopId;
        this.partitionId = partitionId;
        this.serviceId = serviceId;
        this.cause = cause;
        this.sinkMessagesOut = sinkMessagesOut;
        this.altSinkMessagesOut = altSinkMessagesOut;
        this.stats = taskStats;
    }

    public String toString() {
        final StringBuilder sb = new StringBuilder(getClass().getName());
        sb.append("{queryId=" + queryId);
        sb.append(",bopId=" + bopId);
        sb.append(",partitionId=" + partitionId);
        sb.append(",serviceId=" + serviceId);
        sb.append(",sinkChunksOut=" + sinkMessagesOut);
        sb.append(",altSinkChunksOut=" + altSinkMessagesOut);
        sb.append(",stats=" + stats);
        if (cause != null)
            sb.append(",cause=" + cause);
        sb.append("}");
        return sb.toString();
    }

    @Override
    public UUID getQueryId() {
        return queryId;
    }

    @Override
    public int getBOpId() {
        return bopId;
    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public UUID getServiceId() {
        return serviceId;
    }

    @Override
    public Throwable getCause() {
        return cause;
    }

    @Override
    public int getSinkMessagesOut() {
        return sinkMessagesOut;
    }

    @Override
    public int getAltSinkMessagesOut() {
        return altSinkMessagesOut;
    }

    @Override
    public BOpStats getStats() {
        return stats;
    }

    @Override
    public void writeExternal(final ObjectOutput out) throws IOException {
        out.writeLong(queryId.getMostSignificantBits());
        out.writeLong(queryId.getLeastSignificantBits());
        out.writeLong(serviceId.getMostSignificantBits());
        out.writeLong(serviceId.getLeastSignificantBits());
        out.writeInt(bopId);
        out.writeInt(partitionId); // Note: partitionId is 32-bits clean
        LongPacker.packLong(out,sinkMessagesOut);
        LongPacker.packLong(out,altSinkMessagesOut);
//        out.writeInt(sinkMessagesOut);
//        out.writeInt(altSinkMessagesOut);
        out.writeObject(stats);
        out.writeObject(cause);
    }

    @Override
    public void readExternal(final ObjectInput in) throws IOException,
            ClassNotFoundException {
        queryId = new UUID(in.readLong()/* MSB */, in.readLong()/* LSB */);
        serviceId = new UUID(in.readLong()/* MSB */, in.readLong()/* LSB */);
        bopId = in.readInt();
        partitionId = in.readInt();
        sinkMessagesOut = LongPacker.unpackInt(in);
        altSinkMessagesOut = LongPacker.unpackInt(in);
//        sinkMessagesOut = in.readInt();
//        altSinkMessagesOut = in.readInt();
        stats = (BOpStats) in.readObject();
        cause = (Throwable) in.readObject();
    }

}
