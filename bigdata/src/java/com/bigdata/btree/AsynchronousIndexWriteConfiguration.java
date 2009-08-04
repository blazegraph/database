package com.bigdata.btree;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.CognitiveWeb.extser.LongPacker;

import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.resources.StaleLocatorException;
import com.bigdata.service.ndx.IAsynchronousWriteBufferFactory;
import com.bigdata.service.ndx.pipeline.AbstractMasterTask;
import com.bigdata.service.ndx.pipeline.AbstractSubtask;

/**
 * Configuration for the asynchronous index write API.
 * 
 * @see IAsynchronousWriteBufferFactory
 * @see AbstractMasterTask
 * @see AbstractSubtask
 * 
 * @todo rename since not specific to asynchronous index writes, even if normally
 * used in that context.
 */
public class AsynchronousIndexWriteConfiguration implements Externalizable {

    /**
     * 
     */
    private static final long serialVersionUID = -5142532074728169578L;

    /**
     * The capacity of the queue on which the application writes. Chunks are
     * drained from this queue by the {@link AbstractTaskMaster}, broken into
     * splits, and each split is written onto the {@link AbstractSubtask} sink
     * handling writes for the associated index partition.
     */
    public int getMasterQueueCapacity() {
        return masterQueueCapacity;
    }

    public void setMasterQueueCapacity(int masterQueueCapacity) {
        this.masterQueueCapacity = masterQueueCapacity;
    }

    private int masterQueueCapacity;

    /**
     * The desired size of the chunks that the master will process.
     */
    public int getMasterChunkSize() {
        return masterChunkSize;
    }

    public void setMasterChunkSize(int masterChunkSize) {
        this.masterChunkSize = masterChunkSize;
    }

    private int masterChunkSize;

    /**
     * The time in nanoseconds that the master will combine smaller chunks so
     * that it can satisfy the desired <i>masterChunkSize</i>.
     */
    public long getMasterChunkTimeoutNanos() {
        return masterChunkTimeoutNanos;
    }

    public void setMasterChunkTimeoutNanos(long masterChunkTimeoutNanos) {
        this.masterChunkTimeoutNanos = masterChunkTimeoutNanos;
    }

    private long masterChunkTimeoutNanos;

    /**
     * The time in nanoseconds after which an idle sink will be closed. Any
     * buffered writes are flushed when the sink is closed. The idle timeout is
     * reset (a) if a chunk is available to be drained by the sink; or (b) if a
     * chunk is drained from the sink. If no chunks become available the the
     * sink will eventually decide that it is idle, will flush any buffered
     * writes, and will close itself.
     * <p>
     * If the idle timeout is LT the {@link #getSinkChunkTimeoutNanos()} then a
     * sink will remain open as long as new chunks appear and are combined
     * within idle timeout, otherwise the sink will decide that it is idle and
     * will flush its last chunk and close itself. If the idle timeout is
     * {@link Long#MAX_VALUE} then the sink will identify itself as idle and
     * will only be closed if the master is closed or the sink has received a
     * {@link StaleLocatorException} for the index partition on which the sink
     * is writing.
     */
    public long getSinkIdleTimeoutNanos() {
        return sinkIdleTimeoutNanos;
    }

    public void setSinkIdleTimeoutNanos(long sinkIdleTimeoutNanos) {
        this.sinkIdleTimeoutNanos = sinkIdleTimeoutNanos;
    }

    private long sinkIdleTimeoutNanos;

    /**
     * The time in nanoseconds that the {@link AbstractSubtask sink} will wait
     * inside of the {@link IAsynchronousIterator} when it polls the iterator
     * for a chunk. This value should be relatively small so that the sink
     * remains responsible rather than blocking inside of the
     * {@link IAsynchronousIterator} for long periods of time.
     */
    public long getSinkPollTimeoutNanos() {
        return sinkPollTimeoutNanos;
    }

    public void setSinkPollTimeoutNanos(long sinkPollTimeoutNanos) {
        this.sinkPollTimeoutNanos = sinkPollTimeoutNanos;
    }

    private long sinkPollTimeoutNanos;

    /**
     * The capacity of the internal queue for the per-sink output buffer.
     */
    public int getSinkQueueCapacity() {
        return sinkQueueCapacity;
    }

    public void setSinkQueueCapacity(int sinkQueueCapacity) {
        this.sinkQueueCapacity = sinkQueueCapacity;
    }

    private int sinkQueueCapacity;

    /**
     * The desired size of the chunks written that will be written by the
     * {@link AbstractSubtask sink}.
     */
    public int getSinkChunkSize() {
        return sinkChunkSize;
    }

    public void setSinkChunkSize(int sinkChunkSize) {
        this.sinkChunkSize = sinkChunkSize;
    }

    private int sinkChunkSize;

    /**
     * The maximum amount of time in nanoseconds that a sink will combine
     * smaller chunks so that it can satisfy the desired <i>sinkChunkSize</i>.
     */
    public long getSinkChunkTimeoutNanos() {
        return sinkChunkTimeoutNanos;
    }

    public void setSinkChunkTimeoutNanos(long sinkChunkTimeoutNanos) {
        this.sinkChunkTimeoutNanos = sinkChunkTimeoutNanos;
    }

    private long sinkChunkTimeoutNanos;

    public String toString() {

        final StringBuilder sb = new StringBuilder();
        
        sb.append(getClass().getName());
        
        sb.append("{ masterQueueCapacity=" + masterQueueCapacity);
        
        sb.append(", masterChunkSize=" + masterChunkSize);
        
        sb.append(", masterChunkTimeoutNanos=" + masterChunkTimeoutNanos);

        sb.append(", sinkIdleTimeoutNanos=" + sinkIdleTimeoutNanos);

        sb.append(", sinkPollTimeoutNanos=" + sinkPollTimeoutNanos);

        sb.append(", sinkQueueCapacity=" + sinkQueueCapacity);
        
        sb.append(", sinkChunkSize=" + sinkChunkSize);
        
        sb.append(", sinkChunkTimeoutNanos=" + sinkChunkTimeoutNanos);

        sb.append("}");
        
        return sb.toString();
        
    }
    
    /**
     * <strong>De-serialization ctor</strong>
     */
    public AsynchronousIndexWriteConfiguration() {
        
    }

    /**
     * 
     * @param masterQueueCapacity
     *            The capacity of the queue on which the application writes.
     *            Chunks are drained from this queue by the
     *            {@link AbstractTaskMaster}, broken into splits, and each split
     *            is written onto the {@link AbstractSubtask} sink handling
     *            writes for the associated index partition.
     * @param masterChunkSize
     *            The desired size of the chunks that the master will process.
     * @param masterChunkTimeoutNanos
     *            The time in nanoseconds that the master will combine smaller
     *            chunks so that it can satisfy the desired
     *            <i>masterChunkSize</i>.
     * @param sinkIdleTimeoutNanos
     *            The time in nanoseconds after which an idle sink will be
     *            closed. Any buffered writes are flushed when the sink is
     *            closed. The idle timeout is reset (a) if a chunk is available
     *            to be drained by the sink; or (b) if a chunk is drained from
     *            the sink. If no chunks become available the the sink will
     *            eventually decide that it is idle, will flush any buffered
     *            writes, and will close itself.
     *            <p>
     *            If the idle timeout is LT the
     *            {@link #getSinkChunkTimeoutNanos()} then a sink will remain
     *            open as long as new chunks appear and are combined within idle
     *            timeout, otherwise the sink will decide that it is idle and
     *            will flush its last chunk and close itself. If this is
     *            {@link Long#MAX_VALUE} then the sink will identify itself as
     *            idle and will only be closed if the master is closed or the
     *            sink has received a {@link StaleLocatorException} for the
     *            index partition on which the sink is writing.
     * @param sinkPollTimeoutNanos
     *            The time in nanoseconds that the {@link AbstractSubtask sink}
     *            will wait inside of the {@link IAsynchronousIterator} when it
     *            polls the iterator for a chunk. This value should be
     *            relatively small so that the sink remains responsible rather
     *            than blocking inside of the {@link IAsynchronousIterator} for
     *            long periods of time.
     * @param sinkQueueCapacity
     *            The capacity of the internal queue for the per-sink output
     *            buffer.
     * @param sinkChunkSize
     *            The desired size of the chunks written that will be written by
     *            the {@link AbstractSubtask sink}.
     * @param sinkChunkTimeoutNanos
     *            The maximum amount of time in nanoseconds that a sink will
     *            combine smaller chunks so that it can satisify the desired
     *            <i>sinkChunkSize</i>.
     */
    public AsynchronousIndexWriteConfiguration(//
            final int masterQueueCapacity,//
            final int masterChunkSize,//
            final long masterChunkTimeoutNanos,//
            final long sinkIdleTimeoutNanos,//
            final long sinkPollTimeoutNanos,//
            final int sinkQueueCapacity,//
            final int sinkChunkSize,//
            final long sinkChunkTimeoutNanos//
            ) {

        if (masterQueueCapacity <= 0)
            throw new IllegalArgumentException();

        if (masterChunkSize <= 0)
            throw new IllegalArgumentException();
        
        if (masterChunkTimeoutNanos <= 0)
            throw new IllegalArgumentException();
        
        if (sinkIdleTimeoutNanos <= 0)
            throw new IllegalArgumentException();
        
        if (sinkPollTimeoutNanos <= 0)
            throw new IllegalArgumentException();
        
        if (sinkQueueCapacity <= 0)
            throw new IllegalArgumentException();
        
        if (sinkChunkTimeoutNanos <= 0)
            throw new IllegalArgumentException();
        
        if (sinkIdleTimeoutNanos <= 0)
            throw new IllegalArgumentException();

        this.masterQueueCapacity = masterQueueCapacity;
        this.masterChunkSize = masterChunkSize;
        this.masterChunkTimeoutNanos = masterChunkTimeoutNanos;

        this.sinkIdleTimeoutNanos = sinkIdleTimeoutNanos;
        this.sinkPollTimeoutNanos = sinkPollTimeoutNanos;
        this.sinkQueueCapacity = sinkQueueCapacity;
        this.sinkChunkSize = sinkChunkSize;
        this.sinkChunkTimeoutNanos = sinkChunkTimeoutNanos;

    }

    private static transient final int VERSION0 = 0;

    public void readExternal(final ObjectInput in) throws IOException,
            ClassNotFoundException {

        final int version = (int) LongPacker.unpackLong(in);

        if (version != VERSION0)
            throw new IOException("Unknown version: " + version);

        // master.
        masterQueueCapacity = (int) LongPacker.unpackLong(in);

        masterChunkSize = (int) LongPacker.unpackLong(in);
        
        masterChunkTimeoutNanos = LongPacker.unpackLong(in);

        // sink.
        sinkIdleTimeoutNanos = LongPacker.unpackLong(in);

        sinkPollTimeoutNanos = LongPacker.unpackLong(in);

        sinkQueueCapacity = (int) LongPacker.unpackLong(in);

        sinkChunkSize = (int) LongPacker.unpackLong(in);

        sinkChunkTimeoutNanos = LongPacker.unpackLong(in);

    }

    public void writeExternal(final ObjectOutput out) throws IOException {

        LongPacker.packLong(out, VERSION0);
        
        LongPacker.packLong(out, masterQueueCapacity);
        LongPacker.packLong(out, masterChunkSize);
        LongPacker.packLong(out, masterChunkTimeoutNanos);
        
        LongPacker.packLong(out, sinkIdleTimeoutNanos);
        LongPacker.packLong(out, sinkPollTimeoutNanos);
        LongPacker.packLong(out, sinkQueueCapacity);
        LongPacker.packLong(out, sinkChunkSize);
        LongPacker.packLong(out, sinkChunkTimeoutNanos);

    }

}
