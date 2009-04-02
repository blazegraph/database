package com.bigdata.mdi;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.bigdata.journal.ITimestampService;
import com.bigdata.resources.ResourceManager;

/**
 * Encapsulate the reason why an index partition was created and the
 * synchronous overflow counter of the data service on which the index
 * partition was created as of that action. This information may be used to
 * support heuristics which refuse to move an index partition which was
 * recently moved or to join an index partition which was recently split,
 * etc.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class IndexPartitionCause implements Externalizable {

    /**
     * 
     */
    private static final long serialVersionUID = 4643434468430418713L;

    /**
     * Typesafe enumeration of reasons why an index partition was created.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    static public enum CauseEnum {

        /**
         * The initial registration of a scale-out index.
         */
        Register((byte) 0),

        /** An index partition was split. */
        Split((byte) 1),

        /** Two or more index partitions were joined. */
        Join((byte) 2),

        /** An index partition was moved to another data service. */
        Move((byte) 3);

        private CauseEnum(final byte code) {
            this.code = code;
        }

        private final byte code;

        static IndexPartitionCause.CauseEnum valueOf(final byte code) {
            switch (code) {
            case 0:
                return Register;
            case 1:
                return Split;
            case 2:
                return Join;
            case 3:
                return Move;
            default:
                throw new IllegalArgumentException("code=" + code);
            }
        }

    }

    private IndexPartitionCause.CauseEnum cause;

    private long synchronousOverflowCounter;

    private long lastCommitTime;

    /**
     * The underlying cause.
     */
    public IndexPartitionCause.CauseEnum getCause() {
        return cause;
    }

    /**
     * The value of the synchronous overflow counter at the time that the
     * index partition was created.
     */
    public long getSynchronousOverflowCounter() {
        return synchronousOverflowCounter;
    }

    /**
     * The lastCommitTime for the live journal. Note that this will be 0L if no
     * commits have been performed on the journal. This makes the "time" less
     * robust than the overflow counter.
     */
    public long getLastCommitTime() {
        return lastCommitTime;
    }

    /**
     * Factory for {@link CauseEnum#Register}.
     */
    static public IndexPartitionCause register(ResourceManager resourceManager) {

        return new IndexPartitionCause(
                IndexPartitionCause.CauseEnum.Register,
                resourceManager.getSynchronousOverflowCount(),
                resourceManager.getLiveJournal()
                        .getLastCommitTime());

    }
    
    /**
     * Factory for {@link CauseEnum#Split}.
     */
    static public IndexPartitionCause split(ResourceManager resourceManager) {

        return new IndexPartitionCause(
                IndexPartitionCause.CauseEnum.Split,
                resourceManager.getSynchronousOverflowCount(),
                resourceManager.getLiveJournal()
                        .getLastCommitTime());

    }

    /**
     * Factory for {@link CauseEnum#Join}.
     */
    static public IndexPartitionCause join(ResourceManager resourceManager) {

        return new IndexPartitionCause(
                IndexPartitionCause.CauseEnum.Join,
                resourceManager.getSynchronousOverflowCount(),
                resourceManager.getLiveJournal()
                        .getLastCommitTime());

    }

    /**
     * Factory for {@link CauseEnum#Move}.
     */
    static public IndexPartitionCause move(ResourceManager resourceManager) {

        return new IndexPartitionCause(
                IndexPartitionCause.CauseEnum.Move,
                resourceManager.getSynchronousOverflowCount(),
                resourceManager.getLiveJournal()
                        .getLastCommitTime());

    }
    
    /**
     * 
     * @param cause
     *            The reason why the index partition was created.
     * @param synchronousOverflowCounter
     *            The value of the counter at the time that the index partition
     *            was created.
     * @param lastCommitTime
     *            The lastCommitTime for the live journal. Note that this will
     *            be 0L if no commits have been performed on the journal. This
     *            makes the "time" less robust than the overflow counter.
     */
    public IndexPartitionCause(final IndexPartitionCause.CauseEnum cause,
            final long synchronousOverflowCounter, final long lastCommitTime) {

        if (cause == null)
            throw new IllegalArgumentException();

        if (synchronousOverflowCounter < 0)
            throw new IllegalArgumentException();

        this.cause = cause;

        this.synchronousOverflowCounter = synchronousOverflowCounter;

        this.lastCommitTime = lastCommitTime;

    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {

        cause = CauseEnum.valueOf(in.readByte());

        synchronousOverflowCounter = in.readLong();

        lastCommitTime = in.readLong();

    }

    public void writeExternal(ObjectOutput out) throws IOException {

        out.writeByte(cause.code);
        out.writeLong(synchronousOverflowCounter);
        out.writeLong(lastCommitTime);

    }

    public String toString() {
        
        return getClass().getName() + "{" + cause + ",overflowCounter="
                + synchronousOverflowCounter + ",timestamp=" + lastCommitTime
                + "}";

    }

}
