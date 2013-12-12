package com.bigdata.ha.msg;

import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;

import com.bigdata.io.DataOutputBuffer;
import com.bigdata.rawstore.Bytes;

public class HASendState implements IHASendState, Externalizable {

//    private static final Logger log = Logger.getLogger(HASendState.class);

    private static final long serialVersionUID = 1;

    private long messageId;
    private UUID originalSenderId;
    private UUID senderId;
    private long token;
    private int replicationFactor;

    /**
     * De-serialization constructor.
     */
    public HASendState() {

    }

    public HASendState(final long messageId, final UUID originalSenderId,
            final UUID senderId, final long token, final int replicationFactor) {

        if (originalSenderId == null)
            throw new IllegalArgumentException();

        if (senderId == null)
            throw new IllegalArgumentException();

        if (replicationFactor <= 0)
            throw new IllegalArgumentException();

        this.messageId = messageId;
        this.originalSenderId = originalSenderId;
        this.senderId = senderId;
        this.token = token;
        this.replicationFactor = replicationFactor;

    }

    @Override
    public long getMessageId() {

        return messageId;

    }

    @Override
    public UUID getOriginalSenderId() {

        return originalSenderId;
    }

    @Override
    public UUID getSenderId() {

        return senderId;
    }

    @Override
    public long getQuorumToken() {

        return token;

    }

    @Override
    public int getReplicationFactor() {

        return replicationFactor;

    }

    @Override
    public byte[] getMarker() {

        final byte[] a = new byte[MAGIC_SIZE + currentVersionLen];

        final DataOutputBuffer dob = new DataOutputBuffer(0/* len */, a);

        try {

            dob.writeLong(MAGIC);
            
            writeExternal2(dob);

        } catch (IOException e) {

            throw new RuntimeException(e);

        }

        return a;

    }

    @Override
    public String toString() {

        return super.toString() + "{messageId=" + messageId
                + ",originalSenderId=" + originalSenderId + ",senderId="
                + senderId + ",token=" + token + ", replicationFactor="
                + replicationFactor + "}";

    }

    @Override
    public boolean equals(final Object obj) {

        if (this == obj)
            return true;

        if (!(obj instanceof IHASendState))
            return false;

        final IHASendState t = (IHASendState) obj;

        return messageId == t.getMessageId()
                && originalSenderId.equals(t.getOriginalSenderId())
                && senderId.equals(t.getSenderId()) && token == t.getQuorumToken()
                && replicationFactor == t.getReplicationFactor();

    }

    @Override
    public int hashCode() {

        // based on the messageId and the hashCode of the senderId
        return ((int) (messageId ^ (messageId >>> 32))) + senderId.hashCode();
    }

    /**
     * Magic data only included in the marker.
     */
    private static final long MAGIC = 0x13759f98e8363caeL;
    private static final int MAGIC_SIZE = Bytes.SIZEOF_LONG;
    
    private static final transient short VERSION0 = 0x0;
    private static final transient int VERSION0_LEN = //
            Bytes.SIZEOF_LONG + // messageId
            Bytes.SIZEOF_UUID + // originalSenderId
            Bytes.SIZEOF_UUID + // senderId
            Bytes.SIZEOF_LONG + // token
            Bytes.SIZEOF_INT // replicationFactor
    ;

    private static final transient short currentVersion = VERSION0;
    private static final transient int currentVersionLen = VERSION0_LEN;

    @Override
    public void readExternal(final ObjectInput in) throws IOException,
            ClassNotFoundException {

        final short version = in.readShort();

        if (version != VERSION0)
            throw new RuntimeException("Bad version for serialization");

        messageId = in.readLong();

        originalSenderId = new UUID(//
                in.readLong(), /* MSB */
                in.readLong() /* LSB */
        );

        senderId = new UUID(//
                in.readLong(), /* MSB */
                in.readLong() /* LSB */
        );

        token = in.readLong();

        replicationFactor = in.readInt();

    }

    @Override
    public void writeExternal(final ObjectOutput out) throws IOException {

        writeExternal2(out);

    }

    private void writeExternal2(final DataOutput out) throws IOException {

        out.writeShort(currentVersion);

        out.writeLong(messageId);

        out.writeLong(originalSenderId.getMostSignificantBits());
        out.writeLong(originalSenderId.getLeastSignificantBits());

        out.writeLong(senderId.getMostSignificantBits());
        out.writeLong(senderId.getLeastSignificantBits());

        out.writeLong(token);

        out.writeInt(replicationFactor);

    }

    // static final private int MARKER_SIZE = 8;
    //
    // /**
    // * Unique marker generation with JVM wide random number generator.
    // *
    // * @return A "pretty unique" marker.
    // */
    // private byte[] genMarker() {
    //
    // final byte[] token = new byte[MARKER_SIZE];
    //
    // while (!unique1(token)) {
    // r.nextBytes(token);
    // }
    //
    // return token;
    // }
    //
    // /**
    // * Checks that the first byte is not repeated in the remaining bytes, this
    // * simplifies search for the token in the input stream.
    // */
    // static private boolean unique1(final byte[] bytes) {
    // final byte b = bytes[0];
    // for (int t = 1; t < bytes.length; t++) {
    // if (bytes[t] == b)
    // return false;
    // }
    //
    // return true;
    // }

}
