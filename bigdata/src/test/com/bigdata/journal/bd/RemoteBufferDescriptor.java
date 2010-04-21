package com.bigdata.journal.bd;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Backend interface.
 */
public interface RemoteBufferDescriptor {
    
    /**
     * An identifier for a specific buffer view when is assigned when the
     * buffer is registered to make its contents available to the failover
     * group
     */
    public UUID getId();

    /**
     * The #of bytes to be read from the buffer.
     */
    public int size();

    /**
     * The checksum of the data in the buffer.
     */
    public int getChecksum();

    /**
     * The buffer containing the data.
     */
    public ByteBuffer getBuffer();
    
}