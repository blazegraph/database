package com.bigdata.journal.bd;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.UUID;

import com.bigdata.service.ResourceService;
import com.bigdata.service.ResourceService.ReadResourceTask;

/**
 * The smart proxy object (sent over the wire). This communicates with the
 * {@link ResourceService} on the backend for efficient transfer of the
 * buffered data over a socket.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
 * @version $Id$
 * 
 * @todo we are sending the {@link InetAddress} each time. It would be nice
 *       to have a smaller message if we could somehow move that into an
 *       appropriate context.
 */
public class RemoteBufferDescriptorProxy implements
        BufferDescriptor, Externalizable {

    private UUID id;

    private int size;

    private int chksum;

    private InetAddress addr;

    private int port;
    
    public UUID getId() {
        return id;
    }

    public int size() {
        return size;
    }

    public int getChecksum() {
        return chksum;
    }

    /**
     * @todo as per {@link ResourceService#ReadResourceTask}, read the data
     *       on the socket from the remote service.
     *       <P>
     *       We will have to refactor the {@link ResourceService} a smidge.
     *       It is designed to read from a file and write on a file. We will
     *       need to abstract that to allow reading from the identified
     *       buffer and writing on the caller's buffer. We might as well
     *       change the UUID thing at the same time.
     */
    public void get(ByteBuffer b) {

        try {
            new ReadResourceTask(addr, port, id, null/*
                                                      * Currently [file], but
                                                      * this should be the
                                                      * caller's ByteBuffer
                                                      */).call();
        } catch (Exception e) {

            throw new RuntimeException(e);
            
        }
        
    }

    /**
     * Deserialization constructor.
     */
    public RemoteBufferDescriptorProxy() {

    }

    /**
     * 
     * @param id
     *            The identifier for the buffer.
     * @param size
     *            The #of bytes of data in the buffer.
     * @param chksum
     *            The checksum of the data in the buffer.
     * @param addr
     *            The address of the service which will send the data in the
     *            remote buffer.
     * @param port
     *            The port at which to talk to that service.
     */
    public RemoteBufferDescriptorProxy(final UUID id, final int size,
            final int chksum, final InetAddress addr, final int port) {
        this.id = id;
        this.size = size;
        this.chksum = chksum;
        this.addr = addr;
        this.port = port;
    }
    
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {

        final long mostSigBits = in.readLong();
        final long leastSigBits = in.readLong();
        this.id = new UUID(mostSigBits, leastSigBits);
        this.size = in.readInt();
        this.chksum = in.readInt();
        this.port = in.readInt();
        this.addr = (InetAddress)in.readObject();

    }

    public void writeExternal(ObjectOutput out) throws IOException {

        out.writeLong(id.getMostSignificantBits());
        out.writeLong(id.getLeastSignificantBits());
        out.writeInt(size);
        out.writeInt(chksum);
        out.writeInt(port);
        out.writeObject(addr);

    }

}