package com.bigdata.journal.bd;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * A descriptor of a buffer from which a remote node may read some data.
 * This is part of a smart proxy pattern.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
 * @version $Id$
 * 
 * @todo When exporting a smart proxy for this, use DGC to avoid GC of the
 *       entry in the cache on the remote node.
 * 
 * @todo The backend implementation must not have side effects on the
 *       position, limit or mark for the buffer when transferring the data.
 *       This will let multiple remote clients read from the same buffer
 *       instance.
 */
public class RemoteBufferDescriptorImpl implements RemoteBufferDescriptor {

    private UUID id;
    private int size;
    private int chksum;
    private ByteBuffer b;
    
    public UUID getId() {return id;}

    public int size() {return size;}
    
    public int getChecksum() {return chksum;}
    
    public ByteBuffer getBuffer() {return b;}
    
    // @todo size is redundant.
    RemoteBufferDescriptorImpl(final UUID id, final int size, final int checksum,
            final ByteBuffer b) {

        this.id = id;
        this.size = size;
        this.chksum = checksum;
        this.b = b;

    }

}