package com.bigdata.journal.bd;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.UUID;

import com.bigdata.cache.ConcurrentWeakValueCache;
import com.bigdata.service.ResourceService;

/**
 * A factory which wraps {@link ByteBuffer}s and exposes them for remote
 * reads. The factory generates {@link RemoteBufferDescriptorImpl} objects,
 * which are Jini style smart proxies. These objects are exported as
 * {@link RemoteBufferDescriptorProxy} instances, which are
 * {@link Serializable} and can be send using RMI to a JVM which can then
 * request the data in the described buffer. The actual data transfer will
 * then take place using a low-level socket protocol.
 * <p>
 * The caller's buffers are wrapped as read-only views. An internal weak
 * value cache is maintained for those read-only views buffers. Entries are
 * cleared from the cache after the {@link RemoteBufferDescriptorImpl}
 * instances are swept by GC.
 */
class BufferDescriptionFactory {

    private final ResourceService resourceService;
    
    public BufferDescriptionFactory(final ResourceService resourceService) {

        this.resourceService = resourceService;
        
    }
    
    /**
     * Weak value cache. Entries are cleared after the values have been
     * swept by GC.
     */
    final private ConcurrentWeakValueCache<UUID, RemoteBufferDescriptorImpl> cache = new ConcurrentWeakValueCache<UUID, RemoteBufferDescriptorImpl>();

    /**
     * FIXME Refactor the {@link ResourceService} to abstract the identifier for
     * the resource so we do not need to use a {@link UUID} here, which is both
     * a stress for the {@link SecureRandom} generator and for the
     * {@link HashMap}. This is also required since the {@link ResourceService}
     * reads from a (remote) file and writes on a (local) file and we need to
     * read from a remote buffer and write on a local buffer.
     * 
     * @todo The id could be the long addr plus the UUID of the journal
     *       resource? We might use the UUID of the journal to resolve which
     *       record was demanded for failover reads.
     */
    private UUID nextId() {

        return UUID.randomUUID();

    }

    /**
     * Wrap the caller's buffer with an object which supports remote reads
     * on the data in that buffer. The caller MUST NOT change the position,
     * limit, or mark of the buffer once it has been wrapped. The buffer
     * SHOULD be a direct {@link ByteBuffer} since socket level IO will be
     * more efficient. 
     * 
     * @param b
     * @param chksum
     * @return
     */
    public RemoteBufferDescriptorImpl addBuffer(ByteBuffer b, final int chksum) {

        // take a read-only view of the caller's buffer to prevent side-effects.
        b = b.asReadOnlyBuffer();
        
        while (true) {

            final UUID id = nextId();

            final RemoteBufferDescriptorImpl impl = new RemoteBufferDescriptorImpl(
                    id, b.remaining(), chksum, b);

            if (cache.putIfAbsent(id, impl) == null) {

                return impl;

            }
            
            // try again.
        
        }
        
    }

}