/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
/*
 * Created on Apr 21, 2010
 */

package com.bigdata.journal.ha;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.concurrent.RunnableFuture;

import com.bigdata.cache.ConcurrentWeakValueCache;
import com.bigdata.journal.AbstractJournal;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class HAGlue {

    /**
     * A descriptor of a buffer from which a remote node may read some data.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     * 
     * FIXME make this into a smart proxy which exposes the ability to read
     * the data from the buffer on the remote node.  use DGC to avoid GC of
     * the entry for the buffer descriptor in the cache on the remote node.
     */
    public static class BufferDescriptor implements Externalizable {

        private int id;
        private int size;
        private int chksum;
        
        /**
         * An identifier for a specific buffer view when is assigned when the
         * buffer is registered to make its contents available to the failover
         * group
         */
        public int getId() {return id;}

        /**
         * The #of bytes to be read from the buffer.
         */
        public int size() {return size;}
        
        /**
         * The checksum of the data in the buffer.
         */
        public int getChecksum() {return chksum;}
        
        /**
         * Deserialization constructor.
         */
        public BufferDescriptor() {
            
        }

        BufferDescriptor(final int id, final int size, final int checksum) {

            this.id = id;
            this.size = size;
            this.chksum = checksum;
            
        }

        public void readExternal(ObjectInput in) throws IOException,
                ClassNotFoundException {

            this.id = in.readInt();
            this.size = in.readInt();
            this.chksum = in.readInt();
            
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            
            out.writeInt(id);
            out.writeInt(size);
            out.writeInt(chksum);
            
        }
        
    }

    /*
     * @todo Factory for buffer descriptors, resolving ids to buffers, sending
     * data to the requester on a socket, and managing the release of those
     * buffers.
     */
    static class BufferDescriptionFactory {

        private class Wrapper {
            final BufferDescriptor bd;
            final ByteBuffer b;
            Wrapper(BufferDescriptor bd,ByteBuffer b) {
                this.bd = bd;
                this.b = b;
            }
        }
        
        final private ConcurrentWeakValueCache<Integer, Wrapper> cache = new ConcurrentWeakValueCache<Integer,Wrapper>();

        private int nextId = 0;

        /*
         * @todo would be nice to have an eventually consistent approach for
         * this which did not rely on CAS operations (not AtomicInteger). There
         * are some things like this in the transactional memory literature. It
         * would also have to handle rollover, which makes it tricker.
         */
        private synchronized int nextId() {

            if (nextId == Integer.MAX_VALUE) {

                nextId = 0;
                
            } else {
                
                nextId++;
                
            }

            return nextId;
            
        }
        
        /**
         * Return a {@link BufferDescriptor} for the caller's buffer. The
         * position, limit, and mark of the buffer must not be changed by this
         * operation.
         * 
         * @param b 
         * @param chksum
         * @return
         */
        public BufferDescriptor addBuffer(ByteBuffer b, int chksum) {
            
            while(true) {
            
                final int id = nextId();
                
                final Wrapper w = new Wrapper( new BufferDescriptor(id, b
                        .remaining(), chksum), b);

                if (cache.putIfAbsent(id, w) == null) {
                    
                    return w.bd;
                    
                }
                
                // try again.
            
            }
            
        }

        public Buffer get(final BufferDescriptor bd) {

            final Wrapper w = cache.get(bd.getId());
            
            if(w == null) return null;
            
            return w.b;
            
        }
        
    }
    
    /*
     * write pipeline.
     */
    
    public RunnableFuture<Void> write(long newaddr, long oldaddr, BufferDescriptor bd) {
        
        throw new UnsupportedOperationException();
        
    }

    /*
     * file extension.
     */
    
    public RunnableFuture<Void> truncate(long extent) {
        
        throw new UnsupportedOperationException();
        
    }

    /*
     * commit protocol.
     */

    public void commitNow(long commitTime) {
     
        throw new UnsupportedOperationException();

    }
    
    public void abort() {
 
        throw new UnsupportedOperationException();

    }

    /**
     * Message sent to each non-master member of the quorum. The node should
     * retrieve the root block from the master, flush all writes to the backing
     * channel, and acknowledge "yes" if ready to commit.  If the node can not
     * prepare for any reason, then it must return "no".
     * 
     * @param commitCounter
     * @param bd
     *            A buffer from which the node can obtain the root block for the
     *            commit operation.
     * 
     * @return Vote yes/no on prepared to commit.
     */
    RunnableFuture<Boolean> prepare2Phase(long commitCounter, BufferDescriptor bd) {

        throw new UnsupportedOperationException();

    }

    /**
     * Message sent to each non-master member of the quorum telling it to commit
     * using the root block from the corresponding prepare message.
     * 
     * @param commitCounter
     * 
     * @return
     */
    RunnableFuture<Void> commit2Phase(long commitCounter) {

        throw new UnsupportedOperationException();

    }

    /**
     * Message sent to each non-master member of the quorum telling it to
     * discard any writes which would be associated with the specified commit
     * counter. If the node had prepared, then it would also discard the root
     * block image if it had been read. The node then issues an
     * {@link AbstractJournal#abort()} request, reloading all state from the
     * last root block.
     * 
     * @param commitCounter
     * @return
     */
    RunnableFuture<Void> abort2Phase(long commitCounter) {

        throw new UnsupportedOperationException();

    }

    /*
     * @todo Quorum membership
     */

    /**
     * 
     */
    public interface Quorum {
       
        /** The #of members of the quorum. */
        int size();
        
        /** The quorum target capacity is the replication factor (k). */
        int capacity();
        
        /** Return true iff size GTE (capacity + 1). */
        boolean isQuorumMet();
        
        /**
         * Submits the message for execution by the next node in the quorum.
         */
        <T> void applyNext(RunnableFuture<T> r);

//        /**
//         * The set of nodes which are discovered and have an agreement on the
//         * distributed state of the journal.
//         */
//        ServiceID[] getServiceIDsInQuorum();
        
    }
    
    public Quorum getQuorum() {

        throw new UnsupportedOperationException();
        
    }
    
}
