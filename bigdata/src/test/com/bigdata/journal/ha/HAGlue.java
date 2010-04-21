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
import java.util.concurrent.Executor;
import java.util.concurrent.RunnableFuture;

import net.jini.core.lookup.ServiceID;

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

        /**
         * The set of nodes which are discovered and have an agreement on the
         * distributed state of the journal.
         */
        ServiceID[] getServicesInQuorum();
        
    }
    
    public Executor[] getQuorum() {

        throw new UnsupportedOperationException();
        
    }
    
}
