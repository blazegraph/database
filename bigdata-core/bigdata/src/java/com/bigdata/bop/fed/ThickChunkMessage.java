/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on Sep 9, 2010
 */

package com.bigdata.bop.fed;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.rmi.RemoteException;
import java.util.UUID;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.engine.IChunkAccessor;
import com.bigdata.bop.engine.IChunkMessage;
import com.bigdata.bop.engine.IQueryClient;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.io.LongPacker;
import com.bigdata.io.ShortPacker;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVCache;
import com.bigdata.rdf.internal.encoder.IVSolutionSetDecoder;
import com.bigdata.rdf.internal.encoder.IVSolutionSetEncoder;
import com.bigdata.relation.accesspath.EmptyCloseableIterator;
import com.bigdata.relation.accesspath.ThickCloseableIterator;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * A thick version of this interface in which the chunk is sent inline with the
 * RMI message.
 * <p>
 * Note: The encoding is {@link IV} specific and supports the {@link IVCache}
 * associations. However, it CAN NOT be used with non-{@link IV} data. This is
 * fine in the deployed system but it makes the class properly dependent on the
 * RDF layer.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class ThickChunkMessage<E> implements IChunkMessage<E>, Externalizable {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private IQueryClient queryController;

    private UUID queryControllerId;
    
    private UUID queryId;

    private int bopId;
    
    private int partitionId;

    private int solutionCount;
    
    private byte[] data;

    @Override
    public IQueryClient getQueryController() {
        return queryController;
    }
    
    @Override
    public UUID getQueryControllerId() {
        return queryControllerId;
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
    public boolean isLastInvocation() {
        return false; // Never.
    }

    @Override
    public boolean isMaterialized() {
        return true;
    }

    @Override
    public int getSolutionCount() {
        return solutionCount;
    }
    
    public int getBytesAvailable() {
        return data.length;
    }

    @Override
    public String toString() {

        return getClass().getName() + "{queryId=" + queryId + ",bopId=" + bopId
                + ",partitionId=" + partitionId + ",controller="
                + queryController + ", solutionCount=" + solutionCount
                + ", bytesAvailable=" + data.length + "}";

    }

    /**
     * De-serialization constructor.
     */
    public ThickChunkMessage() {
        
    }
    
    public ThickChunkMessage(final IQueryClient queryController,
            final UUID queryId, final int bopId, final int partitionId,
            final IBindingSet[] source) {

        if (queryController == null)
            throw new IllegalArgumentException();

        if (queryId == null)
            throw new IllegalArgumentException();

        if (source == null)
            throw new IllegalArgumentException();

        // do not send empty chunks
        if (source.length == 0)
            throw new IllegalArgumentException();

        this.queryController = queryController;
        try {
            this.queryControllerId = queryController.getServiceUUID();
        } catch (RemoteException e) {
            throw new RuntimeException(e);
        }
        
        this.queryId = queryId;

        this.bopId = bopId;

        this.partitionId = partitionId;

        this.solutionCount = source.length;
        
        if (solutionCount == 0) {
            
            this.data = null;
            
        } else {

            /*
             * Encode the solutions.
             */

            // SWAG.
            final int initialCapacity = solutionCount * 24;

            final DataOutputBuffer out = new DataOutputBuffer(initialCapacity);
            
            final IVSolutionSetEncoder encoder = new IVSolutionSetEncoder();
            
            for (int i = 0; i < source.length; i++) {
            
                encoder.encodeSolution(out, source[i]);
                
            }

            this.data = out.toByteArray();
            
        }

    }

    @Override
    public void materialize(final FederatedRunningQuery runningQuery) {
        // NOP
    }

    @Override
    public void release() {
        if (chunkAccessor != null)
            chunkAccessor.close();
    }

    private transient volatile ChunkAccessor chunkAccessor = null;

    @Override
    public IChunkAccessor<E> getChunkAccessor() {

        return new ChunkAccessor();
        
    }

    /**
     * Deserialization of the binding sets.
     * <p>
     * Note: Both very small chunks (1-5 solutions) and large chunks (100-1000+)
     * are common on a cluster. The observed chunk size is a function of the
     * selectivity of the access path together with the chunk capacity (e.g., as
     * set by a query hint).
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/395">HTree
     *      performance tuning</a>
     */
    private class ChunkAccessor implements IChunkAccessor<E> {

        private final ICloseableIterator<E[]> source;

        @SuppressWarnings("unchecked")
        public ChunkAccessor() {

            if (solutionCount == 0) {
                
                source = new EmptyCloseableIterator<E[]>();
                
                return;
                
            }

            final IVSolutionSetDecoder decoder = new IVSolutionSetDecoder();

            final IBindingSet[] a = new IBindingSet[solutionCount];

            // Note: close() is NOT required for DataInputBuffer.
            final DataInputBuffer in = new DataInputBuffer(data, 0/* off */,
                    data.length);

            for (int i = 0; i < solutionCount; i++) {

                a[i] = decoder
                        .decodeSolution(in, true/* resolveCachedValues */);

            }

            source = new ThickCloseableIterator<E[]>(
                    ((E[][]) new IBindingSet[][] { a }));

        }

        @Override
        public ICloseableIterator<E[]> iterator() {

            return source;

        }

        public void close() {

            source.close();

        }

    }

    @Override
    public void writeExternal(final ObjectOutput out) throws IOException {
        ShortPacker.packShort(out, currentVersion);
        out.writeObject(queryController);
        out.writeLong(queryControllerId.getMostSignificantBits());
        out.writeLong(queryControllerId.getLeastSignificantBits());
        out.writeLong(queryId.getMostSignificantBits());
        out.writeLong(queryId.getLeastSignificantBits());
        out.writeInt(bopId);
        out.writeInt(partitionId);// Note: 32-bit clean.
        LongPacker.packLong(out, solutionCount); // non-negative
        if (solutionCount > 0) {
            LongPacker.packLong(out, data.length);
            out.write(data);
        }
    }

    @Override
    public void readExternal(final ObjectInput in) throws IOException,
            ClassNotFoundException {
        final short version = ShortPacker.unpackShort(in);
        if (version != VERSION0) {
            throw new IOException("Unknown version: " + version);
        }
        queryController = (IQueryClient) in.readObject();
        queryControllerId = new UUID(in.readLong()/* MSB */, in.readLong()/* LSB */);
        queryId = new UUID(in.readLong()/* MSB */, in.readLong()/* LSB */);
        bopId = in.readInt();
        partitionId = in.readInt();
        solutionCount = LongPacker.unpackInt(in);
        if (solutionCount > 0) {
            final int len = LongPacker.unpackInt(in);
            data = new byte[len];
            in.readFully(data);
        }

    }

    /**
     * The original version.
     */
    private static final transient short VERSION0 = 0x0;

    private static final transient short currentVersion = VERSION0;

}
