/**

Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

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
 * Created on Apr 9, 2012
 */

package com.bigdata.rdf.sparql.ast.cache;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.NoSuchElementException;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import org.apache.log4j.Logger;

import com.bigdata.bop.IBindingSet;
import com.bigdata.btree.Checkpoint;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rdf.internal.encoder.IVSolutionSetDecoder;
import com.bigdata.rdf.internal.encoder.IVSolutionSetEncoder;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rwstore.PSOutputStream;
import com.bigdata.rwstore.sector.IMemoryManager;
import com.bigdata.striterator.ICloseableIterator;

/**
 * Metadata for a solution set declaration (sort of like a checkpoint record).
 * 
 * TODO Work out the relationship to {@link Checkpoint} records. Everything
 * about this solution set which is updatable should be organized into a
 * checkpoint. The metadata declaration for the solution set should also be
 * organized into the checkpoint, perhaps as an ISPO[], perhaps using a rigid
 * schema. The size of the solution set should be visible when its metadata is
 * looked at as a graph.
 */
final class SolutionSetMetadata {

    private static final Logger log = Logger
            .getLogger(SolutionSetMetadata.class);

    /**
     * The name of the solution set.
     */
    public final String name;

    /**
     * The metadata describing the solution set.
     */
    private final ISPO[] metadata;
    
    /**
     * The {@link IMemoryManager} on which the solution set will be written and
     * from which it will be read.
     */
    private final IMemoryManager allocationContext;

    /**
     * The #of solutions in this solutionset.
     */
    private long solutionCount;
    
    /**
     * The address from which the solution set may be read.
     */
    private long solutionSetAddr;

    /**
     * 
     * @param name
     *            The name of the solution set.
     * @param allocationContext
     *            The {@link IMemoryManager} on which the solution set will be
     *            written and from which it will be read.
     * @param metadata
     *            The metadata describing the solution set.
     */
    public SolutionSetMetadata(final String name,
            final IMemoryManager allocationContext, final ISPO[] metadata) {

        if (name == null)
            throw new IllegalArgumentException();

        if (allocationContext == null)
            throw new IllegalArgumentException();

        this.name = name;

        this.allocationContext = allocationContext;

        this.metadata = metadata;

    }

    public void clear() {

        allocationContext.clear();
        solutionSetAddr = IRawStore.NULL;

    }

    public ICloseableIterator<IBindingSet[]> get() {

        final long solutionSetAddr = this.solutionSetAddr;

        if (solutionSetAddr == IRawStore.NULL)
            throw new IllegalStateException();

        try {
            return new SolutionSetStreamDecoder(solutionSetAddr, solutionCount);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * The per-chunk header is currently two int32 fields. The first field is
     * the #of solutions in that chunk. The second field is the #of bytes to
     * follow in the payload for that chunk.
     * 
     * TODO Should there be an overall header for the solution set or are we
     * going to handle that through the solution set metadata?
     */
    static private int CHUNK_HEADER_SIZE = Bytes.SIZEOF_INT + Bytes.SIZEOF_INT;

    /**
     * Stream decoder for solution sets.
     */
    private class SolutionSetStreamDecoder implements
            ICloseableIterator<IBindingSet[]> {

        private final long solutionSetCount;
        private final DataInputStream in;
        private final IVSolutionSetDecoder decoder;

        private boolean open = false;
        
        /** The next chunk of solutions to be visited. */
        private IBindingSet[] bsets = null;
        
        /** The #of solution sets which have been decoded so far. */
        private long nsolutions = 0;

        public SolutionSetStreamDecoder(final long solutionSetAddr,
                final long solutionSetCount) throws IOException {

            this.solutionSetCount = solutionSetCount;

            this.in = new DataInputStream(
                    wrapInputStream(allocationContext
                            .getInputStream(solutionSetAddr)));

            this.open = true;

            this.decoder = new IVSolutionSetDecoder();

        }

        @Override
        public void close() {
            if(open) {
                open = false;
                try {
                    in.close();
                } catch (IOException e) {
                    // Unexpected exception.
                    log.error(e, e);
                }
            }
        }

        @Override
        public boolean hasNext() {

            if (open && bsets == null) {
            
                /*
                 * Read ahead and extract a chunk of solutions.
                 */

                try {

                    if ((bsets = decodeNextChunk()) == null) {

                        // Nothing more to be read.
                        close();

                    }

                } catch (IOException e) {

                    throw new RuntimeException(e);

                }

            }

            return open && bsets != null;

        }

        /**
         * Read ahead and decode the next chunk of solutions.
         * 
         * @return The decoded solutions.
         */
        private IBindingSet[] decodeNextChunk() throws IOException {

            if (nsolutions == solutionSetCount) {

                // Nothing more to be read.

                if (log.isDebugEnabled())
                    log.debug("Read solutionSet: solutionSetSize=" + nsolutions);
                
                return null;

            }
            
            // #of solutions in this chunk.
            final int chunkSize = in.readInt();

            // #of bytes in this chunk.
            final int byteLength = in.readInt();

            // Read in all the bytes in the next chunk.
            final byte[] a = new byte[byteLength];

            // read data
            in.readFully(a);

            // Wrap byte[] as stream.
            final DataInputBuffer buf = new DataInputBuffer(a);

            // Allocate array for the decoded solutions.
            final IBindingSet[] t = new IBindingSet[chunkSize];

            // Decode the solutions into the array.
            for (int i = 0; i < chunkSize; i++) {

                t[i] = decoder
                        .decodeSolution(buf, true/* resolveCachedValues */);

            }

            // Update the #of solution sets which have been decoded.
            nsolutions += chunkSize;

            if (log.isTraceEnabled())
                log.trace("Read chunk: chunkSize=" + chunkSize + ", bytesRead="
                        + (CHUNK_HEADER_SIZE + byteLength)
                        + ", solutionSetSize=" + nsolutions);

            // Return the decoded solutions.
            return t;

        }

        @Override
        public IBindingSet[] next() {
            
            if (!hasNext())
                throw new NoSuchElementException();
            
            final IBindingSet[] t = bsets;
            
            bsets = null;
            
            return t;
        }

        @Override
        public void remove() {
           
            throw new UnsupportedOperationException();
            
        }

    }

    public void put(final ICloseableIterator<IBindingSet[]> src) {

        if (src == null)
            throw new IllegalArgumentException();

        final IVSolutionSetEncoder encoder = new IVSolutionSetEncoder();

        // Stream writing onto the backing store.
        final PSOutputStream out = allocationContext.getOutputStream();
        // Address from which the solutions may be read.
        final long newAddr;
        // #of solutions written.
        long nsolutions = 0;
        // #of bytes for the encoded solutions (before compression).
        long nbytes = 0;
        // The #of chunks written.
        long chunkCount = 0;
        try {

            final DataOutputBuffer buf = new DataOutputBuffer();
            
            final DataOutputStream os = new DataOutputStream(
                    wrapOutputStream(out));

            try {

                while (src.hasNext()) {

                    // Discard the data in the buffer.
                    buf.reset();

                    // Chunk of solutions to be written.
                    final IBindingSet[] chunk = src.next();

                    // Write solutions.
                    for (int i = 0; i < chunk.length; i++) {

                        encoder.encodeSolution(buf, chunk[i]);

                    }

                    // #of bytes written onto the buffer.
                    final int bytesBuffered = buf.limit();

                    // Write header (#of solutions in this chunk).
                    os.writeInt(chunk.length);

                    // Write header (#of bytes buffered).
                    os.writeInt(bytesBuffered);

                    // transfer buffer data to output stream.
                    os.write(buf.array(), 0/* off */, bytesBuffered);

                    // += headerSize + bytesBuffered.
                    nbytes += CHUNK_HEADER_SIZE + bytesBuffered;

                    nsolutions += chunk.length;

                    chunkCount++;

                    if (log.isDebugEnabled())
                        log.debug("Wrote chunk: chunkSize=" + chunk.length
                                + ", chunkCount=" + chunkCount
                                + ", bytesBuffered=" + bytesBuffered
                                + ", solutionSetSize=" + nsolutions);

                }

                os.flush();
            
            } finally {
                
                os.close();
                
            }

            out.flush();

            newAddr = out.getAddr();

            if (log.isDebugEnabled())
                log.debug("Wrote solutionSet: solutionSetSize=" + nsolutions
                        + ", chunkCount=" + chunkCount + ", encodedBytes="
                        + nbytes + ", bytesWritten=" + out.getBytesWritten());

        } catch (IOException e) {

            throw new RuntimeException(e);

        } finally {

            try {
                out.close();
            } catch (IOException e) {
                // Unexpected exception.
                log.error(e, e);
            }

        }

        if (solutionSetAddr != IRawStore.NULL) {

            /*
             * Release the old solution set.
             */

            allocationContext.free(solutionSetAddr);

        }

        // TODO This is not atomic (needs lock).
        solutionSetAddr = newAddr;
        solutionCount = nsolutions;

    }

    /**
     * TODO Test performance with and without gzip. Extract into the CREATE
     * schema.
     */
    private static final boolean zip = true;
    
    private OutputStream wrapOutputStream(final OutputStream out)
            throws IOException {

        if (zip)
            return new DeflaterOutputStream(out);

        return out;

    }

    private InputStream wrapInputStream(final InputStream in)
            throws IOException {

        if (zip)
            return new InflaterInputStream(in);

        return in;

    }

}
