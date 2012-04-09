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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.NoSuchElementException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.log4j.Logger;

import com.bigdata.bop.IBindingSet;
import com.bigdata.btree.Checkpoint;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rdf.internal.encoder.IVSolutionSetDecoder;
import com.bigdata.rdf.internal.encoder.IVSolutionSetEncoder;
import com.bigdata.rwstore.PSOutputStream;
import com.bigdata.rwstore.sector.IMemoryManager;
import com.bigdata.striterator.ICloseableIterator;

/**
 * Metadata for a solution set declaration (sort of like a checkpoint record).
 * 
 * TODO Unit tests (this can be tested in isolation).
 * 
 * TODO Work out the relationship to {@link Checkpoint} records.
 */
final class SolutionSetMetadata {

    private static final Logger log = Logger
            .getLogger(SolutionSetMetadata.class);

    public final String name;

    private final IMemoryManager allocationContext;

    private long addr;

    public SolutionSetMetadata(final String name,
            final IMemoryManager allocationContext) {

        if (name == null)
            throw new IllegalArgumentException();

        if (allocationContext == null)
            throw new IllegalArgumentException();

        this.name = name;

        this.allocationContext = allocationContext;

    }

    public void clear() {

        allocationContext.clear();
        addr = IRawStore.NULL;

    }

    public ICloseableIterator<IBindingSet[]> get() {

        final long addr = this.addr;

        if (addr == IRawStore.NULL)
            throw new IllegalStateException();

        try {
            return new SolutionSetStreamDecoder(addr);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * Stream decoder for solution sets.
     */
    private class SolutionSetStreamDecoder implements
            ICloseableIterator<IBindingSet[]> {

        private final DataInputStream in;
        private final IVSolutionSetDecoder decoder;

        private boolean open = false;
        
        /** The next chunk of solutions to be visited. */
        private IBindingSet[] bsets = null;

        public SolutionSetStreamDecoder(final long addr)
                throws IOException {

            this.in = new DataInputStream(
                    wrapInputStream(allocationContext.getInputStream(addr)));

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

                    bsets = decodeNextChunk();
                    
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
        try {

            final DataOutputBuffer buf = new DataOutputBuffer();
            
            final OutputStream os = wrapOutputStream(out);

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
                os.write(chunk.length);

                // Write header (#of bytes buffered).
                os.write(bytesBuffered);
                
                // transfer buffer data to output stream.
                os.write(buf.array(), 0/* off */, bytesBuffered);

                // += headerSize (chunkSize,bytesBuffered) + bytesBuffered.
                nbytes += (Bytes.SIZEOF_INT + Bytes.SIZEOF_INT) + bytesBuffered;

                nsolutions += chunk.length;

            }

            os.flush();

            out.flush();

            newAddr = out.getAddr();

            if (log.isDebugEnabled())
                log.debug("Wrote " + nsolutions + "; encodedBytes=" + nbytes
                        + " bytes, bytesWritten=" + out.getBytesWritten());

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

        if (addr != IRawStore.NULL) {

            allocationContext.free(addr);

        }

        addr = newAddr;

    }

    private OutputStream wrapOutputStream(final OutputStream out)
            throws IOException {
     
        return new GZIPOutputStream(out);
        
    }

    private InputStream wrapInputStream(final InputStream in)
            throws IOException {

        return new GZIPInputStream(in);

    }

}
