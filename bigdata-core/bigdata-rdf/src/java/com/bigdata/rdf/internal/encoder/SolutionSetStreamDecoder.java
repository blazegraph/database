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
 * Created on Jun 29, 2012
 */
package com.bigdata.rdf.internal.encoder;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;

import com.bigdata.bop.IBindingSet;
import com.bigdata.io.DataInputBuffer;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * Stream decoder for solution sets (chunk oriented).
 * 
 * @see SolutionSetStreamEncoder
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class SolutionSetStreamDecoder implements
        ICloseableIterator<IBindingSet[]> {

    private static final Logger log = Logger
            .getLogger(SolutionSetStreamDecoder.class);

    /** The name of the solution set (optional). */
    private final String name;

    private final long solutionSetCount;
    private final DataInputStream in;
    private final IVSolutionSetDecoder decoder;

    private boolean open = false;

    /** The next chunk of solutions to be visited. */
    private IBindingSet[] bsets = null;

    /** The #of solution sets which have been decoded so far. */
    private long nsolutions = 0;

    /**
     * 
     * @param name
     *            The name of the solution set.
     * @param in
     *            The solutions are read from this stream.
     * @param solutionSetCount
     *            The #of solutions to be read from the stream.
     * @throws IOException
     */
    public SolutionSetStreamDecoder(final String name,
            final DataInputStream in, final long solutionSetCount) {

        if (in == null)
            throw new IllegalArgumentException();

        if (solutionSetCount < 0)
            throw new IllegalArgumentException();

        this.name = name;

        this.solutionSetCount = solutionSetCount;

        this.in = in;

        this.open = true;

        this.decoder = new IVSolutionSetDecoder();

    }

    @Override
    public void close() {
        if (open) {
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
                log.debug("Read solutionSet: name=" + name
                        + ", solutionSetSize=" + nsolutions);

            return null;

        }

        // The version# for this chunk.
        // final int version =
        in.readInt(); // version

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

            t[i] = decoder.decodeSolution(buf, true/* resolveCachedValues */);

            if (log.isTraceEnabled())
                log.trace("Read: name=" + name + ", solution=" + t[i]);

        }

        // Update the #of solution sets which have been decoded.
        nsolutions += chunkSize;

        if (log.isTraceEnabled())
            log.trace("Read chunk: name=" + name + ", chunkSize=" + chunkSize
                    + ", bytesRead="
                    + (SolutionSetStreamEncoder.CHUNK_HEADER_SIZE + byteLength)
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
