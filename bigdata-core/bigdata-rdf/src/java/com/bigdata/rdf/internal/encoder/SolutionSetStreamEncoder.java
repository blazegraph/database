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

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.log4j.Logger;

import com.bigdata.bop.IBindingSet;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.rdf.sparql.ast.ISolutionSetStats;
import com.bigdata.rdf.sparql.ast.SolutionSetStatserator;
import com.bigdata.util.Bytes;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * Stream encoder for solution sets (chunk oriented).
 * 
 * @see SolutionSetStreamDecoder
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * TODO Refactor the test suite for this from the SparqlCache test suite.
 */
public class SolutionSetStreamEncoder {

    private static final Logger log = Logger
            .getLogger(SolutionSetStreamEncoder.class);

    /**
     * The per-chunk header.
     * <p>
     * Note: The version number is per-chunk in case we ever support splicing
     * solution streams (for example, appending one stream onto another at a
     * low-level in the store).
     */
    static int CHUNK_HEADER_SIZE = //
            Bytes.SIZEOF_INT + // version number.
            Bytes.SIZEOF_INT + // #of solutions in chunk.
            Bytes.SIZEOF_INT // #of bytes in payload
    ;

    /** The initial version. */
    static final int VERSION0 = 0x0;

    /** The current version. */
    static int CURRENT_VERSION = VERSION0;

    /** The name of the solution set (optional). */
    private final String name;
    /** #of solutions written. */
    private long nsolutions = 0;
    /** #of bytes for the encoded solutions (before compression). */
    private long nbytes = 0;
    /** The #of chunks written. */
    private long chunkCount = 0;
    /** The statistics for the encoded solutions (set when done). */
    private ISolutionSetStats stats = null;

    /** #of solutions written. */
    public long getSolutionCount() {
        return nsolutions;
    }

    /** The statistics for the encoded solutions (set when done). */
    public ISolutionSetStats getStats() {
        return stats;
    }

    public SolutionSetStreamEncoder(final String name) {

        this.name = name;

    }

    /**
     * Encode solutions onto the output stream.
     * 
     * @param os
     *            The output stream.
     * @param src2
     *            The solutions to be encoded.
     */
    public void encode(final DataOutputStream os,
            final ICloseableIterator<IBindingSet[]> src2) throws IOException {

        // wrap with class to compute statistics over the observed
        // solutions.
        final SolutionSetStatserator src = new SolutionSetStatserator(src2);

        final IVSolutionSetEncoder encoder = new IVSolutionSetEncoder();

        final DataOutputBuffer buf = new DataOutputBuffer();

        try {

            while (src.hasNext()) {

                // Discard the data in the buffer.
                buf.reset();

                // Chunk of solutions to be written.
                final IBindingSet[] chunk = src.next();

                // Write solutions.
                for (int i = 0; i < chunk.length; i++) {

                    encoder.encodeSolution(buf, chunk[i]);

                    if (log.isTraceEnabled())
                        log.trace("Wrote name=" + name + ", solution="
                                + chunk[i]);

                }

                // #of bytes written onto the buffer.
                final int bytesBuffered = buf.limit();

                // The chunk version number.
                os.writeInt(CURRENT_VERSION);

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
                    log.debug("Wrote chunk: name=" + name + ", chunkSize="
                            + chunk.length + ", chunkCount=" + chunkCount
                            + ", bytesBuffered=" + bytesBuffered
                            + ", solutionSetSize=" + nsolutions);

            }

            // Save reference to the solution set statistics.
            this.stats = src.getStats();

            if (log.isDebugEnabled())
                log.debug("Wrote solutionSet: name=" + name
                        + ", solutionSetSize=" + nsolutions + ", chunkCount="
                        + chunkCount + ", encodedBytes=" + nbytes
                // + ", bytesWritten=" + out.getBytesWritten()
                );

        } finally {

            src.close();
            // os.close();

        }

    }

}
