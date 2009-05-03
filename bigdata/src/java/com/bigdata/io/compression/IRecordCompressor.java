/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on May 2, 2009
 */

package com.bigdata.io.compression;

import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Interface for record-level compression.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IRecordCompressor {

    /**
     * Compresses data onto the output stream.
     * 
     * @param bin
     *            The data. The data from the position to the limit will be
     *            compressed. The position will be advanced to the limit as a
     *            side effect.
     * @param os
     *            The stream onto which the compressed data are written.
     */
    void compress(final ByteBuffer bin, final OutputStream os);

    /**
     * Compresses data onto the output stream.
     * 
     * @param bytes
     *            The data.
     * @param os
     *            The stream onto which the compressed data are written.
     */
    void compress(final byte[] bytes, final OutputStream os);

    /**
     * Compresses data onto the output stream.
     * 
     * @param bytes
     *            The source data.
     * @param off
     *            The offset of the first source byte that will be compressed
     *            onto the output stream.
     * @param len
     *            The #of source bytes that will be compressed onto the output
     *            stream.
     * @param os
     *            The stream onto which the compressed data are written.
     */
    void compress(final byte[] bytes, final int off, final int len,
            final OutputStream os);

    /**
     * Decompress a {@link ByteBuffer} containing the record and return the
     * uncompressed state.
     * 
     * @param bin
     *            The compressed data.
     * 
     * @return A view onto a shared buffer. The data between position() and
     *         limit() are the decompressed data. The contents of this buffer
     *         are valid only until the next compression or decompression
     *         request.
     */
    ByteBuffer decompress(final ByteBuffer bin);

    /**
     * Decompress a <code>byte[]</code> containing the record and return the
     * uncompressed state.
     * 
     * @param bin
     *            The compressed data.
     * 
     * @return A read-only view onto a shared buffer. The data between
     *         position() and limit() are the decompressed data. The contents of
     *         this buffer are valid only until the next compression or
     *         decompression request. The position will be zero. The limit will
     *         be the #of decompressed bytes.
     */
    ByteBuffer decompress(final byte[] bin);

}
