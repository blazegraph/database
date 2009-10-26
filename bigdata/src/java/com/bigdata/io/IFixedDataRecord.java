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
 * Created on Sep 8, 2009
 */

package com.bigdata.io;

import it.unimi.dsi.io.InputBitStream;

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Interface adds some methods for stream-based access to {@link IDataRecord}.
 * <p>
 * Note: These methods are isolated on this interface to prevent confusion with
 * the stream-based semantics of the {@link ByteArrayBuffer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IFixedDataRecord extends IDataRecord {

    /**
     * Return an input stream that will read from the slice.
     */
    public DataInputBuffer getDataInput();
    
    /**
     * Return a bit stream that will read from the slice.
     * <p>
     * Note: You DO NOT need to close this stream since it is backed by a
     * byte[]. In fact, {@link InputBitStream#close()} when backed by a byte[]
     * appears to have relatively high overhead, which is weird.
     */
    public InputBitStream getInputBitStream();
    
    /**
     * Write the slice on the output stream.
     * 
     * @param os
     *            The output stream.
     * 
     * @throws IOException
     */
    void writeOn(final OutputStream os) throws IOException;
    
    /**
     * Write the slice on the output stream.
     * 
     * @param os
     *            The output stream.
     *            
     * @throws IOException
     */
    void writeOn(final DataOutput out) throws IOException;
    
    /**
     * Write part of the slice on the output stream.
     * 
     * @param os
     *            The output stream.
     *            
     * @throws IOException
     */
    void writeOn(final OutputStream os, final int aoff,
            final int alen) throws IOException;
}
