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
package com.bigdata.rwstore;

import java.io.InputStream;

/**
 * Interface for reading and writing streams on a persistence store.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface IStreamStore {

    /**
     * Return an output stream which can be used to write on the backing store.
     * You can recover the address used to read back the data from the
     * {@link IPSOutputStream}.
     * 
     * @return The output stream.
     */
    public IPSOutputStream getOutputStream();

    /**
     * Return an output stream which can be used to write on the backing store
     * within the given allocation context. You can recover the address used to
     * read back the data from the {@link IPSOutputStream}.
     * 
     * @param context
     *            The context within which any allocations are made by the
     *            returned {@link IPSOutputStream}.
     *            
     * @return an output stream to stream data to and to retrieve an address to
     *         later stream the data back.
     */
    public IPSOutputStream getOutputStream(final IAllocationContext context);

    /**
     * Return an input stream from which a previously written stream may be read
     * back.
     * 
     * @param addr
     *            The address at which the stream was written.
     *            
     * @return an input stream for the data for provided address
     */
    public InputStream getInputStream(long addr);

}
