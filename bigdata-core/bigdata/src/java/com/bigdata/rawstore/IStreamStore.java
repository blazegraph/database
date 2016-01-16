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
package com.bigdata.rawstore;

import java.io.InputStream;

/**
 * Interface for reading and writing streams on a persistence store.
 * 
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/555" > Support
 *      PSOutputStream/InputStream at IRawStore </a>
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
