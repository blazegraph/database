/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Nov 20, 2006
 */

package com.bigdata.btree;

import java.io.DataInput;
import java.io.IOException;
import java.io.Serializable;

import com.bigdata.io.DataOutputBuffer;

/**
 * (De-)serialize the keys in a {@link Leaf} or {@link Node}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @deprecated by {@link IDataSerializer}.
 */
public interface IKeySerializer extends Serializable {

    /**
     * De-serialize the keys.
     * 
     * @param is
     *            The input stream.
     * 
     * @return The keys.
     */
    public IKeyBuffer getKeys(DataInput is) throws IOException;

    /**
     * Serialize the keys onto the buffer.
     * 
     * @param os
     *            The output stream (the caller is responsible for flushing
     *            the stream).
     * @param keys
     *            The keys from a {@link Leaf} or {@link Node}.
     */
    public void putKeys(DataOutputBuffer os, IKeyBuffer keys)
            throws IOException;

}
