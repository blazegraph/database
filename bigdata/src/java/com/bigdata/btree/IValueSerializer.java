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
import com.bigdata.rawstore.IAddressManager;

/**
 * (De-)serialize the values in a {@link Leaf}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IValueSerializer extends Serializable {

    /**
     * De-serialize the values.
     * 
     * @param is
     *            The input stream.
     * @param values
     *            The array into which the values must be written.
     * @param nvals
     *            The #of valid values in the array. The values in indices
     *            [0:n-1] are defined and must be read from the buffer and
     *            written on the array.
     */
    public void getValues(DataInput is, Object[] values, int nvals)
            throws IOException;

    /**
     * Serialize the values.
     * 
     * @param os
     *            The output stream.
     * @param values
     *            The array of values from the {@link Leaf}.
     * @param nvals
     *            The #of valid values in the array. The values in indices
     *            [0:n-1] are defined and must be written.
     */
    public void putValues(DataOutputBuffer os, Object[] values, int nvals)
            throws IOException;

}
