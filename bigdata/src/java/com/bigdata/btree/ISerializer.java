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
 * Created on May 27, 2008
 */

package com.bigdata.btree;

import java.io.Serializable;

/**
 * An abstraction for serializing and de-serializing objects as byte[]s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ISerializer extends Serializable {

    /**
     * Serialize an object.
     * 
     * @param obj
     *            A object (optional).
     * 
     * @return A byte[] that is the serialization of that object.
     */
    byte[] serialize(Object obj);

    /**
     * De-serialize an object.
     * <p>
     * Note: Some serializers use the convention that a <code>null</code> will
     * be de-serialized as a <code>null</code>. This convention makes it easy
     * to de-serialize the value and then test to see whether or not the value
     * was in fact found in the index.
     * 
     * @param data
     *            The data.
     * 
     * @return The object for that data.
     */
    Object deserialize(byte[] data);

}
