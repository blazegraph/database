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

package com.bigdata.io;

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

/**
 * An abstraction for serializing and de-serializing objects on streams.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see SerializerUtil
 * @see IRecordSerializer
 * 
 * @todo reconcile {@link org.CognitiveWeb.extser.ISerializer}
 */
public interface IStreamSerializer<T> extends Serializable {

    /**
     * Serialize an object.
     * <p>
     * Note: All state required to de-serialize the object must be written onto
     * the stream. That may include serializer state as well, such as dictionary
     * that will be used on the other end to decode the object. In such cases
     * the serializer needs to know how to de-serialize both the dictionary and
     * the data. Stateful serializers such as <em>extSer</em> must encapsulate
     * all requisite state on the output stream.
     * 
     * @param out
     *            The stream onto which the object's state will be written.
     * @param obj
     *            The object.
     */
    void serialize(ObjectOutput out, T obj);

    /**
     * De-serialize an object.
     * 
     * @param in
     *            The stream from which the object's state will be read.
     * 
     * @return The object.
     */
    T deserialize(ObjectInput in);

}
