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
 * Created on Jun 10, 2011
 */

package com.bigdata.btree.keys;

import com.bigdata.rdf.internal.IV;
import com.bigdata.striterator.IKeyOrder;

/**
 * An extension interface for encoding and decoding unsigned byte[] keys.
 * <p>
 * Note: This was developed to provide backwards compatibility for the full text
 * index with the change of term identifiers from simple <code>long</code>
 * values to variable length {@link IV}s. However, the extension principle is of
 * general use and a similar mechanism exists to allow the override when
 * assembling key components for {@link IKeyOrder}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <V>
 *            The value type of the document identifier.
 */
public interface IKeyBuilderExtension<V> {

    /**
     * Decode an object from an unsigned byte[] key.
     * 
     * @param key
     *            The key.
     * @param off
     *            The offset of the start of the encoded object.
     *            
     * @return The decoded object.
     */
    V decode(byte[] key,int off);

    /**
     * Return as-encoded byte length of an object.
     * 
     * @param obj
     *            The object.
     *            
     * @return The byte length when encoded into a component of an unsigned
     *         byte[] key.
     */
    int byteLength(V obj);

    /**
     * Encode the object.
     * 
     * @param keyBuilder
     *            The object being used to encode the key.
     * @param obj
     *            The object to be encoded.
     */
    void encode(IKeyBuilder keyBuilder,V obj);
    
}
