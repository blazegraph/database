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
 * Created on Jan 31, 2008
 */

package com.bigdata.btree;


/**
 * An interface defining non-batch methods available on a local (non-remote)
 * btree, including method for inserting, removing, lookup, and containment
 * tests where keys and values are implicitly converted to and from byte[]s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ILocalBTree extends ILinearList {

    /**
     * Insert with auto-magic handling of keys and value objects.
     * 
     * @param key
     *            The key - when not a byte[] the key is implicitly converted to
     *            a byte[] using {@link KeyBuilder#asSortKey(Object)}
     * @param value
     *            The value - when not a byte[] the value is converted to a
     *            byte[] using Java standard serialization.
     * 
     * @return The old value, if any. The application MUST de-serialize the
     *         byte[] if they are storing objects.
     */
    public byte[] insert(Object key, Object value);
    
    /**
     * Lookup a value for a key.
     * 
     * @param key
     *            The key - when not a byte[] the key is implicitly converted to
     *            a byte[] using {@link KeyBuilder#asSortKey(Object)}
     *            
     * @return The value or <code>null</code> if there is no entry for that
     *         key.
     */
    public byte[] lookup(Object key);

    /**
     * Return true iff there is an entry for the key.
     * 
     * @param key
     *            The key - when not a byte[] the key is implicitly converted to
     *            a byte[] using {@link KeyBuilder#asSortKey(Object)}
     * 
     * @return True if the btree contains an entry for that key.
     */
    public boolean contains(Object key);
        
    /**
     * Remove the key and its associated value.
     * 
     * @param key
     *            The key - when not a byte[] the key is implicitly converted to
     *            a byte[] using {@link KeyBuilder#asSortKey(Object)}
     * 
     * @return The value stored under that key or <code>null</code> if the key
     *         was not found.
     */
    public byte[] remove(Object key);

}
