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
 * Created on Feb 7, 2007
 */

package com.bigdata.btree;

/**
 * <p>
 * Interface for non-batch operations on a B+-Tree mapping non-null variable
 * length unsigned byte[] keys to arbitrary values.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @see UnicodeKeyBuilder, which may be used to encode one or more primitive
 *      data type values or Unicode strings into a variable length unsigned
 *      byte[] key.
 */
public interface ISimpleBTree {

    /**
     * Insert or update a value under the key.
     * 
     * @param key
     *            The key.
     * @param value
     *            The value (may be null).
     * 
     * @return The previous value under that key or <code>null</code> if the
     *         key was not found or if the previous entry for that key was
     *         marked as deleted.
     */
    public byte[] insert(byte[] key, byte[] value);
    
    /**
	 * Insert or update a value under the key iff there is no entry for that key
	 * in the index. This is equivalent to
	 * 
	 * <pre>
	 * if (!contains(key))
	 * 	insert(key, value);
	 * </pre>
	 * 
	 * However, if the index allows <code>null</code> values to be stored under
	 * a key and the application in fact stores <code>null</code> values for
	 * some tuples, then caller is not able to decide using this method whether
	 * or not the mutation was applied based on the return value. For these
	 * cases if the caller needs to know whether or not the conditional mutation
	 * actually took place, the caller CAN use the pattern
	 * <code>if(!contains()) insert(key,value);</code> to obtain that
	 * information.
	 * 
	 * @param key
	 *            The key.
	 * @param value
	 *            The value (may be null).
	 * 
	 * @return The previous value under that key or <code>null</code> if the key
	 *         was not found or if the previous entry for that key was marked as
	 *         deleted. Note that the return value MAY be <code>null</code> even
	 *         if there was an entry under the key. This is because the index is
	 *         capable of storing a <code>null</code> value. In such cases the
	 *         conditional mutation WAS NOT applied.
	 * 
	 * @see BLZG-1539 (putIfAbsent)
	 */
    public byte[] putIfAbsent(byte[] key, byte[] value);
    
    /**
     * Lookup a value for a key.
     * 
     * @return The value stored under that key or <code>null</code> if there
     *         is no entry for that key or if the entry under that key is marked
     *         as deleted.
     */
    public byte[] lookup(byte[] key);

    /**
     * Return <code>true</code> iff there is a (non-deleted) index entry for
     * the key. An index entry with a <code>null</code> value will cause this
     * method to return <code>true</code>. A deleted index entry will cause
     * this method to return <code>false</code>.
     * 
     * @param key
     *            The key.
     * 
     * @return <code>true</code> if the index contains an (un-deleted) entry
     *         for that key.
     */
    public boolean contains(byte[] key);
        
    /**
     * Remove the key and its associated value.
     * 
     * @param key
     *            The key.
     * 
     * @return The value stored under that key or <code>null</code> if the key
     *         was not found or if the previous entry under that key was marked
     *         as deleted.
     */
    public byte[] remove(byte[] key);

}
