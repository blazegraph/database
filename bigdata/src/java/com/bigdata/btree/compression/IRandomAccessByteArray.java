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
 * Created on Mar 6, 2008
 */

package com.bigdata.btree.compression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.bigdata.io.ByteArrayBuffer;

/**
 * Interface designed for both flyweight wrappers over <code>byte[][]</code>
 * data and random access to the keys or values in a node or leaf of a B+Tree.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IRandomAccessByteArray extends Iterable<byte[]>{
    
    /**
     * The #of defined keys (size).
     */
    public int getKeyCount();

    /**
     * The maximum #of keys that may be held in the buffer (capacity).
     */
    public int getMaxKeys();
    
    /**
     * Return the key at the specified index.  Whenever possible, the reference
     * to the key is returned.  When necessary, a new byte[] will be allocated
     * and returned to the caller.
     * 
     * @param index
     *            The key index in [0:nkeys-1].
     *            
     * @return The key.
     */
    public byte[] getKey(int index);
    
    /**
     * Copy the key onto the output stream. This is often used with an
     * {@link ByteArrayBuffer} so that the same backing byte[] can be
     * overwritten by each visited key.
     * 
     * @param index
     *            The key index in [0:nkeys-1].
     * @param out
     *            The output stream onto which the key will be copied.
     * 
     * @return The #of bytes copied.
     * 
     * @throws NullPointerException
     *             if the key at that index is <code>null</code>.
     */
    public int copyKey(int index,DataOutput os) throws IOException;

    /**
     * The length of the key at that index.
     * 
     * @param index The index in [0:nkeys-1].
     * 
     * @return The length of the key at that index.
     *  
     * @throws NullPointerException
     *             if the key at that index is <code>null</code>.
     */
    public int getLength(int index);
    
    /**
     * Return <code>true</code> iff the key at that index is
     * <code>null</code>.
     * 
     * @param index
     *            The key index in [0:nkeys-1].
     */
    public boolean isNull(int index);

    /**
     * Return <code>true</code> if this implementation is read-only.
     */
    public boolean isReadOnly();

    /**
     * Set a key at the index.
     * 
     * @param index
     *            The key index in [0:nkeys-1].
     * @param key
     *            The key.
     */
    public void setKey(int index, byte[] key);

    /**
     * Append a key to the end of the buffer, incrementing the #of keys in
     * the buffer.
     * 
     * @param key
     *            A key.
     * 
     * @return The #of keys in the buffer.
     */
    public int add(byte[] key);
    
    /**
     * Append a key to the end of the buffer, incrementing the #of keys in the
     * buffer.
     * 
     * @param key
     *            A buffer containing a key.
     * @param off
     *            The offset of the first byte to be copied.
     * @param len
     *            The #of bytes to be copied.
     * 
     * @return The #of keys in the buffer.
     */
    public int add(byte[] key,int off,int len);
    
    /**
     * Append a key to the end of the buffer, incrementing the #of keys in the
     * buffer.
     * 
     * @param in
     *            The input stream from which the key will be read.
     * @param len
     *            The #of bytes to be read.
     * @return The #of keys in the buffer.
     * 
     * @todo also define variant of setKey() that copies bytes from an input
     *       stream.
     */
    public int add(DataInput in, int len) throws IOException;

}
