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
 * Created on Jan 25, 2008
 */

package com.bigdata.btree.compression;

import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.IKeyBuffer;
import com.bigdata.btree.MutableKeyBuffer;
import com.bigdata.btree.NodeSerializer;
import com.bigdata.btree.proc.IKeyArrayIndexProcedure;

/**
 * Interface for custom serialization of logical <code>byte[][]</code> data
 * such are used to represent keys or values. Some implementations work equally
 * well for both (e.g., no compression, huffman compression, dictionary
 * compression) while others can require that the data are fully ordered (e.g.,
 * hu-tucker compression which only works for keys).
 * <p>
 * Note: there are a few odd twists to this API. It is designed to be used both
 * for the keys and values in the {@link AbstractBTree} and for the keys and
 * values in {@link IKeyArrayIndexProcedure}s, which are auto-split against the
 * index partitions and hence use a {fromIndex, toIndex}. This leads to the
 * requirement that the caller allocate the {@link IRandomAccessByteArray} into
 * which the data will be de-serialized so that they can specify the capacity of
 * that object.
 * <p>
 * The standard practice is to write out <code>toIndex</code> as (toIndex -
 * fromIndex), which is the #of keys to be written. When the data are read back
 * in the fromIndex is assumed to be zero and toIndex will be the #of elements
 * to read. This has the effect of making the array compact. Likewise, when
 * <code>deserializedSize == 0</code> the size of the array to be allocated on
 * deserialization is normally computed as <code>toIndex - fromIndex</code> so
 * that the resulting array will be compact on deserialization. A non-zero value
 * is specified by the {@link NodeSerializer} so that the array is correctly
 * dimensioned for the branching factor of the {@link AbstractBTree} when it is
 * de-serialized.
 * <p>
 * Note: When the {@link IDataSerializer} will be used for values stored in an
 * {@link AbstractBTree} then it MUST handle [null] elements. The B+Tree both
 * allows <code>null</code>s and the {@link MutableKeyBuffer} will report a
 * <code>null</code> when the corresponding index entry is deleted (in an
 * index that supports deletion markers).
 * 
 * FIXME Verify that RMI serialization is heavily buffered since we are NOT
 * buffering the {@link OutputBitStream} and {@link InputBitStream} objects in
 * order to minimize heap churn and since we have the expectation that they are
 * writing/reading on buffered streams.
 * 
 * @todo support compression (hu-tucker, huffman, delta-coding of keys,
 *       application defined compression, etc.)
 *       <p>
 *       user-defined tokenization into symbols (hu-tucker, huffman)
 * 
 * @todo test suites for each of the {@link IDataSerializer}s, including when
 *       the reference is a null byte[][], when it has zero length, and when
 *       there are null elements in the byte[][].
 * 
 * @todo could use hamming codes for bytes.
 * 
 * @todo variants for custom parsing into symbols for hamming codes.
 * 
 * @todo use a delta from key to key specifying how much to be reused - works
 *       well if deserializing keys or for a linear key scan, but not for binary
 *       search on the raw node/leaf record.
 * 
 * @todo verify that keys are monotonic during constructor from mutable keys and
 *       during de-serialization.
 * 
 * @todo add nkeys to the abstract base class and keep a final (read-only) copy
 *       on this class. if the value in the base class is changed then it is an
 *       error but this allows direct access to the field in the base class for
 *       the mutable key buffer implementation.
 * 
 * @todo even more compact serialization of sorted byte[]s is doubtless
 *       possible, perhaps as a patricia tree, trie, or other recursive
 *       decomposition of the prefixes. figure out what decomposition is the
 *       most compact, easy to compute from sorted byte[]s, and supports fast
 *       search on the keys to identify the corresponding values.
 */
public interface IDataSerializer extends Serializable {

    /**
     * Write a byte[][], possibly empty and possibly <code>null</code>.
     * 
     * @param out
     *            An {@link OutputStream} that implements the {@link DataOutput}
     *            interface.
     * @param fromIndex
     *            The index of the first key in <i>keys</i> to be processed
     *            (inclusive).
     * @param toIndex
     *            The index of the last key in <i>keys</i> to be processed.
     * @param keys
     *            The data (MAY be a <code>null</code> reference IFF
     *            <code>toIndex-fromIndex == 0</code>, MAY be an empty array,
     *            and MAY contain <code>null</code> elements in the array).
     * 
     * @throws IOException
     */
    void write(DataOutput out, IRandomAccessByteArray raba) throws IOException;

//    void write(DataOutput out, int fromIndex, int toIndex, byte[][] keys) throws IOException;

//    public byte[][] read(DataInput in) throws IOException;

    /**
     * Variant that may be used to read into a caller provided
     * {@link IKeyBuffer}. The <code>byte[]</code>s are appended to the
     * {@link IKeyBuffer}. An exception is thrown if the {@link IKeyBuffer} is
     * not large enough.
     * 
     * @param in The elements will be read from here.
     * @param raba The elements will be appended to this object.
     * @throws IOException
     * 
     * @todo make sure that we can obtain an OutputStream that will let us
     *       append a key to the {@link IKeyBuffer} so that we can avoid
     *       allocation of byte[]s during de-serialization.
     */
    public void read(DataInput in, IRandomAccessByteArray raba) throws IOException;
    
//    /**
//     * Return an interface that may be used to read the data. When efficient
//     * this method MAY be implemented such that it buffers the data as a byte[]
//     * operates directly on that buffer. For example, this may be used to allow
//     * access to data stored in compressed form.
//     * 
//     * @param in
//     * @return
//     * @throws IOException
//     */
//    void read(DataInput in,Constructor<IRandomAccessByteArray>ctor) throws IOException; 
    
//    /**
//     * Read a byte[][], possibly empty and possibly <code>null</code>.
//     * 
//     * @param in
//     *            An {@link InputStream} that implements the {@link DataInput}
//     *            interface.
//     * 
//     * @return The keys.
//     * 
//     * @throws IOException
//     */
//    byte[][] read(DataInput in) throws IOException;

}
