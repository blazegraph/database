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
import com.bigdata.btree.NodeSerializer;
import com.bigdata.btree.proc.IKeyArrayIndexProcedure;
import com.bigdata.btree.raba.IRaba;
import com.bigdata.btree.raba.MutableKeyBuffer;
import com.bigdata.btree.raba.codec.IRabaCoder;

/**
 * Interface for custom serialization of logical <code>byte[][]</code> data such
 * are used to represent keys or values. Some implementations work equally well
 * for both (e.g., no compression, huffman compression, dictionary compression)
 * while others can require that the data are fully ordered (e.g., hu-tucker
 * compression which only works for keys).
 * <p>
 * Note: there are a few odd twists to this API. It is designed to be used both
 * for the keys and values in the {@link AbstractBTree} and for the keys and
 * values in {@link IKeyArrayIndexProcedure}s, which are auto-split against the
 * index partitions and hence use a {fromIndex, toIndex}. This leads to the
 * requirement that the caller allocate the
 * {@link IMutableRandomAccessByteArray} into which the data will be
 * de-serialized so that they can specify the capacity of that object.
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
 * <code>null</code> when the corresponding index entry is deleted (in an index
 * that supports deletion markers).
 * 
 * FIXME Verify that RMI serialization is heavily buffered since we are NOT
 * buffering the {@link OutputBitStream} and {@link InputBitStream} objects in
 * order to minimize heap churn and since we have the expectation that they are
 * writing/reading on buffered streams.
 * 
 * @todo test suites for each of the {@link IDataSerializer}s, including when
 *       the reference is a null byte[][], when it has zero length, and when
 *       there are null elements in the byte[][].
 * 
 * @todo verify that keys are monotonic during constructor from mutable keys and
 *       during de-serialization.
 * 
 * @deprecated This interface is stream oriented. As a consequence it is not
 *             possible to operate directly on the coded representation. This
 *             has been deprecated by {@link IRabaCoder} which is designed to
 *             code an {@link IRaba} and return an object wrapping the coded
 *             record while providing efficient random access and search
 *             operations.
 */
public interface IDataSerializer extends Serializable {

    /**
     * Code a logical <code>byte[][]</code> onto an output stream.
     * 
     * @param out
     *            An {@link OutputStream} that implements the {@link DataOutput}
     *            interface.
     * @param raba
     *            The logical byte[][] (MAY be an empty array, and MAY contain
     *            <code>null</code> elements iff {@link IRaba#isKeys()} is
     *            <code>false</code>).
     * 
     * @throws IOException
     */
    void write(DataOutput out, IRaba raba) throws IOException;

    /**
     * Decode an input stream into a caller provided {@link IRaba}. The
     * <code>byte[]</code>s are appended to the {@link IRaba}. An exception is
     * thrown if the {@link IRaba} is not large enough.
     * 
     * @param in
     *            The elements will be read from here.
     * @param raba
     *            The elements will be appended to this object.
     * 
     * @throws IOException
     */
    public void read(DataInput in, IRaba raba) throws IOException;

}
