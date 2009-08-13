package com.bigdata.btree.raba.codec;

import java.io.DataOutput;
import java.nio.ByteBuffer;

import com.bigdata.btree.compression.IDataSerializer;
import com.bigdata.btree.raba.IRandomAccessByteArray;

/**
 * Interface for coding a logical byte[][] onto a {@link ByteBuffer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
 * @version $Id$
 * 
 * @todo reconcile with {@link IDataSerializer}, which does the same thing
 *       for a {@link DataOutput} stream.
 */
public interface IDataCoder {

    /**
     * Return <code>true</code> if <code>null</code> byte[]s are allowed in the
     * logical byte[][] to be encoded.
     */
    public boolean isNullAllowed();

    /**
     * Encode the data.
     * 
     * @param raba
     *            The data.
     * 
     * @return The encoded data paired with the state and logic to decode the
     *         data.
     * 
     * @throws NullPointerException
     *             if <code>null</code>s are not allowed and any of the byte[]s
     *             in the logical byte[][] is <code>null</code>.
     */
    public IRabaDecoder encode(final IRandomAccessByteArray raba);

    /**
     * 
     * @param size
     *            The #of coded values.
     * @param data
     *            The record containing the coded data.
     * 
     * @return The decoder.
     */
    public IRabaDecoder decode(final int size, final ByteBuffer data);

}