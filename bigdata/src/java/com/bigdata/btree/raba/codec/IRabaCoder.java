package com.bigdata.btree.raba.codec;

import java.io.DataOutput;
import java.nio.ByteBuffer;

import com.bigdata.btree.ResultSet;
import com.bigdata.btree.compression.IDataSerializer;
import com.bigdata.btree.proc.IIndexProcedure;
import com.bigdata.btree.raba.IRaba;

/**
 * Interface for coding a logical byte[][] onto a {@link ByteBuffer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo reconcile with {@link IDataSerializer}, which does the same thing for a
 *       {@link DataOutput} stream. Attend use of the coded data in
 *       {@link ResultSet} and {@link IIndexProcedure}s.
 */
public interface IRabaCoder {

    /**
     * Return <code>true</code> this implementation can code B+Tree keys
     * (supports search on the coded representation). Note that some
     * implementation can code either keys or values.
     */
    public boolean isKeyCoder();

    /**
     * Return <code>true</code> this implementation can code B+Tree values
     * (allows <code>null</code>s). Note that some implementation can code
     * either keys or values.
     */
    public boolean isValueCoder();

    /**
     * Encode the data.
     * 
     * @param raba
     *            The data.
     * 
     * @return The encoded data paired with the state and logic to decode the
     *         data.
     * 
     * @throws UnsupportedOperationException
     *             if {@link IRaba#isKeys()} is true and this {@link IRabaCoder}
     *             can not code keys.
     * @throws UnsupportedOperationException
     *             if {@link IRaba#isKeys()} is false and this
     *             {@link IRabaCoder} can not code values.
     */
    public IRabaDecoder encode(final IRaba raba);

    /**
     * Return an {@link IRaba} which can access the coded data. Implementations
     * SHOULD NOT materialize a backing byte[][]. Instead, the implementation
     * SHOULD access the data in place within the {@link ByteBuffer}. Frequently
     * used fields MAY be cached, but the whole point of the {@link IRabaCoder}
     * is to minimize the in-memory footprint for the B+Tree by using a coded
     * (aka compressed) representation of the keys and values whenever possible.
     * 
     * @param data
     *            The record containing the coded data.
     * 
     * @return The decoder.
     */
    public IRabaDecoder decode(final ByteBuffer data);

}
