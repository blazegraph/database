/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Dec 17, 2006
 */

package com.bigdata.btree;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

import com.bigdata.io.ByteBufferInputStream;


/**
 * Bulk data (de-)compressor used for leaves in {@link IndexSegment}s. The
 * compression and decompression operations of a given {@link RecordCompressor}
 * reuse a shared instance buffer. Any decompression result is valid only until
 * the next compression or decompression operation performed by that
 * {@link RecordCompressor}. When used in a single-threaded context this
 * reduces allocation while maximizing the opportunity for bulk transfers.
 * 
 * @todo define an interface for a record compressor and write the class of the
 *       compressor used into the {@link IndexSegmentMetadata} so that we can
 *       experiment with other compression schemes in a backward compatible
 *       manner. If the compressor has parameters then those can be capture by
 *       subclassing or instance data.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RecordCompressor implements Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = -2028159717578047153L;

    /**
     * A huge portion of the cost associated with using {@link Deflator} is
     * the initialization of a new instance. Since this code is designed to
     * operate within a single-threaded environment, we just reuse the same
     * instance for each invocation.
     */
    final private transient Deflater _deflater;

    final private transient Inflater _inflater = new Inflater();

    /**
     * Reused on each decompression request and reallocated if buffer size would
     * be exceeded. This will achieve a steady state sufficient to decompress
     * any given input in a single pass.
     */
    private transient byte[] _buf = new byte[1024];

    /**
     * Create a record compressor.
     * 
     * @param level
     *            The compression level.
     * 
     * @see Deflater#BEST_SPEED
     * @see Deflater#BEST_COMPRESSION
     */
    public RecordCompressor(int level) {

        _deflater = new Deflater(Deflater.BEST_SPEED);

    }

    /**
     * Create a record compressor using {@link Deflator#BEST_SPEED}.
     */
    public RecordCompressor() {
        
        this(Deflater.BEST_SPEED);
        
    }

    /**
     * Compresses data onto the output stream.
     * 
     * @param bin
     *            The data. The data from the position to the limit will be
     *            compressed. The position will be advanced to the limit as a
     *            side effect.
     * @param os
     *            The stream onto which the compressed data are written.
     */
    public void compress(ByteBuffer bin, OutputStream os) {

        if(bin.hasArray() && bin.position()==0 && bin.limit() == bin.capacity()) {

            /*
             * The source buffer is backed by an array so we delegate using the
             * position() and limit() of the source buffer and the backing
             * array.
             */
            
            compress(bin.array(),bin.position(),bin.limit(),os);
            
            // Advance the position to the limit.
            bin.position(bin.limit());
            
        } else {

            /*
             * Figure out how much data needs to be written.
             */
            int size = bin.remaining();

            /*
             * If the shared buffer is not large enough then reallocate it as a
             * sufficiently large buffer.
             */
            if (_buf.length < size) {

                _buf = new byte[size];

            }

            /*
             * Copy the data from the ByteBuffer into the shared instance
             * buffer.
             */
            bin.get(_buf, 0, size);

            /*
             * Compress the data onto the output stream.
             */
            compress(_buf, 0, size, os);

        }

    }

    /**
     * Compresses data onto the output stream.
     * 
     * @param bytes
     *            The data.
     * @param os
     *            The stream onto which the compressed data are written.
     */
    public void compress(byte[] bytes,OutputStream os) {
        
        compress(bytes,0,bytes.length,os);
        
    }
    
    /**
     * Compresses data onto the output stream.
     * 
     * @param bytes
     *            The source data.
     * @param off
     *            The offset of the first source byte that will be compressed
     *            onto the output stream.
     * @param len
     *            The #of source bytes that will be compressed onto the output
     *            stream.
     * @param os
     *            The stream onto which the compressed data are written.
     */
    public void compress(byte[] bytes, int off, int len, OutputStream os) {

        _deflater.reset(); // required w/ instance reuse.

        DeflaterOutputStream dos = new DeflaterOutputStream(os, _deflater);

        try {

            /*
             * Write onto deflator that writes onto the output stream.
             */
            dos.write(bytes, off, len);

            /*
             * Flush and close the deflator instance.
             * 
             * Note: The caller is unable to do this as they do not have access
             * to the {@link Deflator}. However, if this flushes through to the
             * underlying sink then that could drive IOs with the application
             * being aware that synchronous IO was occurring.
             */
            dos.flush();

            dos.close();

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

    }

    /**
     * Decompresses a {@link ByteBuffer} containing the record and return the
     * uncompressed state.
     * 
     * @param bin
     *            The compressed data.
     * 
     * @return A view onto a shared buffer. The data between position() and
     *         limit() are the decompressed data. The contents of this buffer
     *         are valid only until the next compression or decompression
     *         request.
     */
    public ByteBuffer decompress(ByteBuffer bin) {

        _inflater.reset(); // reset required by reuse.

        final int size = bin.limit();

        InflaterInputStream iis = new InflaterInputStream(
                new ByteBufferInputStream(bin), _inflater, size);

        return decompress(iis);

    }

    /**
     * Decompress a <code>byte[]</code> containing the record and return the
     * uncompressed state.
     * 
     * @param bin
     *            The compressed data.
     * 
     * @return A read-only view onto a shared buffer. The data between
     *         position() and limit() are the decompressed data. The contents of
     *         this buffer are valid only until the next compression or
     *         decompression request. The position will be zero. The limit will
     *         be the #of decompressed bytes.
     */
    public ByteBuffer decompress(byte[] bin) {
     
        _inflater.reset(); // reset required by reuse.

        final int size = bin.length;
        
        InflaterInputStream iis = new InflaterInputStream(
                new ByteArrayInputStream(bin), _inflater, size);

        return decompress( iis );
        
    }

    /**
     * This decompresses data into a shared instance byte[]. If the byte[] runs
     * out of capacity then a new byte[] is allocated with twice the capacity,
     * the data is copied into new byte[], and decompression continues. The
     * shared instance byte[] is then returned to the caller. This approach is
     * suited to single-threaded processes that achieve a suitable buffer size
     * and then perform zero allocations thereafter.
     * 
     * @return A read-only view onto a shared buffer. The data between
     *         position() and limit() are the decompressed data. The contents of
     *         this buffer are valid only until the next compression or
     *         decompression request. The position will be zero. The limit will
     *         be the #of decompressed bytes.
     */
    protected ByteBuffer decompress(InflaterInputStream iis) {

        int off = 0;
        
        try {

            while (true) { // use bulk I/O.

                int capacity = _buf.length - off;

                if (capacity == 0) {

                    byte[] tmp = new byte[_buf.length * 2];

                    System.arraycopy(_buf, 0, tmp, 0, off);

                    _buf = tmp;

                    capacity = _buf.length - off;

                }

                int nread = iis.read(_buf, off, capacity);

                if (nread == -1)
                    break; // EOF.

                off += nread;

            }

        }

        catch (IOException ex) {

            throw new RuntimeException(ex);

        }
//
//      /*
//      * make an exact fit copy of the uncompressed data and return it to
//      * the caller.
//      */
//
//     byte[] tmp = new byte[off];
//
//     System.arraycopy(_buf, 0, tmp, 0, off);
//
////     return tmp;
//        return ByteBuffer.wrap(tmp, 0, off);

        return ByteBuffer.wrap(_buf, 0, off).asReadOnlyBuffer();

    }

}
