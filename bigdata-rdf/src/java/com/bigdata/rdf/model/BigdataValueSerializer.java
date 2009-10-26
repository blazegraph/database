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
 * Created on Aug 12, 2008
 */

package com.bigdata.rdf.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.CognitiveWeb.extser.ShortPacker;
import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;

import com.bigdata.io.DataInputBuffer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.rdf.lexicon.ITermIndexCodes;

/**
 * Helper class provides efficient stand-off serialization of RDF {@link Value}
 * objects.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BigdataValueSerializer<V extends Value> {

    /*
     * Serialization.
     */
    
    /**
     * Version zero(0) of the serialization format.
     */
    protected static final short VERSION0 = 0x0;
    
    /**
     * Error message indicates that the version code in the serialized
     * record did not correspond to a known serialization version for an RDF
     * value.
     */
    protected static final String ERR_VERSION = "Bad version";
    
    /**
     * Error message indicates that the term code in the serialized record
     * did not correspond to a known term code for an RDF value of the
     * appropriate type (e.g., a URI code where an Literal code was
     * expected). The codes are defined by {@link ITermIndexCodes}.
     */
    protected static final String ERR_CODE = "Bad term code";

    /**
     * Factory used to de-serialize {@link Value}s.
     */
    private final ValueFactory valueFactory;
    
    /**
     * Create an instance that will materialize objects using the caller's
     * factory.
     * 
     * @param valueFactory
     *            The value factory.
     */
    public BigdataValueSerializer(final ValueFactory valueFactory) {

        if (valueFactory == null)
            throw new IllegalArgumentException();
        
        this.valueFactory = valueFactory;
        
    }

    /**
     * Routine for efficient serialization of an RDF {@link Value}.
     * 
     * @return The byte[] containing the serialized data record.
     * 
     * @throws RuntimeException
     *             if there is a IO problem
     * 
     * @see {@link #deserialize(byte[])}
     */
    public byte[] serialize(final V val) {
        
        final DataOutputBuffer out = new DataOutputBuffer(128);

        return serialize(val, out);
        
    }
    
    /**
     * Variant which permits reuse of the same buffer. This has the
     * advantage that the buffer is reused on each invocation and swiftly
     * grows to its maximum extent.
     * 
     * @param out
     *            The buffer - the caller is responsible for resetting the
     *            buffer before each invocation.
     * 
     * @return The byte[] containing the serialized data record. This array
     *         is newly allocated so that a series of invocations of this
     *         method return distinct byte[]s.
     */
    public byte[] serialize(final V val, final DataOutputBuffer out) {
        
        try {

            final short version = VERSION0;

            ShortPacker.packShort(out, version);

            final byte termCode = getTermCode(val);

            /*
             * Note: VERSION0 writes the termCode immediately after the
             * packed version identifier. Other versions MAY do something
             * else.
             */
            out.writeByte(termCode);

            /*
             * FIXME There are inefficiencies in the DataOutputBuffer when
             * writing UTF8. See if we can work around those using the ICU
             * package. The issue is documented in the DataOutputBuffer
             * class.
             */
            serialize(val, version, termCode, out);

            return out.toByteArray();

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }
                    
    }
    
    /**
     * Routine for efficient de-serialization of an RDF {@link Value}.
     * <p>
     * Note: This automatically uses the {@link BigdataValueFactoryImpl} to create
     * the {@link BigdataValue}s from the de-serialized state so the factory
     * reference is always set on the returned {@link BigdataValueImpl}.
     * 
     * @param b
     *            The byte[] containing the serialized data record.
     * 
     * @return The {@link BigdataValue}.
     * 
     * @throws RuntimeException
     *             if there is an IO problem.
     * 
     * @see {@link #serialize()}
     */
    public V deserialize(final byte[] b) {

        return deserialize(new DataInputBuffer(b));
        
    }

    /**
     * Routine for efficient de-serialization of a {@link BigdataValue}.
     * <p>
     * Note: This automatically uses the {@link BigdataValueFactoryImpl} to create
     * the {@link BigdataValue}s from the de-serialized state so the factory
     * reference is always set on the returned {@link BigdataValueImpl}.
     * 
     * @param b
     *            An input stream from which the serialized data may be read.
     * 
     * @return The {@link BigdataValue}.
     * 
     * @throws RuntimeException
     *             if there is an IO problem.
     * 
     * @see {@link #serialize()}
     */
    public V deserialize(final DataInputBuffer in) {
        
        try {

            final short version = in.unpackShort();

            if (version != VERSION0) {

                throw new RuntimeException(ERR_VERSION + " : " + version);

            }

            /*
             * Note: The term code immediately follows the packed version
             * code for VERSION0 - this is not necessarily true for other
             * serialization versions.
             */

            final byte termCode = in.readByte();

            return deserialize(VERSION0, termCode, in);

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }
         
    }
    
    /**
     * Implements the serialization of a Literal, URI, or BNode.
     * 
     * @param version
     *            The serialization version number (which has already been
     *            written on <i>out</i> by the caller).
     * @param termCode
     *            The byte encoding the type of term as defined by
     *            {@link ITermIndexCodes} (this has already been written on
     *            <i>out</i> by the caller).
     * @param out
     *            The data are written here.
     * 
     * @throws IOException
     */
    protected void serialize(final V val, final short version,
            final byte termCode, final DataOutput out) throws IOException {
    
        switch(termCode) {
 
        case ITermIndexCodes.TERM_CODE_BND: {
            
            if (true) {

                /*
                 * Note: disabled since we never write the BNode as a value in
                 * the id:term index because BNodes IDs are only consistent, not
                 * stable.
                 */

                throw new UnsupportedOperationException();

            }

            out.writeUTF(((BNode) val).getID());

            break;
        
        }

        case ITermIndexCodes.TERM_CODE_URI: {
            
            // Serialize as UTF.
            out.writeUTF(((URI)val).stringValue());

            break;
            
        }
        
        case ITermIndexCodes.TERM_CODE_LIT:
            
            out.writeUTF(((Literal)val).getLabel());
            
            break;
        
        case ITermIndexCodes.TERM_CODE_LCL:

            /*
             * Note: This field is ASCII [A-Za-z0-9] and "-". However, this
             * method writes using UTF-8 so it will generate one byte per
             * character and it is probably more work to write the data
             * directly as ASCII bytes.
             */
            
            out.writeUTF(((Literal)val).getLanguage());
            
            out.writeUTF(((Literal)val).getLabel());
            
            break;
        
        case ITermIndexCodes.TERM_CODE_DTL:
            
            out.writeUTF(((Literal)val).getDatatype().stringValue());

            out.writeUTF(((Literal)val).getLabel());

            break;

        default:
        
            throw new IOException(ERR_CODE + " : " + termCode);
        
        }

    }
    
//  Note: These are commented out since we face a "read-replace" problem if we
//  try to implement Externalizable in a stand-off class.
//    
//    /**
//     * Helper for {@link Externalizable}.
//     * @param val
//     * @param out
//     * @throws IOException
//     */
//    void writeExternal(BigdataValue val, ObjectOutput out) throws IOException {
//    
//        final byte termCode = getTermCode(val);
//        
//        out.writeByte(termCode);
//        
//        serialize(val, VERSION0, termCode, out);
//        
//    }
//
//    /**
//     * Helper for {@link Externalizable}.
//     * @param in
//     * @return
//     * @throws IOException
//     */
//    BigdataValue readExternal(ObjectInput in) throws IOException {
//        
//        final byte termCode = in.readByte();
//        
//        return deserialize(VERSION0, termCode, in);
//        
//    }
    
    /**
     * Implements the de-serialization of a Literal, URI, or BNode.
     * <p>
     * Note: This automatically uses the {@link BigdataValueFactoryImpl} to create
     * the {@link BigdataValue}s from the de-serialized state so the factory
     * reference is always set on the returned {@link BigdataValueImpl}.
     * 
     * @param version
     *            The serialization version number (which has already been read
     *            by the caller).
     * @param termCode
     *            The byte encoding the type of term as defined by
     *            {@link ITermIndexCodes} (this has already been read by the
     *            caller).
     * @param in
     *            The data are read from here.
     * 
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    protected V deserialize(final short version, final byte termCode,
            final DataInput in) throws IOException {
    
        switch(termCode) {
        
        case ITermIndexCodes.TERM_CODE_BND: {
            
            if(true) {
                
                /*
                 * Note: disabled since we never write the BNode as a value in
                 * the id:term index because BNodes IDs are only consistent, not
                 * stable.
                 */

                throw new UnsupportedOperationException();

            }

            assert termCode == ITermIndexCodes.TERM_CODE_BND;

            return (V)valueFactory.createBNode(in.readUTF());

        }

        case ITermIndexCodes.TERM_CODE_URI: {

            return (V)valueFactory.createURI(in.readUTF());

        }

        case ITermIndexCodes.TERM_CODE_LIT: {

            final String label = in.readUTF();

            return (V)valueFactory.createLiteral(label);

        }
        
        case ITermIndexCodes.TERM_CODE_LCL: {

            final String language = in.readUTF();
            final String label = in.readUTF();

            return (V)valueFactory.createLiteral(label, language);
        }
        
        case ITermIndexCodes.TERM_CODE_DTL: {

            final String datatype = in.readUTF();

            final String label = in.readUTF();

            return (V) valueFactory.createLiteral(label, valueFactory
                    .createURI(datatype));
            
        }

        default:

            throw new IOException(ERR_CODE + " : " + termCode);

        }
        
    }
    
    /**
     * Return the term code as defined by {@link ITermIndexCodes} for this type
     * of term. This is used to places URIs, different types of literals, and
     * bnodes into disjoint parts of the key space for sort orders.
     * 
     * @see ITermIndexCodes
     */
    protected byte getTermCode(final Value val) {
        
        if (val == null)
            throw new IllegalArgumentException();
        
        if (val instanceof URI) {
        
            return ITermIndexCodes.TERM_CODE_URI;
            
        } else if (val instanceof Literal) {
            
            final Literal lit = (Literal) val;
            
            if (lit.getLanguage() != null)
                return ITermIndexCodes.TERM_CODE_LCL;

            if (lit.getDatatype() != null)
                return ITermIndexCodes.TERM_CODE_DTL;

            return ITermIndexCodes.TERM_CODE_LIT;

        } else if (val instanceof BNode) {

            return ITermIndexCodes.TERM_CODE_BND;
            
        } else {
            
            throw new IllegalArgumentException("class="+val.getClass().getName());
            
        }

    }

}
