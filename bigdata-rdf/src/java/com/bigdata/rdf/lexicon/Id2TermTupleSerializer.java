/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Jul 7, 2008
 */

package com.bigdata.rdf.lexicon;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.openrdf.model.Value;

import com.bigdata.btree.DefaultTupleSerializer;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.keys.ASCIIKeyBuilderFactory;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.raba.codec.SimpleRabaCoder;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.rdf.model.BigdataValueSerializer;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Encapsulates key and value formation for the reverse lexicon index.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Id2TermTupleSerializer extends DefaultTupleSerializer<Long, BigdataValue> {

    /**
     * 
     */
    private static final long serialVersionUID = 4841769875819006615L;

    /**
     * The namespace of the owning {@link LexiconRelation}.
     */
    private String namespace;

    /**
     * A (de-)serialized backed by a {@link BigdataValueFactoryImpl} for the
     * {@link #namespace} of the owning {@link LexiconRelation}.
     */
    transient private BigdataValueSerializer<BigdataValue> valueSer;
    
    /**
     * Used to serialize RDF {@link Value}s.
     * <p>
     * Note: While this object is not thread-safe, the mutable B+Tree is
     * restricted to a single writer so it does not have to be thread-safe.
     */
    final transient private DataOutputBuffer buf = new DataOutputBuffer(Bytes.SIZEOF_LONG);

    transient private BigdataValueFactory valueFactory;

    /**
     * De-serialization ctor.
     */
    public Id2TermTupleSerializer() {

        super();
        
    }

    /**
     * 
     * @param keyBuilderFactory
     *            A factory that does not support unicode and has an
     *            initialCapacity of {@value Bytes#SIZEOF_LONG}.
     */
    public Id2TermTupleSerializer(final String namespace,final BigdataValueFactory valueFactory) {
        
        super(//
                new ASCIIKeyBuilderFactory(Bytes.SIZEOF_LONG),//
                getDefaultLeafKeysCoder(),//
//                getDefaultValuesCoder()
                SimpleRabaCoder.INSTANCE
//                CanonicalHuffmanRabaCoder.INSTANCE
//                new ConditionalRabaCoder(//
//                        SimpleRabaCoder.INSTANCE, // small
//                        CanonicalHuffmanRabaCoder.INSTANCE, // large
//                        33/*btreeBranchingFactor+1*/
//                        )
        );

        if (namespace == null)
            throw new IllegalArgumentException();
        
        this.namespace = namespace;
        this.valueFactory=valueFactory;
        this.valueSer = this.valueFactory.getValueSerializer();

    }
    
    /**
     * Converts a long integer that identifies an RDF {@link Value} into a key
     * suitable for use with the id:term index.
     * <p>
     * Note: The code that handles efficient batch insertion of terms into the
     * database replicates the logic for encoding the term identifer as an
     * unsigned long integer.
     * 
     * @param id
     *            The term identifier.
     * 
     * @return The id expressed as an unsigned byte[] key of length 8.
     * 
     * @see #key2Id()
     */
    public byte[] id2key(long id) {
        
        return getKeyBuilder().reset().append(id).getKey();
        
    }
    
    /**
     * Decodes the term identifier key to a term identifier.
     * 
     * @param key
     *            The key for an entry in the id:term index.
     * 
     * @return The term identifier.
     */
    public Long deserializeKey(ITuple tuple) {

        final byte[] key = tuple.getKeyBuffer().array();

        final long id = KeyBuilder.decodeLong(key, 0);

        return id;

    }

    /**
     * Return the unsigned byte[] key for a term identifier.
     * 
     * @param obj
     *            The term identifier as a {@link Long}.
     */
    public byte[] serializeKey(final Object obj) {

        return id2key(((Long) obj).longValue());
        
    }

    /**
     * Return the byte[] value, which is the serialization of an RDF
     * {@link Value}.
     * 
     * @param obj
     *            An RDF {@link Value}.
     */
    public byte[] serializeVal(final BigdataValue obj) {
        
        buf.reset();
        
        return valueSer.serialize(obj, buf);

    }

    /**
     * De-serializes the {@link ITuple} as a {@link BigdataValue}, including
     * the term identifier extracted from the unsigned byte[] key, and sets
     * the appropriate {@link BigdataValueFactoryImpl} reference on that object.
     */
    public BigdataValue deserialize(final ITuple tuple) {

        final long id = deserializeKey(tuple);

        final BigdataValue tmp = valueSer.deserialize(tuple.getValueStream());

        tmp.setIV(id);

        return tmp;

    }

    /**
     * Included only the UTF serialization of the namespace field without
     * explicit version support.
     * 
     * <pre>
     * namespace:UTF
     * </pre>
     */
    static final transient short VERSION0 = 0;

    /**
     * Added the UTF serialization of the class name of the value factory
     * and an explicit version number in the serialization format. This
     * version is detected by a read of an empty string from the original
     * UTF field.
     * 
     * <pre>
     * "":UTF
     * valueFactoryClass:UTF
     * namespace:UTF
     * </pre>
     * 
     * Note: THere are unit tests for this backward compatible serialization
     * change in TestId2TermTupleSerializer.
     */
    static final transient short VERSION1 = 1;

    private static final transient short VERSION = VERSION1;

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        short version = VERSION0;
        String s1 = in.readUTF();
        String s2 = BigdataValueFactoryImpl.class.getName();
        if (s1.length() == 0) {
            version = in.readShort();
            s1 = in.readUTF();
            s2 = in.readUTF();
        }
        switch (version) {
        case VERSION0:
        case VERSION1:
            break;
        default:
            throw new IOException("unknown version=" + version);
        }
        final String namespace = s1;
        final String valueFactoryClass = s2;
        // set the namespace field.
        this.namespace = namespace;
        // resolve the valueSerializer from the value factory class.
        try {
            final Class<?> vfc = Class.forName(valueFactoryClass);
            if (!BigdataValueFactory.class.isAssignableFrom(vfc)) {
                throw new RuntimeException(
                        AbstractTripleStore.Options.VALUE_FACTORY_CLASS
                                + ": Must extend: "
                                + BigdataValueFactory.class.getName());
            }
            final Method gi = vfc.getMethod("getInstance", String.class);
            this.valueFactory = (BigdataValueFactory) gi
                    .invoke(null, namespace);
        } catch (NoSuchMethodException e) {
            throw new IOException(e);
        } catch (InvocationTargetException e) {
            throw new IOException(e);
        } catch (IllegalAccessException e) {
            throw new IOException(e);
        }
        valueSer = this.valueFactory.getValueSerializer();
    }
    
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        final short version = VERSION;
        final String valueFactoryClass = valueFactory.getClass().getName();
        switch (version) {
        case VERSION0:
            out.writeUTF(namespace);
            break;
        case VERSION1:
            out.writeUTF("");
            out.writeShort(version);
            out.writeUTF(namespace);
            out.writeUTF(valueFactoryClass);
            break;
        default:
            throw new AssertionError();
        }
    }

}
