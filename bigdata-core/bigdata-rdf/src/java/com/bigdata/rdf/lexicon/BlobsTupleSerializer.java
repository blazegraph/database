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
import com.bigdata.btree.raba.codec.SimpleRabaCoder;
import com.bigdata.io.ByteArrayBuffer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.internal.impl.BlobIV;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.rdf.model.BigdataValueSerializer;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.util.Bytes;

/**
 * Encapsulates key and value formation for the TERMS index.  The keys are
 * {@link BlobIV}s.  The values are {@link BigdataValue}s serialized using the
 * {@link BigdataValueSerializer}.  Large values are converted to raw records
 * and must be materialized before they can be deserialized.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BlobsTupleSerializer extends DefaultTupleSerializer<IV, BigdataValue> {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * The namespace of the owning {@link LexiconRelation}.
     */
    private String namespace;
    
    /**
     * A (de-)serialized backed by a {@link BigdataValueFactoryImpl} for the
     * {@link #namespace} of the owning {@link LexiconRelation}.
     */
    transient private BigdataValueSerializer<BigdataValue> valueSer;

    private static transient final int INITIAL_CAPACITY = 512;
    
    /**
     * Used to serialize RDF {@link Value}s.
     * <p>
     * Note: While this object is not thread-safe, the mutable B+Tree is
     * restricted to a single writer so it does not have to be thread-safe.
     */
    final transient private DataOutputBuffer buf = new DataOutputBuffer(INITIAL_CAPACITY);

    /**
     * Used to serialize RDF {@link Value}s.
     * <p>
     * Note: While this object is not thread-safe, the mutable B+Tree is
     * restricted to a single writer so it does not have to be thread-safe.
     */
    final transient private ByteArrayBuffer tbuf = new ByteArrayBuffer(INITIAL_CAPACITY);

    transient private BigdataValueFactory valueFactory;

    /**
     * De-serialization ctor.
     */
    public BlobsTupleSerializer() {

        super();
        
    }

    /**
     * 
     * @param keyBuilderFactory
     *            A factory that does not support unicode and has an
     *            initialCapacity of {@value Bytes#SIZEOF_LONG}.
     */
    public BlobsTupleSerializer(final String namespace,
            final BigdataValueFactory valueFactory) {
        
        super(//
                new ASCIIKeyBuilderFactory(
                        BlobsIndexHelper.TERMS_INDEX_KEY_SIZE),//
                getDefaultLeafKeysCoder(),//
                // getDefaultValuesCoder()
                SimpleRabaCoder.INSTANCE);

        if (namespace == null)
            throw new IllegalArgumentException();
        
        this.namespace = namespace;
        this.valueFactory = valueFactory;
        this.valueSer = this.valueFactory.getValueSerializer();

    }

    /**
     * Decodes the key to a {@link BlobIV}.
     * 
     * @param key
     *            The key for an entry in the TERMS index.
     * 
     * @return The {@link BlobIV}.
     */
    @Override
    public IV deserializeKey(final ITuple tuple) {

        final byte[] key = tuple.getKeyBuffer().array();

        return IVUtility.decode(key);

    }

    /**
     * Return the unsigned byte[] key for a {@link BlobIV}.
     * 
     * @param obj
     *            The {@link BlobIV}.
     */
    @Override
    public byte[] serializeKey(final Object obj) {

        final BlobIV<?> iv = (BlobIV<?>) obj;

        return iv.encode(getKeyBuilder().reset()).getKey();

    }

    /**
     * Return the byte[] value, which is the serialization of an RDF
     * {@link Value} using the {@link BigdataValueSerializer}.
     * 
     * @param obj
     *            An RDF {@link Value}.
     */
    @Override
    public byte[] serializeVal(final BigdataValue obj) {
        
        return valueSer.serialize(obj, buf.reset(), tbuf);

    }

    /**
     * De-serializes the {@link ITuple} as a {@link BigdataValue}, including
     * the term identifier extracted from the unsigned byte[] key, and sets
     * the appropriate {@link BigdataValueFactoryImpl} reference on that object.
     */
    @Override
    public BigdataValue deserialize(final ITuple tuple) {

//        if(tuple.isNull()) {
//            /*
//             * Note: The NullIV does not have a value in the index.
//             */
//            return null;
//        }
        
        final IV<?,?> iv = deserializeKey(tuple);
        
        final BigdataValue tmp = valueSer.deserialize(tuple.getValueStream(),
                new StringBuilder());

        tmp.setIV(iv);

        return tmp;

    }

    /**
     * <pre>
     * valueFactoryClass:UTF
     * namespace:UTF
     * </pre>
     */
    private static final transient byte VERSION0 = 0;

    private static final transient byte VERSION = VERSION0;

    public void readExternal(final ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readExternal(in);
        final byte version = in.readByte();
        final String namespace;
        final String valueFactoryClass;
        switch (version) {
        case VERSION0:
            namespace = in.readUTF();
            valueFactoryClass = in.readUTF();
            break;
        default:
            throw new IOException("unknown version=" + version);
        }
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

    public void writeExternal(final ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeByte(VERSION);
        out.writeUTF(namespace);
        out.writeUTF(valueFactory.getClass().getName());
    }

}
