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

import org.openrdf.model.Value;

import com.bigdata.btree.DefaultTupleSerializer;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.keys.ASCIIKeyBuilderFactory;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.raba.codec.SimpleRabaCoder;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.rdf.model.BigdataValueImpl;
import com.bigdata.rdf.model.BigdataValueSerializer;

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
    transient private BigdataValueSerializer<BigdataValueImpl> valueSer;
    
    /**
     * Used to serialize RDF {@link Value}s.
     * <p>
     * Note: While this object is not thread-safe, the mutable B+Tree is
     * restricted to a single writer so it does not have to be thread-safe.
     */
    final transient private DataOutputBuffer buf = new DataOutputBuffer(Bytes.SIZEOF_LONG);

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
    public Id2TermTupleSerializer(final String namespace) {
        
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

        this.valueSer = BigdataValueFactoryImpl.getInstance(namespace)
                .getValueSerializer();

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
        
        return valueSer.serialize((BigdataValueImpl)obj, buf);

    }

    /**
     * De-serializes the {@link ITuple} as a {@link BigdataValue}, including
     * the term identifier extracted from the unsigned byte[] key, and sets
     * the appropriate {@link BigdataValueFactoryImpl} reference on that object.
     */
    public BigdataValue deserialize(final ITuple tuple) {

        final long id = deserializeKey(tuple);

        final BigdataValue tmp = valueSer.deserialize(tuple.getValueStream());

        tmp.setTermId(id);

        return tmp;

    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        
        super.readExternal(in);
        
        namespace = in.readUTF();

        valueSer = BigdataValueFactoryImpl.getInstance(namespace).getValueSerializer();
        
    }
    
    public void writeExternal(ObjectOutput out) throws IOException {

        super.writeExternal(out);
        
        out.writeUTF(namespace);
        
    }
    
}
