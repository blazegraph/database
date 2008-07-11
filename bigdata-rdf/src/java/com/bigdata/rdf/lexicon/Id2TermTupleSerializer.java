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
 * Created on Jul 7, 2008
 */

package com.bigdata.rdf.lexicon;

import org.openrdf.model.Value;

import com.bigdata.btree.ASCIIKeyBuilderFactory;
import com.bigdata.btree.DefaultTupleSerializer;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;

/**
 * Encapsulates key and value formation for the reverse lexicon index.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Id2TermTupleSerializer extends DefaultTupleSerializer {

    /**
     * 
     */
    private static final long serialVersionUID = 4841769875819006615L;

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
    public Id2TermTupleSerializer(ASCIIKeyBuilderFactory keyBuilderFactory) {
        
        super(keyBuilderFactory);
        
        assert keyBuilderFactory.getInitialCapacity() == Bytes.SIZEOF_LONG;
        
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

        final byte[] key = tuple.getValueBuffer().array();

        final long id = KeyBuilder.decodeLong(key, 0);

        return id;

    }

    /**
     * Return the unsigned byte[] key for a term identifier.
     * 
     * @param obj
     *            The term identifier as a {@link Long}.
     */
    public byte[] serializeKey(Object obj) {

        return id2key(((Long) obj).longValue());
        
    }

    /**
     * Return the byte[] value, which is the serialization of an RDF
     * {@link Value}.
     * 
     * @param obj
     *            An RDF {@link Value}.
     * 
     * FIXME Serialization is tightly linked to the {@link _Value} object - it
     * should also accept {@link BigdataValue}.
     */
    public byte[] serializeVal(Object obj) {
        
        buf.reset();
        
        return ((_Value)obj).serialize(buf);
        
    }

    /**
     * De-serializes the {@link ITuple} as a {@link BigdataValue}, including
     * the term identifier extracted from the unsigned byte[] key.
     * 
     * FIXME De-serialization is tightly linked to the {@link _Value} object
     * rather than {@link BigdataValue}.
     */
    public _Value deserialize(ITuple tuple) {

        final long id = deserializeKey(tuple);
        
        final _Value tmp = _Value.deserialize(tuple.getValueStream());
        
        tmp.setTermId(id);
        
        return tmp;
        
    }

}
