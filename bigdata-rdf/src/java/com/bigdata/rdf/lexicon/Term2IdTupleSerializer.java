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

import java.io.IOException;
import java.util.Properties;
import org.openrdf.model.Value;
import com.bigdata.btree.DefaultTupleSerializer;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.keys.DefaultKeyBuilderFactory;
import com.bigdata.btree.keys.IKeyBuilderFactory;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.internal.TermId;

/**
 * Handles the term:id index (forward mapping for the lexicon). The keys are
 * unsigned byte[]s representing a total order for the RDF {@link Value} space.
 * The index assigns term identifiers, and those term identifiers are stored in
 * the values of the index.
 * 
 * @todo consider storing some additional state in the value for a GC sweep over
 *       the lexicon. alternatively, could handle lexicon compression by
 *       building a new index and only copying those values used in the source
 *       index.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Term2IdTupleSerializer extends DefaultTupleSerializer {

    /**
     * 
     */
    private static final long serialVersionUID = 1486882823994548034L;

    /**
     * Used to serialize term identifiers.
     * <p>
     * Note: While this object is not thread-safe, the mutable B+Tree is
     * restricted to a single writer so it does not have to be thread-safe.
     */
    final transient private DataOutputBuffer idbuf = new DataOutputBuffer(
            Bytes.SIZEOF_LONG);

    /**
     * De-serialization ctor.
     */
    public Term2IdTupleSerializer() {
        
        super();
        
    }
    
    /**
     * Configures the {@link IKeyBuilderFactory} from the caller's <i>properties</i>.
     * 
     * @param properties
     */
    public Term2IdTupleSerializer(final Properties properties) {
        
        this(new DefaultKeyBuilderFactory(properties));
        
    }

    /**
     * Uses the caller's {@link IKeyBuilderFactory}.
     * 
     * @param keyBuilderFactory
     */
    public Term2IdTupleSerializer(final IKeyBuilderFactory keyBuilderFactory) {
        
        super(keyBuilderFactory);
        
    }

//    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
//
//        super.readExternal(in);
//        
//    }
//
//    public void writeExternal(ObjectOutput out) throws IOException {
//
//        super.writeExternal(out);
//        
//    }

    /**
     * Thread-local object for constructing keys for the lexicon.
     */
    public LexiconKeyBuilder getLexiconKeyBuilder() {
        
        return new LexiconKeyBuilder(getKeyBuilder());
        
    }

    /**
     * You can not decode the term:id keys since they include Unicode sort keys
     * and that is a lossy transform.
     * 
     * @throws UnsupportedOperationException
     *             always
     */
    public Object deserializeKey(ITuple tuple) {
        
        throw new UnsupportedOperationException();
        
    }

    /**
     * Return the unsigned byte[] key for an RDF {@link Value}.
     * 
     * @param obj
     *            The RDF {@link Value}.
     */
    public byte[] serializeKey(Object obj) {

        return getLexiconKeyBuilder().value2Key((Value)obj);
        
    }

    /**
     * Return the byte[] value, which is a term identifier written as a packed
     * long integer.
     * 
     * @param obj
     *            A term identifier expressed as a {@link TermId}.
     */
    public byte[] serializeVal(Object obj) {

        try {
            
            IV iv = (IV) obj;

            final byte[] key = iv.encode(KeyBuilder.newInstance()).getKey();
            
            idbuf.reset().write(key);
            
            return idbuf.toByteArray();
            
        } catch(IOException ex) {
            
            throw new RuntimeException(ex);
            
        }
        
    }

    /**
     * De-serializes the {@link ITuple} as a {@link IV} whose value is the
     * term identifier associated with the key. The key itself is not decodable.
     */
    public IV deserialize(ITuple tuple) {

        return IVUtility.decode(tuple.getValue());
        
    }

}
