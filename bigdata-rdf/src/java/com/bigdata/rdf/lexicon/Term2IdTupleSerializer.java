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
import java.util.Locale;
import java.util.Properties;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.XMLSchema;

import com.bigdata.btree.DefaultTupleSerializer;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.keys.DefaultKeyBuilderFactory;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.IKeyBuilderFactory;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.keys.KeyBuilder.StrengthEnum;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.model.OptimizedValueFactory._Literal;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;

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
     * Used to serialize term identifers.
     * <p>
     * Note: While this object is not thread-safe, the mutable B+Tree is
     * restricted to a single writer so it does not have to be thread-safe.
     */
    final transient private DataOutputBuffer idbuf = new DataOutputBuffer(Bytes.SIZEOF_LONG);
    
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
    public Term2IdTupleSerializer(Properties properties) {
        
        this(new DefaultKeyBuilderFactory(properties));
        
    }

    /**
     * Uses the caller's {@link IKeyBuilderFactory}.
     * 
     * @param keyBuilderFactory
     */
    public Term2IdTupleSerializer(IKeyBuilderFactory keyBuilderFactory) {
        
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
     *            A term identifier expressed as a {@link Long}.
     */
    public byte[] serializeVal(Object obj) {

        try {
            
            idbuf.reset().packLong((Long)obj);
        
            return idbuf.toByteArray();
            
        } catch(IOException ex) {
            
            throw new RuntimeException(ex);
            
        }
        
    }

    /**
     * De-serializes the {@link ITuple} as a {@link Long} whose value is the
     * term identifier associated with the key. The key itself is not decodable.
     */
    public Long deserialize(ITuple tuple) {

        try {
            
            return Long.valueOf(tuple.getValueStream().unpackLong());
            
        } catch (IOException e) {
            
            throw new RuntimeException(e);
            
        }
        
    }

    /**
     * Flyweight helper class for building (and decoding to the extent possible)
     * unsigned byte[] keys for RDF {@link Value}s and term identifiers. In
     * general, keys for RDF values are formed by a leading byte that indicates
     * the type of the value (URI, BNode, or some type of Literal), followed by
     * the components of that value type.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class LexiconKeyBuilder implements ITermIndexCodes {

        public final IKeyBuilder keyBuilder;
        
        /**
         * Normally invoked by {@link Term2IdTupleSerializer#getLexiconKeyBuilder()}
         * 
         * @param keyBuilder
         *            The {@link IKeyBuilder} that will determine the
         *            distinctions and sort order among the rdf {@link Value}s.
         *            In general, this should support Unicode and should use
         *            {@link StrengthEnum#Identical} so that all distinctions in
         *            the {@link Value} space are recognized by the lexicon.
         * 
         * @see IKeyBuilder
         * @see IKeyBuilderFactory
         */
        protected LexiconKeyBuilder(IKeyBuilder keyBuilder) {
            
            this.keyBuilder = keyBuilder;
            
        }
        
        /**
         * Returns the sort key for the URI.
         * 
         * @param uri
         *            The URI.
         *            
         * @return The sort key.
         */
        public byte[] uri2key(String uri) {
            
            return keyBuilder.reset().append(TERM_CODE_URI).append(uri).getKey();
            
        }
        
//        public byte[] uriStartKey() {
//            
//            return keyBuilder.reset().append(TERM_CODE_URI).getKey();
//            
//        }
    //
//        public byte[] uriEndKey() {
//            
//            return keyBuilder.reset().append(TERM_CODE_LIT).getKey();
//            
//        }

        public byte[] plainLiteral2key(String text) {
            
            return keyBuilder.reset().append(TERM_CODE_LIT).append(text).getKey();
            
        }
        
        /**
         * Note: The language code is serialized as US-ASCII UPPER CASE for the
         * purposes of defining the total key ordering. The character set for the
         * language code is restricted to [A-Za-z0-9] and "-" for separating subtype
         * codes. The RDF store interprets an empty language code as NO language
         * code, so we require that the languageCode is non-empty here. The language
         * code specifications require that the language code comparison is
         * case-insensitive, so we force the code to upper case for the purposes of
         * comparisons.
         * 
         * @see _Literal#language
         */
        public byte[] languageCodeLiteral2key(String languageCode, String text) {
            
            assert languageCode.length() > 0;
            
            keyBuilder.reset().append(TERM_CODE_LCL);
            
            keyBuilder.appendASCII(languageCode.toUpperCase()).appendNul();
            
            return keyBuilder.append(text).getKey();
            
        }

        /**
         * Formats a datatype literal sort key.  The value is formated according to
         * the datatype URI.
         * 
         * @param datatype
         * @param value
         * @return
         * 
         * FIXME Handle all of the basic data types.
         * 
         * @todo handle unknown datatypes, perhaps by inheritance from some basic
         * classes.
         * 
         * @todo optimize selection of the value encoding.
         * 
         * @todo handle things like xsd:int vs xsd:Integer correctly.
         */
        public byte[] datatypeLiteral2key(String datatype, String value) {
            
            keyBuilder.reset().append(TERM_CODE_DTL).append(datatype).appendNul();

            if(datatype.equals(XMLSchema.INT) || datatype.equals(XMLSchema.INTEGER)) {
                
                keyBuilder.append(Integer.parseInt(value));
                
            } else if(datatype.equals(XMLSchema.LONG)) {
                    
                keyBuilder.append(Long.parseLong(value));
                    
            } else if(datatype.equals(XMLSchema.FLOAT)) {
                
                keyBuilder.append(Float.parseFloat(value));

            } else if(datatype.equals(XMLSchema.DOUBLE)) {
                    
                    keyBuilder.append(Double.parseDouble(value));
                
            } else if(datatype.equals(RDF.XMLLITERAL)) {
                
                keyBuilder.append(value);
                
            } else {
                
                keyBuilder.append(value);
                
            }
            
            return keyBuilder.getKey();
            
        }

//        /**
//         * The key corresponding to the start of the literals section of the
//         * terms index.
//         */
//        public byte[] litStartKey() {
//            
//            return keyBuilder.reset().append(TERM_CODE_LIT).getKey();
//            
//        }
    //
//        /**
//         * The key corresponding to the first key after the literals section of the
//         * terms index.
//         */
//        public byte[] litEndKey() {
//            
//            return keyBuilder.reset().append(TERM_CODE_BND).getKey();
//            
//        }

        public byte[] blankNode2Key(String id) {
            
            return keyBuilder.reset().append(TERM_CODE_BND).append(id).getKey();
            
        }

        /**
         * Return an unsigned byte[] that locates the value within a total ordering
         * over the RDF value space.
         * 
         * @param value
         *            An RDF value.
         * 
         * @return The sort key for that RDF value.
         * 
         * @todo can this be optimized for (and possibly restricted to) the case
         *       where the {@link Value} is a {@link _Value}?
         */
        public byte[] value2Key(Value value) {

            if (value instanceof URI) {

                URI uri = (URI) value;

                String term = uri.toString();

                return uri2key(term);

            } else if (value instanceof Literal) {

                final Literal lit = (Literal) value;

                final String text = lit.getLabel();
                
                final String languageCode = lit.getLanguage();
                
                final URI datatypeUri = lit.getDatatype();
                
                if ( languageCode != null) {

                    /*
                     * language code literal.
                     */
                    return languageCodeLiteral2key(languageCode, text);

                } else if (datatypeUri != null) {

                    /*
                     * datatype literal.
                     */
                    return datatypeLiteral2key(datatypeUri.toString(), text);
                    
                } else {
                    
                    /*
                     * plain literal.
                     */
                    return plainLiteral2key(text);
                    
                }

            } else if (value instanceof BNode) {

                /*
                 * @todo if we know that the bnode id is a UUID that we generated
                 * then we should encode that using faster logic that this unicode
                 * conversion and stick the sort key on the bnode so that we do not
                 * have to convert UUID to id:String to key:byte[].
                 */
                final String bnodeId = ((BNode)value).getID();
                
                return blankNode2Key(bnodeId);

            } else {

                throw new AssertionError();

            }

        }
                
    }

}
