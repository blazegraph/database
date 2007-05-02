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
 * Created on Jan 18, 2007
 */

package com.bigdata.rdf;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.vocabulary.RDF;
import org.openrdf.vocabulary.XmlSchema;

import com.bigdata.btree.IKeyBuilder;
import com.bigdata.btree.KeyBuilder;

/**
 * Helper class for building unsigned byte[] keys for RDF {@link Value}s and
 * statements. In general, keys for RDF values are formed by a leading byte that
 * indicates the type of the value (URI, BNode, or some type of Literal),
 * followed by the components of that value type.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RdfKeyBuilder {

    public final IKeyBuilder keyBuilder;
    
    public RdfKeyBuilder(IKeyBuilder keyBuilder) {
        
        this.keyBuilder = keyBuilder;
        
    }
    
    /**
     * The length of a key for one of the statement indices.  This is one byte
     * for the code followed by 3 long integers.
     */
    final public static int stmtKeyLen = 1 + 8 * 3;
    
    /*
     * Define bytes indicating whether a key in a statement index is a
     * statement, predicate (rule without a body), or a rule with a body. This
     * distinction makes it possible to mix together rules and data in the
     * statement indices.
     */
    
    /** indicates a statement. */
    final public static byte CODE_STMT = 0x01;
    /** indicates a predicate (value is null). */
    final public static byte CODE_PRED = 0x02;
    /** indicates a rule (value is the rule body). */
    final public static byte CODE_RULE = 0x03;
    
    /*
     * Define bytes indicating the type of a term in the term index.
     * 
     * Note: when these signed bytes get encoded as unsigned bytes in a key
     * their values change. For example, 2 becomes 130.
     */
    
    /** indicates a URI. */
    final public static byte CODE_URI = 0x01;

    /** indicates a plain literal. */
    final public static byte CODE_LIT = 0x02;

    /** indicates a literal with a language code. */
    final public static byte CODE_LCL = 0x03;

    /** indicates a literal with a data type URI. */
    final public static byte CODE_DTL = 0x04;

//    /** indicates a XML literal. */
//    final public static byte CODE_XML = 0x05;

    /** indicates a blank node. */
    final public static byte CODE_BND = 0x06;

    /**
     * When true all strings will be <em>assumed</em> to contain 7-bit clean
     * US-ASCII characters and {@link IKeyBuilder#appendASCII(String)} will be
     * used in place of {@link IKeyBuilder#append(String)}.
     * <p>
     * Note: These two processing modes produce incompatible keys and MUST NOT
     * be mixed for an index. The US-ASCII assumption is significantly faster
     * since it avoids all use of unicode aware collation rules and also
     * produces shorter keys.
     */
    final public boolean assumeUSASCII = false;
    
    protected final IKeyBuilder appendString(String s) {
        
        if(assumeUSASCII) {
            
            return keyBuilder.appendASCII(s);
            
        } else {
            
            return keyBuilder.append(s);
            
        }
        
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
        
        keyBuilder.reset().append(CODE_URI);
        return appendString(uri).getKey();
        
    }
    
    public byte[] uriStartKey() {
        
        return keyBuilder.reset().append(CODE_URI).getKey();
        
    }

    public byte[] uriEndKey() {
        
        return keyBuilder.reset().append(CODE_LIT).getKey();
        
    }

    public byte[] plainLiteral2key(String text) {
        
        keyBuilder.reset().append(CODE_LIT);
        
        return appendString(text).getKey();
        
    }
    
    public byte[] languageCodeLiteral2key(String languageCode, String text) {
        
        assert languageCode.length() == 2;
        
        keyBuilder.reset().append(CODE_LCL);
        
        appendString(languageCode).appendNul();
        
        return appendString(text).getKey();
        
    }

    /**
     * Formats a datatype literal sort key.  The value is formated according to
     * the datatype URI.
     * 
     * @param datatype
     * @param value
     * @return
     * 
     * @todo handle unknown datatypes, perhaps by inheritance from some basic
     * classes.
     * 
     * @todo optimize selection of the value encoding.
     * 
     * @todo handle things like xsd:int vs xsd:Integer correctly.
     */
    public byte[] datatypeLiteral2key(String datatype, String value) {
        
        keyBuilder.reset().append(CODE_DTL);

        appendString(datatype).appendNul();

        if(datatype.equals(XmlSchema.INT) || datatype.equals(XmlSchema.INTEGER)) {
            
            keyBuilder.append(Integer.parseInt(value));
            
        } else if(datatype.equals(XmlSchema.LONG)) {
                
            keyBuilder.append(Long.parseLong(value));
                
        } else if(datatype.equals(XmlSchema.FLOAT)) {
            
            keyBuilder.append(Float.parseFloat(value));

        } else if(datatype.equals(XmlSchema.DOUBLE)) {
                
                keyBuilder.append(Double.parseDouble(value));
            
        } else if(datatype.equals(RDF.XMLLITERAL)) {
            
            appendString(value);
            
        } else {
            
            appendString(value);
            
        }
        
        return keyBuilder.getKey();
        
    }

    /**
     * The key corresponding to the start of the literals section of the
     * terms index.
     */
    public byte[] litStartKey() {
        
        return keyBuilder.reset().append(CODE_LIT).getKey();
        
    }

    /**
     * The key corresponding to the first key after the literals section of the
     * terms index.
     */
    public byte[] litEndKey() {
        
        return keyBuilder.reset().append(CODE_BND).getKey();
        
    }

    public byte[] blankNode2Key(String id) {
        
        keyBuilder.reset().append(CODE_BND);
        
        return appendString(id).getKey();
        
    }

    public byte[] value2Key(Value value) {

        if (value instanceof URI) {

            URI uri = (URI) value;

            String term = uri.getURI();

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
                return datatypeLiteral2key(datatypeUri.getURI(), text);
                
            } else {
                
                /*
                 * plain literal.
                 */
                return plainLiteral2key(text);
                
            }

        } else if (value instanceof BNode) {

            /*
             * @todo if we know that the bnode id is a segmentUUID that we generated
             * then we should encode that using faster logic that this unicode
             * conversion and stick the sort key on the bnode so that we do
             * not have to convert UUID to id:String to key:byte[]. 
             */
            final String bnodeId = ((BNode)value).getID();
            
            return blankNode2Key(bnodeId);

        } else {

            throw new AssertionError();

        }

    }
    
    /**
     * Converts a long integer that identifies an RDF {@link Value} into a key
     * suitable for use with the id:term index.
     *  
     * @param id The term identifier.
     * 
     * @return The id expressed as an unsigned byte[] key of length 8.
     */
    public byte[] id2key(long id) {
        
        return keyBuilder.reset().append(id).getKey();
        
    }
    
    /**
     * Encodes a statement represented as three long integers as an unsigned
     * byte[] sort key.
     * <p>
     * Note: while the conversion of long integers into the byte[] is
     * non-trivial the value identifiers are mapped onto 8 bytes at a time and
     * the contents of the array could be rearranged into alternative orders
     * directly. For example, if you provide (s,p,o) then you could form the
     * (p,o,s) key by copying 8 byte sections of the returned sort key around to
     * generate the desired permutation.
     * 
     * @param id1
     *            An RDF value identifier from the term index.
     * @param id2
     *            An RDF value identifier from the term index.
     * @param id3
     *            An RDF value identifier from the term index.
     * 
     * @return The sort key for the statement with those values.
     */
    public byte[] statement2Key(long id1, long id2, long id3) {
        
        return keyBuilder.reset().append(CODE_STMT).append(id1).append(id2)
                .append(id3).getKey();
        
    }

    /*
     * The problem with this method is that it encourages us to reuse a key
     * buffer but the btree (at least when used as part of a local api) requires
     * that we donate the key buffer to the btree.
     */
//    /**
//     * Encodes a statement into the supplied <i>key</i> buffer.
//     * <p>
//     * Note: This can be significantly faster than
//     * {@link #statement2Key(long, long, long)}
//     * 
//     * @param id1
//     * @param id2
//     * @param id3
//     * @param key
//     *            A buffer of length 28 (1 byte for the {@link #CODE_STMT},
//     *            plus three long integers).
//     */
//    public void statement2Key(long id1, long id2, long id3,byte[] key) {
//
//        keyBuilder.reset().append(CODE_STMT).append(id1).append(id2)
//                .append(id3).copyKey(key);
//
//    }
    
    public byte[] pred2Key(long id1, long id2, long id3) {
        
        return keyBuilder.reset().append(CODE_PRED).append(id1).append(id2)
                .append(id3).getKey();
        
    }
    
    public byte[] rule2Key(long id1, long id2, long id3) {
        
        return keyBuilder.reset().append(CODE_RULE).append(id1).append(id2)
                .append(id3).getKey();
        
    }
    
    /**
     * Decodes a statement key.
     * 
     * @param key
     *            A key as encoded by {@link #statement2Key(long, long, long)}
     *            and friends or from one of the statement indices.
     * @param ids
     *            The ids. You have to know the {@link KeyOrder} in order to
     *            figure out which is the subject, predicate, or object.
     * 
     * @return The byte code indicating whether the key was a
     *         {@link #CODE_STMT statement}, {@link #CODE_PRED predicate}, or
     *         {@link #CODE_RULE} rule.
     */
    public byte key2Statement(byte[] key, long[] ids) {
        
        assert key != null;
        assert ids != null;
        assert key.length == 8 * 3 + 1;
        assert ids.length == 3;
        
        byte code = KeyBuilder.decodeByte(key[0]);

        ids[0] = KeyBuilder.decodeLong(key, 1);
        
        ids[1] = KeyBuilder.decodeLong(key, 1+8);
        
        ids[2] = KeyBuilder.decodeLong(key, 1+8+8);
        
        return code;
        
    }
    
}
