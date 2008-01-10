/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Jan 18, 2007
 */

package com.bigdata.rdf.util;

import java.util.Locale;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.XMLSchema;

import com.bigdata.btree.IKeyBuilder;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.rdf.model.OptimizedValueFactory._Literal;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.rdf.store.ITermIndexCodes;

/**
 * Helper class for building unsigned byte[] keys for RDF {@link Value}s and
 * statements. In general, keys for RDF values are formed by a leading byte that
 * indicates the type of the value (URI, BNode, or some type of Literal),
 * followed by the components of that value type.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RdfKeyBuilder implements ITermIndexCodes {

    public final IKeyBuilder keyBuilder;
    
    /**
     * Note: You can pass a {@link KeyBuilder} if you need a light-weight
     * instance of this class for generating keys for the ids index or the
     * statement indices. However, if you need to generate keys for the terms
     * index then you MUST provide an instance of the {@link KeyBuilder}
     * configured for Unicode as appropriate to your {@link Locale}.
     * 
     * @param keyBuilder
     * 
     * @see KeyBuilder
     */
    public RdfKeyBuilder(IKeyBuilder keyBuilder) {
        
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
    
//    public byte[] uriStartKey() {
//        
//        return keyBuilder.reset().append(TERM_CODE_URI).getKey();
//        
//    }
//
//    public byte[] uriEndKey() {
//        
//        return keyBuilder.reset().append(TERM_CODE_LIT).getKey();
//        
//    }

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

//    /**
//     * The key corresponding to the start of the literals section of the
//     * terms index.
//     */
//    public byte[] litStartKey() {
//        
//        return keyBuilder.reset().append(TERM_CODE_LIT).getKey();
//        
//    }
//
//    /**
//     * The key corresponding to the first key after the literals section of the
//     * terms index.
//     */
//    public byte[] litEndKey() {
//        
//        return keyBuilder.reset().append(TERM_CODE_BND).getKey();
//        
//    }

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
        
        return keyBuilder.reset().append(id).getKey();
        
    }
    
    /**
     * Decodes the term identifier key to a term identifier.
     * 
     * @param key The key for an entry in the id:term index.
     * 
     * @return The term identifier.
     */
    public long key2Id(byte[]key) {
        
        return KeyBuilder.decodeLong(key, 0);

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
     * <p>
     * Note: When an identifier is {@link IRawTripleStore#NULL} we can generate
     * a shorter key by not including the NULL value.  This should be fine since
     * identifiers SHOULD NOT be NULL unless they are in the tail position(s) of
     * a triple pattern.  Such keys are always used for rangeCount or rangeQuery
     * purposes where the additional length does not matter (unless it interacts
     * with how we choose to compact the keys for RPC calls).
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
        
        return keyBuilder.reset().append(id1).append(id2).append(id3).getKey();
        
    }
    
    /**
     * Forms the statement key.
     * 
     * @param keyOrder
     *            The key order.
     * @param spo
     *            The statement.
     *            
     * @return The key.
     */
    public byte[] statement2Key(KeyOrder keyOrder, SPO spo) {
        
        switch(keyOrder) {
        case SPO:
            return statement2Key(spo.s, spo.p, spo.o);
        case POS:
            return statement2Key(spo.p, spo.o, spo.s);
        case OSP:
            return statement2Key(spo.o, spo.s, spo.p);
        default:
            throw new UnsupportedOperationException("keyOrder=" + keyOrder);
        }
        
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
    
//    public byte[] pred2Key(long id1, long id2, long id3) {
//        
//        return keyBuilder.reset().append(CODE_PRED).append(id1).append(id2)
//                .append(id3).getKey();
//        
//    }
//    
//    public byte[] rule2Key(long id1, long id2, long id3) {
//        
//        return keyBuilder.reset().append(CODE_RULE).append(id1).append(id2)
//                .append(id3).getKey();
//        
//    }
    
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
     * 
     * @see SPO#SPO(KeyOrder, byte[], Object)
     * 
     * @deprecated The logic for this is now in
     *             {@link SPO#SPO(KeyOrder, byte[], Object)}. This way we do
     *             not have to pass in the ids[].  The distinct term scan code
     *             also does this itself in order to get the 1st term id out of
     *             the key.
     */
    static public void key2Statement(byte[] key, long[] ids) {
        
        assert key != null;
        assert ids != null;
//        assert key.length == 8 * 3 + 1;
        assert key.length == 8 * 3;
        assert ids.length == 3;
        
//        byte code = KeyBuilder.decodeByte(key[0]);
//
//        ids[0] = KeyBuilder.decodeLong(key, 1);
//        
//        ids[1] = KeyBuilder.decodeLong(key, 1+8);
//        
//        ids[2] = KeyBuilder.decodeLong(key, 1+8+8);
        
        ids[0] = KeyBuilder.decodeLong(key, 0);
        
        ids[1] = KeyBuilder.decodeLong(key, 8);
        
        ids[2] = KeyBuilder.decodeLong(key, 8+8);
        
//        return code;
        
    }
    
}
