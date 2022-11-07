package com.bigdata.rdf.lexicon;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.XMLSchema;

import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.IKeyBuilderFactory;
import com.bigdata.btree.keys.StrengthEnum;

/**
 * Flyweight helper class for building (and decoding to the extent possible)
 * unsigned byte[] keys for RDF {@link Value}s and term identifiers. In general,
 * keys for RDF values are formed by a leading byte that indicates the type of
 * the value (URI, BNode, or some type of Literal), followed by the components
 * of that value type.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class LexiconKeyBuilder implements ITermIndexCodes {

    public final IKeyBuilder keyBuilder;

    /**
     * Normally invoked by {@link Term2IdTupleSerializer#getLexiconKeyBuilder()}
     * 
     * @param keyBuilder
     *            The {@link IKeyBuilder} that will determine the distinctions
     *            and sort order among the rdf {@link Value}s. In general, this
     *            should support Unicode and should use
     *            {@link StrengthEnum#Identical} so that all distinctions in the
     *            {@link Value} space are recognized by the lexicon.
     * 
     * @see IKeyBuilder
     * @see IKeyBuilderFactory
     */
    protected LexiconKeyBuilder(final IKeyBuilder keyBuilder) {

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
    public byte[] uri2key(final String uri) {

        return keyBuilder.reset().append(TERM_CODE_URI).append(uri).getKey();

    }

    // public byte[] uriStartKey() {
    //            
    // return keyBuilder.reset().append(TERM_CODE_URI).getKey();
    //            
    // }
    //
    // public byte[] uriEndKey() {
    //            
    // return keyBuilder.reset().append(TERM_CODE_LIT).getKey();
    //            
    // }

    public byte[] plainLiteral2key(final String text) {

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
     * @see Literal#getLanguage()
     */
    public byte[] languageCodeLiteral2key(final String languageCode,
            final String text) {

        assert languageCode.length() > 0;

        keyBuilder.reset().append(TERM_CODE_LCL);

        keyBuilder.appendASCII(languageCode.toUpperCase()).appendNul();

        return keyBuilder.append(text).getKey();

    }

    /**
     * Formats a datatype literal sort key. The value is formated according to
     * the datatype URI.
     * 
     * @param datatype
     * @param value
     * @return
     */
    public byte[] datatypeLiteral2key(final URI datatype, final String value) {

        if (datatype == null)
            throw new IllegalArgumentException();
        
        if (value == null)
            throw new IllegalArgumentException();

        if (datatype.equals(XMLSchema.STRING)) {
            return plainLiteral2key(value);
        }

        /*
         * Note: The full lexical form of the data type URI is serialized into
         * the key as a Unicode sort key followed by a nul byte and then a
         * Unicode sort key formed from the lexical form of the data type value.
         */
        
        // clear out any existing key and add prefix for the DTL space.
        keyBuilder.reset().append(TERM_CODE_DTL);

        // encode the datatype URI as Unicode sort key to make all data
        // types disjoint.
        keyBuilder.append(datatype.stringValue());

        // encode the datatype value as Unicode sort key.
        keyBuilder.append(value);

        keyBuilder.appendNul();

        return keyBuilder.getKey();

    }

    // /**
    // * The key corresponding to the start of the literals section of the
    // * terms index.
    // */
    // public byte[] litStartKey() {
    //            
    // return keyBuilder.reset().append(TERM_CODE_LIT).getKey();
    //            
    // }
    //
    // /**
    // * The key corresponding to the first key after the literals section of
    // the
    // * terms index.
    // */
    // public byte[] litEndKey() {
    //            
    // return keyBuilder.reset().append(TERM_CODE_BND).getKey();
    //            
    // }

    public byte[] blankNode2Key(final String id) {

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
     */
    public byte[] value2Key(final Value value) {

        if (value == null)
            throw new IllegalArgumentException();

        if (value instanceof URI) {

            final URI uri = (URI) value;

            final String term = uri.toString();

            return uri2key(term);

        } else if (value instanceof Literal) {

            final Literal lit = (Literal) value;

            final String text = lit.getLabel();

            final String languageCode = lit.getLanguage();

            final URI datatypeUri = lit.getDatatype();

            if (languageCode != null) {

                /*
                 * language code literal.
                 */
                return languageCodeLiteral2key(languageCode, text);

            } else if (datatypeUri != null) {

                /*
                 * datatype literal.
                 */
                return datatypeLiteral2key(datatypeUri, text);

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
            final String bnodeId = ((BNode) value).getID();

            return blankNode2Key(bnodeId);

        } else {

            throw new AssertionError("Unknown value type: " + value.getClass());

        }

    }

}
