package com.bigdata.rdf.lexicon;

import java.util.HashMap;
import java.util.Map;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.XMLSchema;

import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.IKeyBuilderFactory;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.keys.StrengthEnum;

/**
 * Flyweight helper class for building (and decoding to the extent possible)
 * unsigned byte[] keys for RDF {@link Value}s and term identifiers. In general,
 * keys for RDF values are formed by a leading byte that indicates the type of
 * the value (URI, BNode, or some type of Literal), followed by the components
 * of that value type. For datatype literals, there is an additional 32-bit
 * integer encoded into the key which divides the key space into disjoint value
 * spaces for different datatypes. Each disjoint value space encodes the key
 * onto an unsigned byte[] in order to impose a total ordering within that value
 * space. Data type values for float, double, long, int, short, and byte will
 * each be coded into the minimum number of bytes required to represent that
 * value space. Datatypes which are not explicitly handled are placed into an
 * "unknown" value space, their datatype URI is coded into the key, and finally
 * the data type value is coded as a Unicode sort key using their lexical
 * representation without further normalization. Both plain literals and
 * {@link XMLSchema#STRING} literals are mapped onto the value space of plain
 * literals per <a href="http://www.w3.org/TR/rdf-mt/">RDF Semantics</a>.
 * 
 * FIXME Registering the coders breaks some of the SPARQL unit tests for the
 * Sesame TCK. I need to look into these tests and figure out why they are
 * breaking and whether we can use these coders with SPARQL or not. The problem
 * may be that distinct lexical forms are being mapped onto the same point in
 * the code space and assigned the same term identifier. If SPARQL does not
 * allow that and requires that we perform equality testing in the query rather
 * than forcing convergence onto the value space in the lexicon then these
 * coders can not be used if we are to remain in compliance with the
 * specification.
 *<P>
 * We could make the use of the value space coders an option, and allow people
 * whose applications can benefit from this behavior to enable it themselves.
 * <p>
 * If we not going to be able to exploit the key value space then we can
 * simplify both this logic and the coding of the datatype keys, saving the 4
 * bytes per datatype value in the key used to formulate the disjoint value
 * spaces.
 * 
 * @todo Support has not been implemented for unsigned data types.
 * 
 * @todo Support has not been implemented for a variety of other data types.
 * 
 * @todo The total ordering within a value space makes possible certain query
 *       optimizations since strongly typed values of the same data type can be
 *       traversed in order (or within a half-open range) without sorting. In
 *       addition, it is possible to directly decode values from the key for
 *       many of these value types, e.g., float, double, long, int, short and
 *       byte. However, there may be little utility for that feature since the
 *       statements are encoded by term identifiers and the term identifiers for
 *       data type values DO NOT respect the order of the data type itself.
 * 
 * @todo Allow extension of the {@link IDatatypeKeyCoder} factory. The
 *       extensions must assigned a consistent code for the corresponding
 *       "type." Currently, this must be done in the source code and the
 *       registered coders map is static. Alternatively, this could be mediated
 *       by the GRS for the owning {@link LexiconRelation}. Negative code values
 *       are reserved for the application.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class LexiconKeyBuilder implements ITermIndexCodes {

    public final IKeyBuilder keyBuilder;

    /**
     * Interface for classes encapsulating the logic to encode (and where
     * possible, decode) datatype literals.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    public static interface IDatatypeKeyCoder {

        /**
         * Interpret the text as some specific data type and encode a
         * representation of that data type value suitable for an index whose
         * keys are unsigned byte[]s.
         * 
         * @param keyBuilder
         *            The object used to build up the key.
         * @param text
         *            A lexical representation of the value.
         */
        void encode(IKeyBuilder keyBuilder, String text);

        /**
         * Decode a slice of a byte[] containing a key formed by
         * {@link #encode(IKeyBuilder, String)}.
         * 
         * @param key
         *            The byte[].
         * @param off
         *            The first byte in the slice.
         * @param len
         *            The length of the slice.
         * 
         * @return A lexical representation of the decoded value.
         * 
         * @throws UnsupportedOperationException
         *             If the keys for the datatype encoded by this interface
         *             can not be decoded without loss. For example, this is
         *             true of {@link XMLSchema#STRING} when compressed Unicode
         *             sort keys are used.
         * 
         * @todo the [len] parameter is probably not necessary for most things,
         *       but there are some cool key types that are both variable length
         *       and totally ordered. E.g., eXist XML node identifiers.
         */
        String decode(byte[] key, int off, int len);

        /**
         * Return <code>true</code> if the implementation of this interface
         * encodes keys which can be decoded without loss. "Loss" means that it
         * is not possible to decode a value which correspond to the same point
         * in the value space of the datatype. For example, <code>.5</code> and
         * <code>0.5</code> both represent the same point in the
         * {@link XMLSchema#FLOAT} value space. These values are therefore
         * decodable without loss, even though the decoded value might not have
         * the same lexical representation.
         */
        boolean isDecodable();

        /**
         * Return the unique code designated for the primitive data type handled
         * by this coder. Coders which self-report values for this method which
         * are in conflict will be reported by a runtime exception. The
         * appropriate code values are declared by this interface. The primitive
         * datatypes include:
         * 
         * <pre>
         *         3.2.1 string
         *         3.2.2 boolean
         *         3.2.3 decimal
         *         3.2.4 float
         *         3.2.5 double
         *         3.2.6 duration
         *         3.2.7 dateTime
         *         3.2.8 time
         *         3.2.9 date
         *         3.2.10 gYearMonth
         *         3.2.11 gYear
         *         3.2.12 gMonthDay
         *         3.2.13 gDay
         *         3.2.14 gMonth
         *         3.2.15 hexBinary
         *         3.2.16 base64Binary
         *         3.2.17 anyURI
         *         3.2.18 QName
         *         3.2.19 NOTATION
         * </pre>
         * 
         * @see http://www.w3.org/TR/swbp-xsch-datatypes/
         * @see <a ref="http://www.w3.org/TR/2004/REC-xmlschema-2-20041028/#built-in-primitive-datatypes>primitiv
         *      e data types.</a>
         */
        int code();

        /**
         * Code used for unrecognized or otherwise unhandled data types.
         */
        int CODE_OTHER = 0;
        
        int CODE_XSD_STRING = 1;

        int CODE_XSD_BOOLEAN = 2;

        /**
         * Arbitrary magnitude decimal values with arbitrary digits after the
         * decimal.
         */
        int CODE_XSD_DECIMAL = 3;
        
        int CODE_XSD_FLOAT = 4;

        int CODE_XSD_DOUBLE = 5;

        /** @deprecated until good semantics have been developed. */
        int CODE_XSD_DURATION = 6;

        int CODE_XSD_DATETIME = 7;

        int CODE_XSD_TIME = 8;

        int CODE_XSD_DATE = 9;

        int CODE_XSD_GYEARMONTH = 10;

        int CODE_XSD_GYEAR = 11;

        int CODE_XSD_GMONTHDAY = 12;

        int CODE_XSD_HEXBINARY = 13;

        int CODE_XSD_BASE64BINARY = 14;

        int CODE_XSD_ANYURI = 15;

        int CODE_XSD_QNAME = 16;

        int CODE_XSD_NOTATION = 17;

        /*
         * Arbitrary magnitude integers.
         */
        int CODE_XSD_INTEGER = 18;

        /*
         * Various signed fixed width integer types.
         */
        int CODE_XSD_LONG = 32;

        int CODE_XSD_INT = 33;

        int CODE_XSD_SHORT = 34;

        int CODE_XSD_BYTE = 35;

        /*
         * Various unsigned fixed with integer types.
         */
        int CODE_XSD_ULONG = 36;

        int CODE_XSD_UINT = 37;

        int CODE_XSD_USHORT = 38;

        int CODE_XSD_UBYTE = 39;

        /**
         * An {@link RDF#XMLLITERAL}.
         */
        int CODE_XML_LITERAL = 40;
        
    }

    /**
     * Handles anything derived from the primitive data type
     * {@link XMLSchema#BOOLEAN}. All such values are coded in a single byte.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    public static class XSDBooleanCoder implements IDatatypeKeyCoder {

        public static transient final IDatatypeKeyCoder INSTANCE = new XSDBooleanCoder();
        
        public int code() {

            return CODE_XSD_BOOLEAN;

        }

        public String decode(byte[] buf, int off, int len) {

            return KeyBuilder.decodeByte(buf[off]) == 1 ? "true" : "false";

        }

        public void encode(IKeyBuilder keyBuilder, String text) {

            text = text.trim();

            final boolean t;

            if ("true".equalsIgnoreCase(text) || "1".equals(text)) {
            
                t = true;
                
            } else if ("false".equalsIgnoreCase(text) || "0".equals(text)) {
                
                t = false;
                
            } else {
                
                throw new RuntimeException("Does not match xsd:boolean : "
                        + text);
            
            }

            keyBuilder.append((byte) (t ? 1 : 0));

        }

        /** Yes. */
        public boolean isDecodable() {

            return true;

        }

    }

//    /**
//     * Handles anything derived from the primitive data type
//     * {@link XMLSchema#STRING}. Values are coded as Unicode sort keys and ARE
//     * NOT decodable.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
//     *         Thompson</a>
//     * @version $Id$
//     */
//    public static class XSDStringCoder implements IDatatypeKeyCoder {
//
//        public static transient final IDatatypeKeyCoder INSTANCE = new XSDStringCoder();
//        
//        public int code() {
//
//            return CODE_XSD_STRING;
//
//        }
//
//        public String decode(byte[] buf, int off, int len) {
//
//            throw new UnsupportedOperationException();
//
//        }
//
//        public void encode(IKeyBuilder keyBuilder, String text) {
//
//            keyBuilder.append(text);
//
//        }
//
//        /** No - this class uses Unicode sort keys, which are not decodable. */
//        public boolean isDecodable() {
//
//            return false;
//
//        }
//
//    }

    /**
     * Handles {@link XMLSchema#LONG}. All such values are coded as 64-bit
     * integers.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    public static class XSDLongCoder implements IDatatypeKeyCoder {

        public static transient final IDatatypeKeyCoder INSTANCE = new XSDLongCoder();
        
        public int code() {

            return CODE_XSD_LONG;

        }

        public String decode(byte[] buf, int off, int len) {

            return Long.toString(KeyBuilder.decodeLong(buf, off));

        }

        public void encode(IKeyBuilder keyBuilder, String text) {

            keyBuilder.append(Long.valueOf(text).longValue());

        }

        /** Yes. */
        public boolean isDecodable() {

            return true;

        }

    }

    /**
     * Handles {@link XMLSchema#INT}. All such values are coded as 32-bit
     * integers.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    public static class XSDIntCoder implements IDatatypeKeyCoder {

        public static transient final IDatatypeKeyCoder INSTANCE = new XSDIntCoder();
        
        public int code() {

            return CODE_XSD_INT;

        }

        public String decode(byte[] buf, int off, int len) {

            return Integer.toString(KeyBuilder.decodeInt(buf, off));

        }

        public void encode(IKeyBuilder keyBuilder, String text) {

            keyBuilder.append(Integer.valueOf(text).intValue());

        }

        /** Yes. */
        public boolean isDecodable() {

            return true;

        }

    }

    /**
     * Handles {@link XMLSchema#SHORT}. All such values are coded as 16-bit
     * integers.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    public static class XSDShortCoder implements IDatatypeKeyCoder {

        public static transient final IDatatypeKeyCoder INSTANCE = new XSDShortCoder();
        
        public int code() {

            return CODE_XSD_SHORT;

        }

        public String decode(byte[] buf, int off, int len) {

            return Short.toString(KeyBuilder.decodeShort(buf, off));

        }

        public void encode(IKeyBuilder keyBuilder, String text) {

            keyBuilder.append(Short.valueOf(text).shortValue());

        }

        /** Yes. */
        public boolean isDecodable() {

            return true;

        }

    }

    /**
     * Handles {@link XMLSchema#BYTE}. All such values are coded as 8-bit
     * integers.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    public static class XSDByteCoder implements IDatatypeKeyCoder {

        public static transient final IDatatypeKeyCoder INSTANCE = new XSDByteCoder();
        
        public int code() {

            return CODE_XSD_BYTE;

        }

        public String decode(byte[] buf, int off, int len) {

            return Byte.toString(KeyBuilder.decodeByte(buf[off]));

        }

        public void encode(IKeyBuilder keyBuilder, String text) {

            keyBuilder.append(Byte.valueOf(text).byteValue());

        }

        /** Yes. */
        public boolean isDecodable() {

            return true;

        }

    }

    /**
     * Handles anything derived from the primitive data type
     * {@link XMLSchema#FLOAT}. All such values are coded as 32-bit integers.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    public static class XSDFloatCoder implements IDatatypeKeyCoder {

        public static transient final IDatatypeKeyCoder INSTANCE = new XSDFloatCoder();
        
        public int code() {

            return CODE_XSD_FLOAT;

        }

        public String decode(byte[] buf, int off, int len) {

            return Float.toString(KeyBuilder.decodeFloat(buf, off));

        }

        public void encode(IKeyBuilder keyBuilder, String text) {

            keyBuilder.append(Float.valueOf(text).floatValue());

        }

        /** Yes. */
        public boolean isDecodable() {

            return true;

        }

    }

    /**
     * Handles anything derived from the primitive data type
     * {@link XMLSchema#DOUBLE}. All such values are coded as 64-bit integers.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    public static class XSDDoubleCoder implements IDatatypeKeyCoder {

        public static transient final IDatatypeKeyCoder INSTANCE = new XSDDoubleCoder();
        
        public int code() {

            return CODE_XSD_DOUBLE;

        }

        public String decode(byte[] buf, int off, int len) {

            return Double.toString(KeyBuilder.decodeDouble(buf, off));

        }

        public void encode(IKeyBuilder keyBuilder, String text) {

            keyBuilder.append(Double.valueOf(text).doubleValue());

        }

        /** Yes. */
        public boolean isDecodable() {

            return true;

        }

    }

//    /**
//     * Handles anything derived from the primitive data type
//     * {@link XMLSchema#DATETIME}.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
//     *         Thompson</a>
//     * @version $Id$
//     */
//    public static class XSDDateTimeCoder implements IDatatypeKeyCoder {
//
//        public static transient final IDatatypeKeyCoder INSTANCE = new XSDDateTimeCoder();
//        
//        public int code() {
//
//            return CODE_XSD_DATETIME;
//
//        }
//
//        public String decode(byte[] buf, int off, int len) {
//
//            throw new UnsupportedOperationException();
////            return Double.toString(KeyBuilder.decodeDouble(buf, off));
//
//        }
//
//        public void encode(IKeyBuilder keyBuilder, String text) {
//
//            final XMLGregorianCalendar cal = XMLDatatypeUtil.parseCalendar(text);
//
//            // FIXME normalize to UTC and encode as int64 seconds since epoch or what?
//            
////            keyBuilder.append(.doubleValue());
//
//            throw new UnsupportedOperationException();
//
//        }
//
//        /** No. */
//        public boolean isDecodable() {
//
//            return false;
//
//        }
//
//    }

    /**
     * Handles anything derived from the primitive data type
     * {@link XMLSchema#STRING}. Values are coded as Unicode sort keys and
     * ARE NOT decodable.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    public static class XSDAnyURICoder implements IDatatypeKeyCoder {

        public static transient final IDatatypeKeyCoder INSTANCE = new XSDAnyURICoder();
        
        public int code() {

            return CODE_XSD_ANYURI;

        }

        public String decode(byte[] buf, int off, int len) {

            throw new UnsupportedOperationException();

        }

        public void encode(IKeyBuilder keyBuilder, String text) {

            keyBuilder.append(text);

        }

        /** No - this class uses Unicode sort keys, which are not decodable. */
        public boolean isDecodable() {

            return false;

        }

    }

    /**
     * Handles anything derived from the primitive data type
     * {@link RDF#XMLLITERAL}. Values are coded as Unicode sort keys and ARE NOT
     * decodable.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    public static class XSDXmlLiteralCoder implements IDatatypeKeyCoder {

        public static transient final IDatatypeKeyCoder INSTANCE = new XSDXmlLiteralCoder();
        
        public int code() {

            return CODE_XML_LITERAL;

        }

        public String decode(byte[] buf, int off, int len) {

            throw new UnsupportedOperationException();

        }

        public void encode(IKeyBuilder keyBuilder, String text) {

            keyBuilder.append(text);

        }

        /** No - this class uses Unicode sort keys, which are not decodable. */
        public boolean isDecodable() {

            return false;

        }

    }

    /**
     * Map from the specific datatype URI to the coder instance for that
     * datatype.
     */
    private static final Map<URI/* datatype */, IDatatypeKeyCoder> coders;

    private static final Map<Integer, IDatatypeKeyCoder> codes;

    synchronized static private void registerCoder(final URI datatype,
            IDatatypeKeyCoder newCoder) {

        if (coders == null)
            throw new UnsupportedOperationException();
        
        if (newCoder == null)
            throw new IllegalArgumentException();

        final int code = newCoder.code();

        if (codes.containsKey(code)) {

            throw new IllegalStateException(
                    "Coder already registered for code: code=" + code
                            + ", val=" + codes.get(code) + ", new=" + newCoder);

        }

        if (coders.containsKey(code)) {

            throw new IllegalStateException(
                    "Coder already registered for datatype: datatype="
                            + datatype + ", val=" + codes.get(code) + ", new="
                            + newCoder);

        }

        coders.put(datatype, newCoder);
        
        codes.put(code, newCoder);
        
    }
    
    static {

        if(true) {

            /*
             * Disable coders.
             * 
             * FIXME configuration option for coders? Per triple store instance?
             * Or are the coders just not compatible with the SPARQL
             * specification?
             */
            
            coders = null;
            
            codes = null;
            
        } else {
        
        // datatype URI to coder map.
        coders = new HashMap<URI, IDatatypeKeyCoder>();

        // code to coder map.
        codes = new HashMap<Integer, IDatatypeKeyCoder>();

        // 3.2.1 string and derived types.
        // Note: string is mapped onto plainLiteral by RDF Semantics.
//        registerCoder(XMLSchema.STRING, XSDStringCoder.INSTANCE);
//        registerCoder(XMLSchema.NORMALIZEDSTRING, XSDStringCoder.INSTANCE);
//        registerCoder(XMLSchema.TOKEN, XSDStringCoder.INSTANCE);
//        registerCoder(XMLSchema.LANGUAGE, XSDStringCoder.INSTANCE);
//        registerCoder(XMLSchema.NAME, XSDStringCoder.INSTANCE);
//        registerCoder(XMLSchema.NCNAME, XSDStringCoder.INSTANCE);
//        registerCoder(XMLSchema.ID, XSDStringCoder.INSTANCE);
//        registerCoder(XMLSchema.IDREF, XSDStringCoder.INSTANCE);
//        registerCoder(XMLSchema.IDREFS, XSDStringCoder.INSTANCE);
//        registerCoder(XMLSchema.ENTITY, XSDStringCoder.INSTANCE);
//        registerCoder(XMLSchema.ENTITIES, XSDStringCoder.INSTANCE);
//        registerCoder(XMLSchema.NMTOKEN, XSDStringCoder.INSTANCE);
//        registerCoder(XMLSchema.NMTOKENS, XSDStringCoder.INSTANCE);

        // 3.2.2 boolean
        registerCoder(XMLSchema.BOOLEAN, XSDBooleanCoder.INSTANCE);

        // 3.2.3 decimal and derived types.

        /*
         * @todo decimal really needs to be a BigDecimal representation if such
         * a thing can be mapped onto a totally ordered unsigned byte[] key.
         * Failing that, it needs to be projected to a maximum magnitude fixed
         * byte length representation. Failing that, all comparison of order
         * must be done by the SPARQL engine.
         */
//        registerCoder(XMLSchema.DECIMAL, XSDDecimalCoder.INSTANCE);

        /*
         * @todo integer really needs to be a BigInteger representation if such
         * a thing can be mapped onto a totally ordered unsigned byte[] key
         * (negative integers always appear to be larger than positive integers
         * with naive encoding). Failing that, it needs to be projected to a
         * maximum magnitude fixed byte length representation. Failing that, all
         * comparison of order must be done by the SPARQL engine.
         */
//        registerCoder(XMLSchema.INTEGER, XSDDecimalCoder.INSTANCE);
//        registerCoder(XMLSchema.POSITIVE_INTEGER, XSDDecimalCoder.INSTANCE);
//        registerCoder(XMLSchema.NON_POSITIVE_INTEGER, XSDDecimalCoder.INSTANCE);
//        registerCoder(XMLSchema.NON_NEGATIVE_INTEGER, XSDDecimalCoder.INSTANCE);

        /*
         * These are all fixed width signed datatypes. Each has its own code and
         * its own disjoint value space.
         */
        registerCoder(XMLSchema.LONG, XSDLongCoder.INSTANCE); // 64-bits
        registerCoder(XMLSchema.INT, XSDIntCoder.INSTANCE); // 32-bits
        registerCoder(XMLSchema.SHORT, XSDShortCoder.INSTANCE); // 16-bits
        registerCoder(XMLSchema.BYTE, XSDByteCoder.INSTANCE); // 8 bits.
        /*
         * These are all fixed width unsigned datatypes. Each has its own code
         * and its own disjoint value space.
         * 
         * @todo unsigned long
         * @todo unsigned int
         * @todo unsigned short
         * @todo unsigned byte
         */
//        registerCoder(XMLSchema.UNSIGNED_LONG, XSDULongCoder.INSTANCE); // 64-bits
//        registerCoder(XMLSchema.UNSIGNED_INT, XSDUIntCoder.INSTANCE);// 32-bits
//        registerCoder(XMLSchema.UNSIGNED_SHORT, XSDUShortecimalCoder.INSTANCE); // 16-bits
//        registerCoder(XMLSchema.UNSIGNED_BYTE, XSDUByteCoder.INSTANCE); // 8-bits

        // 3.2.4 float
        registerCoder(XMLSchema.FLOAT, XSDFloatCoder.INSTANCE);
        
        // 3.2.5 double
        registerCoder(XMLSchema.DOUBLE, XSDDoubleCoder.INSTANCE);

        // 3.2.6 duration Note: not implemented yet per W3C Note.
        
        // @todo 3.2.7 dateTime
//        registerCoder(XMLSchema.DATETIME, XSDDateTimeCoder.INSTANCE);
        // @todo 3.2.8 time
        // @todo 3.2.9 date
        // @todo 3.2.10 gYearMonth
        // @todo 3.2.11 gYear
        // @todo 3.2.12 gMonthDay
        // @todo 3.2.13 gDay
        // @todo 3.2.14 gMonth
        // @todo 3.2.15 hexBinary
        // @todo 3.2.16 base64Binary
        // 3.2.17 anyURI
        registerCoder(XMLSchema.ANYURI, XSDAnyURICoder.INSTANCE);
        // @todo 3.2.18 QName
        // @todo 3.2.19 NOTATION

        registerCoder(RDF.XMLLITERAL, XSDXmlLiteralCoder.INSTANCE);

        }
        
    }

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

        if (false && datatype.equals(XMLSchema.STRING)) {

            /*
             * @todo xsd:string is explicitly mapped by RDF Semantics onto plain
             * literals (they entail one another). However, it breaks the SPARQL
             * unit tests if you map them onto the same key.
             */
            return plainLiteral2key(value);
            
        }
        
        if (coders == null) {

            /*
             * Note: This is the original DTL code space. The full lexical form
             * of the data type URI is serialized into the key as a Unicode sort
             * key followed by a nul byte and then a Unicode sort key formed
             * from the lexical form of the data type value.
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
            
        } else {

            // clear out any existing key and add prefix for the DTL space.
            keyBuilder.reset().append(TERM_CODE_DTL2);

            final IDatatypeKeyCoder coder = coders.get(datatype);

            if (coder == null) {

                /*
                 * Unknown datatypes are placed into a disjoint space first, for
                 * all unknown data types, and second, for the specific data
                 * type using its URI encoded as a sort key. Finally, the
                 * unknown data type value is encoded on the key.
                 */

                // disjoint value space for all unknown data type URIs.
                keyBuilder.append(IDatatypeKeyCoder.CODE_OTHER);

                // encode the datatype URI as Unicode sort key to make all
                // unknown
                // types disjoint.
                keyBuilder.append(datatype.stringValue());

                // encode the datatype value as Unicode sort key.
                keyBuilder.append(value);

            } else {

                /*
                 * Use the configured coder.
                 */

                // disjoint value space.
                keyBuilder.append(coder.code());

                // data type specific encoding.
                coder.encode(keyBuilder, value);

            }

            keyBuilder.appendNul();

            return keyBuilder.getKey();
        }

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
