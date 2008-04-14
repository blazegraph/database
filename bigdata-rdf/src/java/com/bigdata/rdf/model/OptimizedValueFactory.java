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
 * Created on Jan 26, 2007
 */

package com.bigdata.rdf.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Comparator;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import javax.xml.datatype.XMLGregorianCalendar;

import org.CognitiveWeb.extser.ShortPacker;
import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.BNodeImpl;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;

import com.bigdata.btree.BytesUtil;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.rdf.rio.IStatementBuffer;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.ITripleStore;
import com.bigdata.rdf.util.RdfKeyBuilder;

public class OptimizedValueFactory implements ValueFactory {

    public static final OptimizedValueFactory INSTANCE = new OptimizedValueFactory();
    
    private OptimizedValueFactory() {
        
    }
    
    /**
     * Converts a {@link Value} for a different {@link ValueFactory} into a
     * {@link _Value}.
     * 
     * @param v
     *            The value.
     * 
     * @return The value iff it is a {@link _Value} and otherwise a
     *         {@link _Value} with the same data. If the value is
     *         <code>null</code> then <code>null</code> is returned.
     */
    final public _Value toNativeValue( Value v ) {
        
        if(v == null) return null;
        
        if( v instanceof URI && ! ( v instanceof _URI) ) {
            
            v = createURI(v.toString());
            
        } else if( v instanceof Literal && ! ( v instanceof _Literal )) {
            
            String label = ((Literal)v).getLabel();
            
            String language = ((Literal)v).getLanguage();
            
            URI datatype = ((Literal)v).getDatatype();
            
            if( language != null ) {

                v = createLiteral(label,language);
                
            } else if( datatype != null ) {
                
                v = createLiteral(label,createURI(datatype.toString()));
                
            } else {
                
                v = createLiteral(label);
                
            }
            
        } else if( v instanceof BNode && ! ( v instanceof _BNode )) {

            v = createBNode( ((BNode)v).getID() );
            
        }
        
        return (_Value)v;
        
    }
    
    /**
     * Converts a {@link _Value} into a {@link Value} using the Sesame object
     * model implementations.
     * 
     * @param v
     *            The value.
     * 
     * @return A {@link Value} with the same data. If the value is
     *         <code>null</code> then <code>null</code> is returned.
     */
    final public Value toSesameObject( Value v ) {
        
        if( v == null ) return null;
        
        if( v instanceof _URI ) {
            
            v = new URIImpl(((_URI) v).term);
            
        } else if( v instanceof _Literal ) {
            
            String label = ((_Literal)v).term;
            
            String language = ((_Literal)v).language;
            
            _URI datatype = ((_Literal)v).datatype;
            
            if( language != null ) {

                v = new LiteralImpl(label,language);
                
            } else if( datatype != null ) {
                
                v = new LiteralImpl(label, new URIImpl(datatype.term));
                
            } else {
                
                v = new LiteralImpl(label);
                
            }
            
        } else if (v instanceof _BNode) {

            v = new BNodeImpl( ((_BNode)v).term );
            
        } else {
            
            throw new AssertionError();
            
        }
        
        return v;
        
    }
    
    public BNode createBNode() {

//        String id = "_"+UUID.randomUUID();
        
        String id = "_"+_ID.incrementAndGet();
        
        return new _BNode(id);
        
    }
    private final AtomicLong _ID = new AtomicLong(0L); 

    public BNode createBNode(String id) {

        return new _BNode(id);

    }

    public Literal createLiteral(String label, String lang) {

        return new _Literal(label, lang);

    }

    public Literal createLiteral(String label, URI datatype) {

        return new _Literal(label, (_URI) INSTANCE.toNativeValue(datatype));

    }

    public Literal createLiteral(String label) {

        return new _Literal(label);

    }

    /**
     * Create an explicit {@link _Statement}.
     */
    public Statement createStatement(Resource s, URI p, Value o) {

        return createStatement(s, p, o, null);

    }

    public Statement createStatement(Resource s, URI p, Value o, Resource c) {

        s = (Resource) toNativeValue(s);
        
        p = (URI) toNativeValue(p);
        
        o = toNativeValue(o);
        
        c = (Resource) toNativeValue(c);
        
        return new _Statement((_Resource) s, (_URI) p, (_Value) o, (_Resource)c,
                StatementEnum.Explicit);
        
    }
    
    public URI createURI(String namespace, String localName) {

        return createURI(namespace + localName);

    }

    public URI createURI(String uriStr) {

        return new _URI(uriStr);

    }

    /**
     * <p>
     * An RDF {@link Value} base class designed to support batch and bulk
     * loading of parsed RDF data.
     * </p>
     * <p>
     * {@link #serialize()} and {@link #deserialize(byte[])} which provide
     * efficient serialization by NOT writing the class identifier in the record
     * and include support for versioning of the serialization format and
     * transparent de-serialization of {@link _Value}s without prior knowledge
     * of the type of RDF Value. There is a packed short version code for each
     * serialized term. In addition, each serialized term has a single byte
     * prefix that identifies the region of the term space (URI, plain literal,
     * language code literal, datatype literal, or blank node) to which the
     * serialized term belongs. In practice, this results in a 2-byte prefix
     * since the version code packs into a single byte and the type code is also
     * a single byte. Note that the serialized term is NOT the basis for ordered
     * comparison of terms - that is always done using the sort key generated by
     * the {@link RdfKeyBuilder}.
     * </p>
     * <p>
     * The {@link Externalizable} implementation is reasonably efficient and is
     * used to implement {@link #serialize()} and {@link #deserialize(byte[])},
     * but it is fatter since it writes the class identifier. Also, it does not
     * support versioning.
     * </p>
     * 
     * @todo consider requiring the translation of the datatype URI to a long
     *       integer for persistence. I have not done this in order to avoid a
     *       dependency on the <em>ids</em> index to resolve the datatype URI
     *       id into the corresponding URI.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    abstract public static class _Value implements Value, Externalizable {

        /**
         * Version zero(0) of the term {@link Externalizable} format.
         */
        protected static final short VERSION0 = 0x0;
        
        /**
         * Error message indicates that the version code in the serialized
         * record did not correspond to a known serialization version for an RDF
         * value.
         */
        protected static final String ERR_VERSION = "Bad version";
        
        /**
         * Error message indicates that the term code in the serialized record
         * did not correspond to a known term code for an RDF value of the
         * appropriate type (e.g., a URI code where an Literal code was
         * expected). The codes are defined by {@link RdfKeyBuilder}.
         */
        protected static final String ERR_CODE = "Bad term code";
        
        /**
         * The primary lexical term (URI, label for literal, ID for BNode).
         * <p>
         * Note that derived classes may define additional attributes such as
         * the language code or datatype for a literal.
         * <p>
         * Note: this field is not final since that interferes with the
         * {@link Externalizable} implementation.
         */
        public /*final*/ String term;
        
        /**
         * The sort key under which this value will be placed into the terms
         * index. The sort key is a representation of the total term, including
         * any additional attributes such as the language code or the datatype
         * URI.
         * 
         * @see RdfKeyBuilder
         */
        public byte[] key = null;

        /**
         * The term identifier assigned to this term by the terms index and the
         * identifier under which the lexical item may be recovered from the
         * term identifiers index.
         */
        public long termId = 0;

        /**
         * The #of times that this term has been used in a {@link Statement}.
         */
        public int count = 0;
        
        /**
         * Initially <code>false</code>, this field is set <code>true</code>
         * if it is determined that a term is in both the terms index and the
         * term identifiers index. This basically captures the intermediate
         * state after we have inserted a term into the term:id mapping, thereby
         * assigning the termId, but have not yet verified that the term is also
         * in the reverse id:term mapping. In this case, [known] should be false
         * until we have that verification. [known] can be set [true] if we
         * resolve a termId from a term since that demonstrates existence in the
         * reverse index and the presence in the reverse index is taken as a
         * guarentee of the existence in the forward index since we always
         * insert into the forward index first and we never retract terms from
         * the lexicon.
         * 
         * FIXME The use of this flag is not always consistent with the javadoc
         * above.
         */
        public boolean known = false;
        
        /**
         * De-serialization constructor.
         */
        protected _Value() {
            
        }
        
        public _Value(String term) {

            this.term = term;

        }

        /**
         * Return the term code as defined by {@link RdfKeyBuilder} for this
         * type of term. This is used to places URIs, different types of
         * literals, and bnodes into disjoint parts of the key space for sort
         * orders.
         * 
         * @see RdfKeyBuilder
         */
        abstract public byte getTermCode();

        /**
         * Compares two {@link _Value} objects, placing them into a total
         * ordering based on the {@link #key}s assigned by the
         * {@link RdfKeyBuilder}.
         * 
         * @exception IllegalStateException
         *                if the sort keys have not been assigned.
         * @exception ClassCastException
         *                if <i>o</i> is not a {@link _Value}.
         */
        final public int compareTo(Object o) {
        
            if(key==null)
                throw new IllegalStateException("no sort key");
            
            _Value oval = (_Value)o;
            
            if(oval.key==null)
                throw new IllegalStateException("no sort key");
            
            return BytesUtil.compareBytes(key, oval.key);
            
        }

        /**
         * Compares to {@link _Value} objects for equality.
         * 
         * @param o
         *            A {@link _Value} object.
         * 
         * @exception ClassCastException
         *                if <i>o</i> is not a {@link _Value}.
         */
        abstract public boolean equals(Object o);

        /**
         * Note: the hash code is based solely on the {@link #term} and does not
         * consider the term class (URI, some type of literal, or BNode) or the
         * attributes for language code or datatype literals.
         */
        final public int hashCode() {

            return term.hashCode();

        }

        public String stringValue() {
            
            return term;
            
        }

        public String toString() {

            return term;
            
//            return term + " {termId=" + termId + ", haveKey=" + (key != null)
//                    + ", known=" + known + "}";

        }

        /**
         * Routine for efficient serialization of an RDF {@link _Value}.
         * 
         * @return The byte[] containing the serialized data record.
         * 
         * @throws RuntimeException if there is a IO problem
         * 
         * @see {@link #deserialize(byte[])}
         */
        public byte[] serialize() {
            
            DataOutputBuffer out = new DataOutputBuffer(128);

            return serialize(out);
            
        }
        
        /**
         * Variant which permits reuse of the same buffer. This has the
         * advantage that the buffer is reused on each invocation and swiftly
         * grows to its maximum extent.
         * 
         * @param out
         *            The buffer - the caller is responsible for resetting the
         *            buffer before each invocation.
         * 
         * @return The byte[] containing the serialized data record. This array
         *         is newly allocated so that a series of invocations of this
         *         method return distinct byte[]s.
         */
        public byte[] serialize(DataOutputBuffer out) {
            
            try {

                final short version = VERSION0;

                ShortPacker.packShort(out, version);

                final byte termCode = getTermCode();

                /*
                 * Note: VERSION0 writes the termCode immediately after the
                 * packed version identifier. Other versions MAY do something
                 * else.
                 */
                out.writeByte(termCode);

                /*
                 * FIXME There are inefficiencies in the DataOutputBuffer when
                 * writing UTF8. See if we can work around those using the ICU
                 * package. The issue is documented in the DataOutputBuffer
                 * class.
                 */
                
                serialize(version, termCode, out);

                return out.toByteArray();

            } catch (IOException ex) {

                throw new RuntimeException(ex);

            }
                        
        }
        
        /**
         * Routine for efficient de-serialization of an RDF {@link _Value}.
         * 
         * @param b
         *            The byte[] containing the serialized data record.
         * 
         * @return The {@link _Value}.
         * 
         * @throws RuntimeException if there is an IO problem.
         * 
         * @see {@link #serialize()}
         */
        public static _Value deserialize(byte[] b) {

            return deserialize( new DataInputBuffer(b) );
            
        }

        /**
         * Routine for efficient de-serialization of an RDF {@link _Value}.
         * 
         * @param b
         *            An input stream from which the serialized data may be
         *            read.
         * 
         * @return The {@link _Value}.
         * 
         * @throws RuntimeException
         *             if there is an IO problem.
         * 
         * @see {@link #serialize()}
         */
        public static _Value deserialize(DataInputBuffer in) {
            
            try {

                final short version = in.unpackShort();

                if (version != VERSION0) {

                    throw new RuntimeException(ERR_VERSION + " : " + version);

                }

                /*
                 * Note: The term code immediately follows the packed version
                 * code for VERSION0 - this is not necessarily true for other
                 * serialization versions.
                 */

                final byte termCode = in.readByte();

                switch (termCode) {

                case RdfKeyBuilder.TERM_CODE_URI: {

                    _URI tmp = new _URI();

                    tmp.deserialize(version, termCode, in);

                    return tmp;

                }

                case RdfKeyBuilder.TERM_CODE_LIT:
                case RdfKeyBuilder.TERM_CODE_LCL:
                case RdfKeyBuilder.TERM_CODE_DTL: {

                    _Literal tmp = new _Literal();

                    tmp.deserialize(version, termCode, in);

                    return tmp;

                }

                case RdfKeyBuilder.TERM_CODE_BND: {

                    _BNode tmp = new _BNode();

                    tmp.deserialize(version, termCode, in);

                    return tmp;

                }

                default:
                    throw new RuntimeException(ERR_CODE + " : " + termCode);

                }

            } catch (IOException ex) {

                throw new RuntimeException(ex);

            }
             
        }
        
        /**
         * Implements the serialization of a Literal, URI, or BNode.
         * 
         * @param version
         *            The serialization version number (which has already been
         *            written on <i>out</i> by the caller).
         * @param termCode
         *            The byte encoding the type of term as defined by
         *            {@link RdfKeyBuilder} (this has already been written on
         *            <i>out</i> by the caller).
         * @param out
         *            The data are written here.
         * 
         * @throws IOException
         */
        abstract protected void serialize(short version, byte termCode,
                DataOutput out) throws IOException;
        
        /**
         * Implements the de-serialization of a Literal, URI, or BNode.
         * 
         * @param version
         *            The serialization version number (which has already been
         *            read by the caller).
         * @param termCode
         *            The byte encoding the type of term as defined by
         *            {@link RdfKeyBuilder} (this has already been read by the
         *            caller).
         * @param in
         *            The data are read from here.
         * 
         * @throws IOException
         */
        abstract protected void deserialize(short version, byte termCode,
                DataInput in) throws IOException;
        
    }

    /**
     * Places {@link #_Value}s into an ordering determined by their assigned
     * variable length unsigned byte[] sort keys.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @see _Value#key
     */
    public static class _ValueSortKeyComparator implements Comparator<_Value> {

        public static final transient Comparator<_Value> INSTANCE = new _ValueSortKeyComparator();

        public int compare(_Value o1, _Value o2) {

            return BytesUtil.UnsignedByteArrayComparator.INSTANCE.compare(
                    o1.key, o2.key);

        }

    }

    /**
     * Places {@link #_Value}s into an ordering determined by their assigned
     * {@link _Value#termId term identifiers}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @see _Value#termId
     */
    public static class TermIdComparator implements Comparator<_Value> {

        public static final transient Comparator<_Value> INSTANCE =
            new TermIdComparator();

        public int compare(_Value term1, _Value term2) {

            /*
             * Note: comparison avoids possible overflow of <code>long</code> by
             * not computing the difference directly.
             */

            final long id1 = term1.termId;
            final long id2 = term2.termId;
            
            if(id1 < id2) return -1;
            if(id1 > id2) return 1;
            return 0;

        }

    }
    
    abstract public static class _Resource extends _Value implements Resource {

        /**
         * De-serialization constructor.
         */
        protected _Resource() {
            
            super();
            
        }
        
        public _Resource(String term) {

            super(term);

        }

    }

    /**
     * A blank node.
     * <p>
     * Note: When {@link AbstractTripleStore.Options#STATEMENT_IDENTIFIERS} is
     * enabled blank nodes in the context position of a statement are recognized
     * as statement identifiers by {@link StatementBuffer}. It coordinates with
     * this class in order to detect when a blank node is a statement identifier
     * and to defer the assertion of statements made using a statement
     * identifier until that statement identifier becomes defined by being
     * paired with a statement.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    final public static class _BNode extends _Resource implements BNode {

        /**
         * 
         */
        private static final long serialVersionUID = 8835732082253951776L;
        
        /**
         * Boolean flag is set during conversion from an RDF interchange syntax
         * into the internal {@link SPO} model if the blank node is a statement
         * identifier.
         */
        public boolean statementIdentifier;
        
        /**
         * De-serialization constructor.
         */
        public _BNode() {

            super();

        }

        public _BNode(String id) {

            super(id);

        }
        
        public String getID() {

            return term;

        }

        /**
         * @return {@link RdfKeyBuilder#TERM_CODE_BND}.
         */
        public byte getTermCode() {
            
            return RdfKeyBuilder.TERM_CODE_BND;
            
        }

        public boolean equals(Object o) {

            if (o == this) return true;

            if( ! (o instanceof _BNode )) return false;
            
            _BNode oval = (_BNode)o;

            return term.equals(oval.term);

        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            
            deserialize(VERSION0, RdfKeyBuilder.TERM_CODE_BND, in);
            
        }

        public void writeExternal(ObjectOutput out) throws IOException {

            serialize(VERSION0, RdfKeyBuilder.TERM_CODE_BND, out);
            
        }

        protected void serialize(short version, byte termCode, DataOutput out)
                throws IOException {
            
            if(true) {
                
                /*
                 * Note: disabled since we never write the BNode as a value in
                 * the id:term index because BNodes IDs are only consistent, not
                 * stable.
                 */ 
                
                throw new UnsupportedOperationException();
                
            }
            
            assert termCode == RdfKeyBuilder.TERM_CODE_BND;
            
            out.writeUTF(term);
            
        }

        protected void deserialize(short version, byte termCode, DataInput in) throws IOException {

            if(true) {
                
                /*
                 * Note: disabled since we never write the BNode as a value in
                 * the id:term index because BNodes IDs are only consistent, not
                 * stable.
                 */ 
                
                throw new UnsupportedOperationException();
                
            }

            assert termCode == RdfKeyBuilder.TERM_CODE_BND;

            term = in.readUTF();
            
        }

    }

    final public static class _Literal extends _Value implements Literal {

        /*
         * Note: these fields are not final since that interfers with the
         * Externalizable implementation.
         */

        /**
         * 
         */
        private static final long serialVersionUID = -5276702268990981415L;

        /**
         * One of the values defined by {@link RdfKeyBuilder} to identify the
         * distinct kinds of RDF literal:
         * <ul>
         * <li>{@link RdfKeyBuilder#TERM_CODE_LIT plain literal}</li>
         * <li>{@link RdfKeyBuilder#TERM_CODE_LCL language code literal}</li>
         * <li>{@link RdfKeyBuilder#TERM_CODE_DTL datatype literal}</li>
         * </ul>
         * Note that the code places each kind of term (Literal, URI, or BNode)
         * into a distinct region of the value space for terms.
         */
        byte code;
        
        /**
         * <p>
         * The optional language tag.
         * </p>
         * <p>
         * The language tag is defined as a possibly empty sequence of
         * characters selected from [A-Za-z0-9]. Subtags MAY be specified and
         * are delimited by a hypen ("-"). The language tag must be interpreted
         * in a case-insensitive manner, so it must be forced to upper/lower
         * case consistently when forming a key based on a language code
         * literal. Likewise, an empty language tag MAY be specified and should
         * be interpreted as equivilent to NO language tag.
         * </p>
         * 
         * @see http://www.w3.org/TR/rdf-syntax-grammar/#section-Syntax-languages
         * @see http://www.w3.org/TR/REC-xml/
         * @see http://www.ietf.org/rfc/rfc3066.txt
         */
        public String language;

        /**
         * The optional datatype URI.
         */
        public _URI datatype;

        /**
         * One of the values defined by {@link RdfKeyBuilder} to identify the
         * distinct kinds of RDF literal:
         * <ul>
         * <li>{@link RdfKeyBuilder#TERM_CODE_LIT plain literal}</li>
         * <li>{@link RdfKeyBuilder#TERM_CODE_LCL language code literal}</li>
         * <li>{@link RdfKeyBuilder#TERM_CODE_DTL datatype literal}</li>
         * </ul>
         * Note that the code places each kind of term (Literal, URI, or BNode)
         * into a distinct region of the value space for terms.
         */
        public byte getTermCode() {
            
            return code;
            
        }

        /**
         * De-serialization constructor.
         */
        public _Literal() {
            
            super();
            
        }
        
        public _Literal(String label) {

            super(label);
            
            this.code = RdfKeyBuilder.TERM_CODE_LIT;

            this.language = null;
            
            this.datatype = null;
            
        }

        public _Literal(String label, String language) {

            super(label);

            if(language==null) {
                
                throw new IllegalArgumentException();
                
            }
            
            if (language.length() == 0) {

                this.code = RdfKeyBuilder.TERM_CODE_LIT;

                this.language = null;
                
                this.datatype = null;

            } else {

                this.code = RdfKeyBuilder.TERM_CODE_LCL;

                this.language = language;

                this.datatype = null;

            }
            
        }

        public _Literal(String label, _URI datatype) {

            super(label);

            if(datatype==null) {
                
                throw new IllegalArgumentException();
                
            }
            
            this.code = RdfKeyBuilder.TERM_CODE_DTL;

            this.language = null;
            
            this.datatype = datatype;

        }

        public String getLabel() {

            return term;

        }

        public URI getDatatype() {

            return datatype;

        }

        public String getLanguage() {

            return language;

        }

        public boolean equals(Object o) {

            if (o == this) return true;

            if( ! (o instanceof _Literal )) return false;
            
            _Literal oval = (_Literal)o;

            // wrong type of literal.
            if( code != oval.code) return false;

            // wrong label.
            if(!term.equals(oval.term)) return false;
            
            if(code==RdfKeyBuilder.TERM_CODE_LIT) {
                
                return true;
                
            } else if(code==RdfKeyBuilder.TERM_CODE_LCL) {

                // check language code.
                return language.equals(oval.language);
                
            } else if(code==RdfKeyBuilder.TERM_CODE_DTL) {

                // check datatype.
                return datatype.equals(oval.datatype);

            } else {
                
                throw new AssertionError();
                
            }

        }
        
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

            final byte termCode = in.readByte();
            
            deserialize(VERSION0, termCode, in);
            
        }

        public void writeExternal(ObjectOutput out) throws IOException {
    
            out.writeByte(code);
            
            serialize(VERSION0,code,out);
            
        }

        /**
         * Write the term on the output stream. The caller has already written
         * the version identifier (if applicable) and the {@link #code}.
         */
        protected void serialize(short version, byte termCode, DataOutput out)
                throws IOException {
            
            switch (termCode) {
            
            case RdfKeyBuilder.TERM_CODE_LIT:
                
                break;
            
            case RdfKeyBuilder.TERM_CODE_LCL:
                
                assert language != null;

                /*
                 * Note: This field is ASCII [A-Za-z0-9] and "-". However, this
                 * method writes using UTF-8 so it will generate one byte per
                 * character and it is probably more work to write the data
                 * directly as ASCII bytes.
                 */
                out.writeUTF(language);
                
                break;
            
            case RdfKeyBuilder.TERM_CODE_DTL:
                
                assert datatype != null;
                
                out.writeUTF(datatype.term);

                break;

            default:
                
                throw new IOException("Not a literal: code=" + code);
            
            }
            
            assert term != null;
            
            out.writeUTF(term);
                        
        }
        
        protected void deserialize(short version, byte termCode, DataInput in)
                throws IOException {

            // save the code on the _Literal.
            this.code = termCode;
            
            switch (code) {
            
            case RdfKeyBuilder.TERM_CODE_LIT:
                
                break;
            
            case RdfKeyBuilder.TERM_CODE_LCL:
                
                language = in.readUTF();

                assert language != null;
                
                break;
            
            case RdfKeyBuilder.TERM_CODE_DTL:
            
                datatype = new _URI(in.readUTF());
                
                assert datatype != null;
                
                break;

            default:
                
                throw new IOException(ERR_CODE+" : "+code);
            
            }

            term = in.readUTF();
            
            assert term != null;
            
        }

        /*
         * FIXME datatype aware methods are not implemented.  frankly, I do not
         * think that we will need them for the RIO integration.
         */
        
        public boolean booleanValue() {
            // TODO Auto-generated method stub
            return false;
        }

        public byte byteValue() {
            // TODO Auto-generated method stub
            return 0;
        }

        public XMLGregorianCalendar calendarValue() {
            // TODO Auto-generated method stub
            return null;
        }

        public BigDecimal decimalValue() {
            // TODO Auto-generated method stub
            return null;
        }

        public double doubleValue() {
            // TODO Auto-generated method stub
            return 0;
        }

        public float floatValue() {
            // TODO Auto-generated method stub
            return 0;
        }

        public int intValue() {
            // TODO Auto-generated method stub
            return 0;
        }

        public BigInteger integerValue() {
            // TODO Auto-generated method stub
            return null;
        }

        public long longValue() {
            // TODO Auto-generated method stub
            return 0;
        }

        public short shortValue() {
            // TODO Auto-generated method stub
            return 0;
        }
        
    }

    final public static class _URI extends _Resource implements URI {
        
        /**
         * 
         */
        private static final long serialVersionUID = 8085405245340777144L;

        /**
         * De-serialization constructor.
         */
        public _URI() {
            
            super();
            
        }
        
        public _URI(String uri) {

            super(uri);

        }

        public _URI(URI uri) {

            super(uri.toString());

        }

        public _URI(String namespace, String localName) {

            super(namespace + localName);

        }

        public String toString() {

            return term;

        }

        public String getLocalName() {

            int i = term.lastIndexOf('#');

            if ((i + 1) < (term.length() - 1)) {

                return term.substring(i + 1);

            }

            return "";

        }

        public String getNamespace() {

            int i = term.lastIndexOf('#');

            if (i > 0) {

                return term.substring(0, i + 1);

            }

            return "";

        }

        /**
         * @return {@link RdfKeyBuilder#TERM_CODE_URI}.
         */
        public byte getTermCode() {

            return RdfKeyBuilder.TERM_CODE_URI;
            
        }

        public boolean equals(Object o) {

            if (o == this) return true;

            if( ! (o instanceof _URI )) return false;
            
            _URI oval = (_URI)o;

            return term.equals(oval.term);

        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            
            deserialize(VERSION0, RdfKeyBuilder.TERM_CODE_URI, in);
            
        }

        public void writeExternal(ObjectOutput out) throws IOException {

            serialize(VERSION0, RdfKeyBuilder.TERM_CODE_URI, out);
            
        }
        
        /**
         * Serializes the URI as a Unicode string.
         */
        protected void serialize(short version, byte termCode, DataOutput out)
                throws IOException {
            
            assert termCode == RdfKeyBuilder.TERM_CODE_URI;
            
            // Serialize as UTF.
            out.writeUTF(term);

        }
        
        protected void deserialize(short version, byte termCode, DataInput in)
            throws IOException {

            assert termCode == RdfKeyBuilder.TERM_CODE_URI;

            term = in.readUTF();
            
        }
        
    }

    /**
     * A statement with optional context (a quad).
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class _Statement implements Statement {

        /**
         * 
         */
        private static final long serialVersionUID = 3259278872142669482L;

        public final _Resource s;

        public final _URI p;

        public final _Value o;

        public final _Resource c;

        /**
         * The #of times this statement is encountered within a
         * {@link com.bigdata.rdf.rio.StatementBuffer}.
         */
        public int count = 0;
        
        /**
         * Initially <code>false</code>, this field is set <code>true</code>
         * if it is determined that a statement is known to the statement
         * indices maintained by the {@link ITripleStore}. This is used to
         * avoid re-definition of statements in the during a bulk load operation
         * where duplicate keys would violate the B+-Tree unique key constraint.
         */
        public boolean known = false;

        public StatementEnum type;
        
//        /**
//         * Create a {@link StatementEnum#Explicit} statement.
//         * 
//         * @param s
//         * @param p
//         * @param o
//         */
//        public _Statement(_Resource s, _URI p, _Value o) {
//
//            this.s = s;
//
//            this.p = p;
//
//            this.o = o;
//            
//            this.type = StatementEnum.Explicit;
//
//        }

        /**
         * Create a statement with the specified {@link StatementEnum}.
         * 
         * @param s
         * @param p
         * @param o
         * @param c
         * @param type
         */
        public _Statement(_Resource s, _URI p, _Value o, _Resource c, StatementEnum type) {

            assert type != null;
            
            this.s = s;

            this.p = p;

            this.o = o;
            
            this.c = c;
            
            this.type = type;
            
        }

        public Value getObject() {

            return o;

        }

        public URI getPredicate() {

            return p;

        }

        public Resource getSubject() {

            return s;

        }

        public Resource getContext() {
            
            return c;
            
        }
        
        /**
         * Note: {@link StatementBuffer} relies on this method returning true
         * iff the term identifiers and the {@link #type} are all equals in
         * order to judge when whether or not two statements are the same or
         * "distinct".
         */
        public boolean equals(_Statement stmt) {
            
            return (s.termId == stmt.s.termId) && //
                   (p.termId == stmt.p.termId) && //
                   (o.termId == stmt.o.termId) && //
                   (type == stmt.type) //
                   ;
            
        }
        
        /**
         * Imposes s:p:o ordering based on termIds only.
         */
        public int compareTo(Object other) {

            if (other == this) {

                return 0;

            }

            final _Statement stmt1 = this;
            final _Statement stmt2 = (_Statement) other;
            
            /*
             * Note: logic avoids possible overflow of [long] by not computing the
             * difference between two longs.
             */
            int ret;
            
            ret = stmt1.s.termId < stmt2.s.termId ? -1 : stmt1.s.termId > stmt2.s.termId ? 1 : 0;
            
            if( ret == 0 ) {
            
                ret = stmt1.p.termId < stmt2.p.termId ? -1 : stmt1.p.termId > stmt2.p.termId ? 1 : 0;
                
                if( ret == 0 ) {
                    
                    ret = stmt1.o.termId < stmt2.o.termId ? -1 : stmt1.o.termId > stmt2.o.termId ? 1 : 0;
                    
                }
                
            }

            return ret;

        }
        
        /**
         * True iff the statements are the same object or if they have the same
         * non-zero term identifiers assigned for the subject, predicate and
         * object positions, or if they have terms in the subject, predicate,
         * and object positions that compare as {@link _Value#equals(Object)}.
         */
        public boolean equals(Object o) {

            if (o == this)
                return true;

            final _Statement stmt1 = this;
            final _Statement stmt2 = (_Statement) o;
            
            if (stmt1.s.termId == 0 || stmt1.p.termId == 0
                    || stmt1.o.termId == 0 || stmt2.s.termId == 0
                    || stmt2.p.termId == 0 || stmt2.o.termId == 0) {

                /*
                 * Note: one or more term identifiers are not assigned so we
                 * compare the terms themselves.
                 */
                return stmt1.s.equals(stmt2.s) && stmt1.p.equals(stmt2.p)
                        && stmt1.o.equals(stmt2.o);
                
            }
            
            // All term identifiers are assigned so we compare them.
            return stmt1.s.termId == stmt2.s.termId
                    && stmt1.p.termId == stmt2.p.termId
                    && stmt1.o.termId == stmt2.o.termId;

        }

        public String toString() {
            
            return "<"+s+", "+p+", "+o+(c==null?"":", "+c)+">"+(type==null?"":" : "+type);
            
        }
        
    }

    /**
     * Imposes s:p:o ordering based on termIds.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class SPOComparator implements Comparator<_Statement> {

        public static final transient Comparator<_Statement> INSTANCE = new SPOComparator();

        public int compare(_Statement stmt1, _Statement stmt2) {

            /*
             * Note: logic avoids possible overflow of [long] by not computing the
             * difference between two longs.
             */
            int ret;
            
            ret = stmt1.s.termId < stmt2.s.termId ? -1 : stmt1.s.termId > stmt2.s.termId ? 1 : 0;
            
            if( ret == 0 ) {
            
                ret = stmt1.p.termId < stmt2.p.termId ? -1 : stmt1.p.termId > stmt2.p.termId ? 1 : 0;
                
                if( ret == 0 ) {
                    
                    ret = stmt1.o.termId < stmt2.o.termId ? -1 : stmt1.o.termId > stmt2.o.termId ? 1 : 0;
                    
                }
                
            }

            return ret;

        }

    }

    /**
     * Imposes p:o:s ordering based on termIds.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class POSComparator implements Comparator<_Statement> {

        public static final transient Comparator<_Statement> INSTANCE = new POSComparator();

        public int compare(_Statement stmt1, _Statement stmt2) {

            /*
             * Note: logic avoids possible overflow of [long] by not computing the
             * difference between two longs.
             */
            int ret;
            
            ret = stmt1.p.termId < stmt2.p.termId ? -1 : stmt1.p.termId > stmt2.p.termId ? 1 : 0;
            
            if( ret == 0 ) {
            
                ret = stmt1.o.termId < stmt2.o.termId ? -1 : stmt1.o.termId > stmt2.o.termId ? 1 : 0;
                
                if( ret == 0 ) {
                    
                    ret = stmt1.s.termId < stmt2.s.termId ? -1 : stmt1.s.termId > stmt2.s.termId ? 1 : 0;
                    
                }
                
            }

            return ret;

        }

    }

    /**
     * Imposes o:s:p ordering based on termIds.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class OSPComparator implements Comparator<_Statement> {

        public static final transient Comparator<_Statement> INSTANCE = new OSPComparator();

        public int compare(_Statement stmt1, _Statement stmt2) {

            /*
             * Note: logic avoids possible overflow of [long] by not computing the
             * difference between two longs.
             */
            int ret;
            
            ret = stmt1.o.termId < stmt2.o.termId ? -1 : stmt1.o.termId > stmt2.o.termId ? 1 : 0;
            
            if( ret == 0 ) {
            
                ret = stmt1.s.termId < stmt2.s.termId ? -1 : stmt1.s.termId > stmt2.s.termId ? 1 : 0;
                
                if( ret == 0 ) {
                    
                    ret = stmt1.p.termId < stmt2.p.termId ? -1 : stmt1.p.termId > stmt2.p.termId ? 1 : 0;
                    
                }
                
            }

            return ret;

        }

    }

    /*
     * FIXME datatyped literal methods are not implemented - I don't know that
     * we will need them on this value factory.
     */
    
    public Literal createLiteral(boolean arg0) {
        // TODO Auto-generated method stub
        return null;
    }

    public Literal createLiteral(byte arg0) {
        // TODO Auto-generated method stub
        return null;
    }

    public Literal createLiteral(short arg0) {
        // TODO Auto-generated method stub
        return null;
    }

    public Literal createLiteral(int arg0) {
        // TODO Auto-generated method stub
        return null;
    }

    public Literal createLiteral(long arg0) {
        // TODO Auto-generated method stub
        return null;
    }

    public Literal createLiteral(float arg0) {
        // TODO Auto-generated method stub
        return null;
    }

    public Literal createLiteral(double arg0) {
        // TODO Auto-generated method stub
        return null;
    }

    public Literal createLiteral(XMLGregorianCalendar arg0) {
        // TODO Auto-generated method stub
        return null;
    }

}
