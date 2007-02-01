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
 * Created on Jan 26, 2007
 */

package com.bigdata.rdf.model;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Comparator;
import java.util.UUID;

import org.openrdf.model.BNode;
import org.openrdf.model.GraphException;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.sesame.sail.StatementIterator;

import com.bigdata.objndx.BytesUtil;
import com.bigdata.rdf.RdfKeyBuilder;
import com.bigdata.rdf.TripleStore;

public class OptimizedValueFactory implements ValueFactory {

    public BNode createBNode() {

        return new _BNode();

    }

    public BNode createBNode(String id) {

        return new _BNode(id);

    }

    public Literal createLiteral(String label, String lang) {

        return new _Literal(label, lang);

    }

    public Literal createLiteral(String label, URI datatype) {

        return new _Literal(label, (_URI)datatype);

    }

    public Literal createLiteral(String label) {

        return new _Literal(label);

    }

    public Statement createStatement(Resource s, URI p, Value o) {

        return new _Statement((_Resource) s, (_URI) p, (_Value) o);

    }

    public URI createURI(String namespace, String localName) {

        return createURI(namespace + localName);

    }

    public URI createURI(String uriStr) {

        return new _URI(uriStr);

    }

    /**
     * An RDF {@link Value} base class designed to support batch and bulk
     * loading of parsed RDF data.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    abstract public static class _Value implements Value, Externalizable {

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
         * The sort key under which this value will be placed into the
         * {@link TripleStore#ndx_termId terms} index. The sort key is a
         * representation of the total term, including any additional attributes
         * such as the language code or the datatype URI.
         * 
         * @see RdfKeyBuilder
         */
        public byte[] key = null;

        /**
         * The term identifier assigned to this term by the
         * {@link TripleStore#ndx_termId terms} index and the identifier under
         * which the lexical item may be recovered from the
         * {@link TripleStore#ndx_idTerm term identifiers} index.
         */
        public long termId = 0;

        /**
         * The #of times that this term has been used in a {@link Statement}.
         */
        public int count = 0;
        
//        /**
//         * Initially <code>false</code>, this field is set <code>true</code>
//         * if multiple occurrences of the same term are identified during an
//         * index load operation. Duplicate filtering is required only for bulk
//         * loads into {@link IndexSegments} since the inputs to that process are
//         * presumed to be an ordered set of distinct key-value data. While
//         * duplicate filtering is not always performed, a term is a duplicate if
//         * this field is set.
//         */
//        public boolean duplicate = false;
        
        /**
         * Initially <code>false</code>, this field is set <code>true</code>
         * if it is determined that a term has already been assigned a term
         * identifier and is therefore in both the
         * {@link TripleStore#ndx_termId terms} index and the
         * {@link TripleStore#ndx_idTerm term identifiers} index. This is used
         * to avoid re-definition of terms in the term identifiers index during
         * a bulk load operation.
         */
        public boolean known = false;
        
        public _Value(String term) {

            this.term = term;

        }

        public StatementIterator getObjectStatements() throws GraphException {

            throw new UnsupportedOperationException();
            
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

        public String toString() {

            return term + " {termId=" + termId + ", haveKey=" + (key != null)
                    /* + ", dup=" + duplicate + */
                    + ", known=" + known + "}";

        }

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

        public _Resource(String term) {

            super(term);

        }

        public void addProperty(URI arg0, Value arg1) throws GraphException {
            throw new UnsupportedOperationException();
        }

        public StatementIterator getSubjectStatements() throws GraphException {
            throw new UnsupportedOperationException();
        }

    }

    final public static class _BNode extends _Resource implements BNode {

        /**
         * 
         */
        private static final long serialVersionUID = 8835732082253951776L;

        public _BNode() {

            super(UUID.randomUUID().toString());

        }

        public _BNode(String id) {

            super(id);

        }
        
        public String getID() {

            return term;

        }

        /**
         * @return {@link RdfKeyBuilder#CODE_BND}.
         */
        public byte getTermCode() {
            
            return RdfKeyBuilder.CODE_BND;
            
        }

        public boolean equals(Object o) {

            if (o == this) return true;

            if( ! (o instanceof _BNode )) return false;
            
            _BNode oval = (_BNode)o;

            return term.equals(oval.term);

        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            
            term = in.readUTF();
            
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            
            out.writeUTF(term);
            
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

        byte code;
        
        public String language;

        public _URI datatype;

        /**
         * @return one of {@link RdfKeyBuilder#CODE_LIT plain literal},
         *         {@link RdfKeyBuilder#CODE_LCL language code literal} or
         *         {@link RdfKeyBuilder#CODE_DTL datatype literal}.
         */
        public byte getTermCode() {
            
            return code;
            
        }

        public _Literal(String label) {

            super(label);
            
            this.code = RdfKeyBuilder.CODE_LIT;

            this.language = null;
            
            this.datatype = null;
            
        }

        public _Literal(String label, String language) {

            super(label);

            this.code = RdfKeyBuilder.CODE_LCL;

            this.language = language;

            this.datatype = null;
            
        }

        public _Literal(String label, _URI datatype) {

            super(label);

            this.code = RdfKeyBuilder.CODE_DTL;

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
            
            if(code==RdfKeyBuilder.CODE_LIT) {
                
                return true;
                
            } else if(code==RdfKeyBuilder.CODE_LCL) {

                // check language code.
                return language.equals(oval.language);
                
            } else if(code==RdfKeyBuilder.CODE_DTL) {

                // check datatype.
                return datatype.equals(oval.datatype);

            } else {
                
                throw new AssertionError();
                
            }

        }
        
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

            code = in.readByte();
            
            term = in.readUTF();
            
            if(code == RdfKeyBuilder.CODE_LCL) {
                
                language = in.readUTF();
                
            } else if(code == RdfKeyBuilder.CODE_DTL ) {
                
                datatype = new _URI(in.readUTF());
                
            }
            
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            
            out.writeByte(code);

            out.writeUTF(term);
            
            if (language != null) {
            
                out.writeUTF(language);
                
            } else if (language != null) {
                
                out.writeUTF(datatype.getURI());
                
            }
            
        }

    }

    final public static class _URI extends _Resource implements URI {
        
        /**
         * 
         */
        private static final long serialVersionUID = 8085405245340777144L;

        public _URI(String uri) {

            super(uri);

        }

        public _URI(String namespace, String localName) {

            super(namespace + localName);

        }

        public String getURI() {

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

        public StatementIterator getPredicateStatements() throws GraphException {
            throw new UnsupportedOperationException();
        }

        /**
         * @return {@link RdfKeyBuilder#CODE_URI}.
         */
        public byte getTermCode() {

            return RdfKeyBuilder.CODE_URI;
            
        }

        public boolean equals(Object o) {

            if (o == this) return true;

            if( ! (o instanceof _URI )) return false;
            
            _URI oval = (_URI)o;

            return term.equals(oval.term);

        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            
            term = in.readUTF();
            
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            
            out.writeUTF(term);
            
        }

    }

    public static class _Statement implements Statement {

        /**
         * 
         */
        private static final long serialVersionUID = 3259278872142669482L;

        public final _Resource s;

        public final _URI p;

        public final _Value o;

        /**
         * The #of times this statement is encountered within a
         * {@link com.bigdata.rdf.rio.Buffer}.
         */
        public int count = 0;
        
        /**
         * Initially <code>false</code>, this field is set <code>true</code>
         * if it is determined that a statement is known to the
         * {@link StatementIndex statement indices} maintained by the
         * {@link TripleStore}. This is used to avoid re-definition of
         * statements in the during a bulk load operation where duplicate keys
         * would violate the B+-Tree unique key constraint.
         */
        public boolean known = false;
        
        public _Statement(_Resource s, _URI p, _Value o) {

            this.s = s;

            this.p = p;

            this.o = o;

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

        public boolean equals(_Statement stmt) {
            
            return (s.termId == stmt.s.termId) && //
                   (p.termId == stmt.p.termId) && //
                   (o.termId == stmt.o.termId);
            
        }
        
        /**
         * Imposes s:p:o ordering based on termIds.
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

}
