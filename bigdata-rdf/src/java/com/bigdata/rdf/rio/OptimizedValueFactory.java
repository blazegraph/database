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

package com.bigdata.rdf.rio;

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

public class OptimizedValueFactory implements ValueFactory {

    public BNode createBNode() {

        return new _BNode();

    }

    public BNode createBNode(String id) {

        //            _BNode bnode = null;
        //            
        //            if(bnodeMap!=null) bnode = bnodeMap.get( id );
        //            
        //            if ( bnode == null ) {
        //                
        //                bnode = new _BNode( id );
        //                
        //                if(bnodeMap!=null) bnodeMap.put( id, bnode );
        //                
        //            }
        //            
        //            return bnode;

        return new _BNode(id);

    }

    public Literal createLiteral(String label, String lang) {

        //            _Literal literal = null;
        //            
        //            if( literalMap!=null) literalMap.get( label );
        //            
        //            if ( literal == null ) {
        //                
        //                literal = new _Literal( label );
        //                
        //                if(literalMap!=null) literalMap.put( label, literal );
        //                
        //            }
        //            
        //            literal.language = lang;
        //            
        //            return literal;

        return new _Literal(label, lang);

    }

    public Literal createLiteral(String label, URI datatype) {

        //            _Literal literal = null;
        //            
        //            if( literalMap!=null) literalMap.get( label );
        //            
        //            if ( literal == null ) {
        //                
        //                literal = new _Literal( label );
        //                
        //                if(literalMap!=null) literalMap.put( label, literal );
        //                
        //            }
        //            
        //            literal.datatype = datatype;
        //            
        //            return literal;

        return new _Literal(label, datatype);

    }

    public Literal createLiteral(String label) {

        //            _Literal literal = null;
        //            
        //            if( literalMap!=null) literalMap.get( label );
        //            
        //            if ( literal == null ) {
        //                
        //                literal = new _Literal( label );
        //                
        //                if(literalMap!=null) literalMap.put( label, literal );
        //                
        //            }
        //            
        //            return literal;

        return new _Literal(label);

    }

    public Statement createStatement(Resource s, URI p, Value o) {

        return new _Statement((_Resource) s, (_URI) p, (_Value) o);

    }

    public URI createURI(String namespace, String localName) {

        return createURI(namespace + localName);

    }

    public URI createURI(String uriStr) {

        //            _URI uri = null;
        //            
        //            if(uriMap!=null) uriMap.get( uriStr );
        //            
        //            if ( uri == null ) {
        //                
        //                uri = new _URI( uriStr );
        //                
        //                if(uriMap!=null) uriMap.put( uriStr, uri );
        //                
        //            }
        //            
        //            return uri;

        return new _URI(uriStr);

    }

    public static class _Value implements Value {

        public final String term;
        
        public long termId;

        /**
         * The sort key under which this value will be placed into the terms
         * index.
         * 
         * @see RdfKeyBuilder
         */
        public byte[] key;

        public _Value(String term) {

            this.term = term;

        }

        public void setTermId(long termId) {

            this.termId = termId;

        }

        public long getTermId() {

            return termId;

        }

        public String getTerm() {

            return term;

        }

        public StatementIterator getObjectStatements() throws GraphException {
            throw new UnsupportedOperationException();
        }

        /**
         * @todo this does not handle typed literals correctly.
         */
        public int compareTo(Object o) {

            if (o == this) {

                return 0;

            }

            if (o instanceof _Value) {

                return ((_Value) o).term.compareTo(term);

            }

            return -1;

        }

        public boolean equals(Object o) {

            return compareTo(o) == 0;

        }

        /**
         * @todo this does not handle typed literals correctly.
         */
        public int hashCode() {

            return term.hashCode();

        }

        public String toString() {

            return term;

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

        public static final transient _ValueSortKeyComparator INSTANCE = new _ValueSortKeyComparator();;

        public int compare(_Value o1, _Value o2) {

            return BytesUtil.UnsignedByteArrayComparator.INSTANCE.compare(
                    o1.key, o2.key);

        }

    }

    public static class _Resource extends _Value implements Resource {

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

    public static class _BNode extends _Resource implements BNode {

        public _BNode() {

            super(UUID.randomUUID().toString());

        }

        public _BNode(String id) {

            super(id);

        }

        public String getID() {

            return term;

        }

    }

    public static class _Literal extends _Value implements Literal {

        String language;

        URI datatype;

        public _Literal(String label) {

            super(label);

        }

        public _Literal(String label, String language) {

            super(label);

            this.language = language;

        }

        public _Literal(String label, URI datatype) {

            super(label);

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

    }

    public static class _URI extends _Resource implements URI {

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

    }

    public static class _Statement implements Statement {

        public final _Resource s;

        public final _URI p;

        public final _Value o;

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

        /**
         * Imposes s:p:o ordering based on termIds.
         */
        public int compareTo(Object other) {

            if (other == this) {

                return 0;

            }

            _Statement stmt = (_Statement) other;

            long ret = s.termId - stmt.s.termId;
            if (ret == 0) {
                ret = p.termId - stmt.p.termId;
                if (ret == 0) {
                    ret = o.termId - stmt.o.termId;
                }
            }

            if (ret == 0)
                return 0;
            if (ret > 0)
                return 1;
            return -1;

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

            long ret = stmt1.s.termId - stmt2.s.termId;

            if (ret == 0) {

                ret = stmt1.p.termId - stmt2.p.termId;

                if (ret == 0) {

                    ret = stmt1.o.termId - stmt2.o.termId;

                }

            }

            if (ret == 0)
                return 0;
            if (ret > 0)
                return 1;
            return -1;

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

            long ret = stmt1.p.termId - stmt2.p.termId;

            if (ret == 0) {

                ret = stmt1.o.termId - stmt2.o.termId;

                if (ret == 0) {

                    ret = stmt1.s.termId - stmt2.s.termId;

                }

            }

            if (ret == 0)
                return 0;
            if (ret > 0)
                return 1;
            return -1;

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

            long ret = stmt1.o.termId - stmt2.o.termId;

            if (ret == 0) {

                ret = stmt1.s.termId - stmt2.s.termId;

                if (ret == 0) {

                    ret = stmt1.p.termId - stmt2.p.termId;

                }

            }

            if (ret == 0)
                return 0;
            if (ret > 0)
                return 1;
            return -1;

        }

    }

}
