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
 * Created on Mar 30, 2005
 */
package com.bigdata.rdf.axioms;

import java.util.Collection;

import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * The axioms for RDF Schema.
 * 
 * @author personickm
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RdfsAxioms extends BaseAxioms {
    
    /**
     * 
     */
    private static final long serialVersionUID = -5221807082849582064L;

    /**
     * De-serialization ctor.
     */
    public RdfsAxioms() {

        super();
        
    }
    
    public RdfsAxioms(AbstractTripleStore db) {

        super(db);
        
    }

    /**
     * Adds the axioms for RDF Schema.
     */
    protected void addAxioms(Collection<BigdataStatement> axioms) {

        super.addAxioms(axioms);
        
        final BigdataValueFactory valueFactory = getValueFactory();
        
        /*
         * RDF AXIOMATIC TRIPLES
         * 
         * @see RDF Model Theory: http://www.w3.org/TR/rdf-mt/ section 3.1
         */ 
        
        axioms.add( valueFactory.createStatement( RDF.TYPE, RDF.TYPE, RDF.PROPERTY, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( RDF.SUBJECT, RDF.TYPE, RDF.PROPERTY, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( RDF.PREDICATE, RDF.TYPE, RDF.PROPERTY, null, StatementEnum.Axiom));  
        axioms.add( valueFactory.createStatement( RDF.OBJECT, RDF.TYPE, RDF.PROPERTY, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( RDF.FIRST, RDF.TYPE, RDF.PROPERTY, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( RDF.REST, RDF.TYPE, RDF.PROPERTY, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( RDF.VALUE, RDF.TYPE, RDF.PROPERTY, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( RDF.NIL, RDF.TYPE, RDF.LIST, null, StatementEnum.Axiom));

        /*
         * RDFS AXIOMATIC TRIPLES
         * 
         * @see RDF Model Theory: http://www.w3.org/TR/rdf-mt/ section 4.1
         */ 

        axioms.add( valueFactory.createStatement( RDF.TYPE, RDFS.DOMAIN, RDFS.RESOURCE, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDFS.DOMAIN, RDFS.DOMAIN, RDF.PROPERTY, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( RDFS.RANGE, RDFS.DOMAIN, RDF.PROPERTY, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( RDFS.SUBPROPERTYOF, RDFS.DOMAIN, RDF.PROPERTY, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDFS.SUBCLASSOF, RDFS.DOMAIN, RDFS.CLASS, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( RDF.SUBJECT, RDFS.DOMAIN, RDF.STATEMENT, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( RDF.PREDICATE, RDFS.DOMAIN, RDF.STATEMENT, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( RDF.OBJECT, RDFS.DOMAIN, RDF.STATEMENT, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( RDFS.MEMBER, RDFS.DOMAIN, RDFS.RESOURCE, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( RDF.FIRST, RDFS.DOMAIN, RDF.LIST, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( RDF.REST, RDFS.DOMAIN, RDF.LIST, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( RDFS.SEEALSO, RDFS.DOMAIN, RDFS.RESOURCE, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDFS.ISDEFINEDBY, RDFS.DOMAIN, RDFS.RESOURCE, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( RDFS.COMMENT, RDFS.DOMAIN, RDFS.RESOURCE, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( RDFS.LABEL, RDFS.DOMAIN, RDFS.RESOURCE, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( RDF.VALUE, RDFS.DOMAIN, RDFS.RESOURCE, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( RDF.TYPE, RDFS.RANGE, RDFS.CLASS, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( RDFS.DOMAIN, RDFS.RANGE, RDFS.CLASS, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( RDFS.RANGE, RDFS.RANGE, RDFS.CLASS, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( RDFS.SUBPROPERTYOF, RDFS.RANGE, RDF.PROPERTY, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( RDFS.SUBCLASSOF, RDFS.RANGE, RDFS.CLASS, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( RDF.SUBJECT, RDFS.RANGE, RDFS.RESOURCE, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( RDF.PREDICATE, RDFS.RANGE, RDFS.RESOURCE, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( RDF.OBJECT, RDFS.RANGE, RDFS.RESOURCE, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( RDFS.MEMBER, RDFS.RANGE, RDFS.RESOURCE, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( RDF.FIRST, RDFS.RANGE, RDFS.RESOURCE, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( RDF.REST, RDFS.RANGE, RDF.LIST, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( RDFS.SEEALSO, RDFS.RANGE, RDFS.RESOURCE, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( RDFS.ISDEFINEDBY, RDFS.RANGE, RDFS.RESOURCE, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( RDFS.COMMENT, RDFS.RANGE, RDFS.LITERAL, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( RDFS.LABEL, RDFS.RANGE, RDFS.LITERAL, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( RDF.VALUE, RDFS.RANGE, RDFS.RESOURCE, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( RDF.ALT, RDFS.SUBCLASSOF, RDFS.CONTAINER, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( RDF.BAG, RDFS.SUBCLASSOF, RDFS.CONTAINER, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( RDF.SEQ, RDFS.SUBCLASSOF, RDFS.CONTAINER, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( RDFS.CONTAINERMEMBERSHIPPROPERTY, RDFS.SUBCLASSOF, RDF.PROPERTY, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDFS.ISDEFINEDBY, RDFS.SUBPROPERTYOF, RDFS.SEEALSO, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( RDF.XMLLITERAL, RDF.TYPE, RDFS.DATATYPE, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( RDF.XMLLITERAL, RDFS.SUBCLASSOF, RDFS.LITERAL, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDFS.DATATYPE, RDFS.SUBCLASSOF, RDFS.CLASS, null, StatementEnum.Axiom));
        
        // closure of above axioms yields below semi-axioms
        
        axioms.add( valueFactory.createStatement( RDFS.RESOURCE, RDF.TYPE, RDFS.CLASS, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDF.OBJECT, RDF.TYPE, RDFS.RESOURCE, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDF.REST, RDF.TYPE, RDFS.RESOURCE, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDF.XMLLITERAL, RDF.TYPE, RDFS.RESOURCE, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDF.NIL, RDF.TYPE, RDFS.RESOURCE, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDF.FIRST, RDF.TYPE, RDFS.RESOURCE, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDF.VALUE, RDF.TYPE, RDFS.RESOURCE, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDF.SUBJECT, RDF.TYPE, RDFS.RESOURCE, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( RDF.TYPE, RDF.TYPE, RDFS.RESOURCE, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDF.PREDICATE, RDF.TYPE, RDFS.RESOURCE, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDFS.RESOURCE, RDF.TYPE, RDFS.RESOURCE , null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( RDFS.DOMAIN, RDF.TYPE, RDF.PROPERTY, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDFS.RESOURCE, RDFS.SUBCLASSOF, RDFS.RESOURCE, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDFS.CLASS, RDF.TYPE, RDFS.CLASS, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDF.PROPERTY, RDF.TYPE, RDFS.CLASS, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDFS.CLASS, RDF.TYPE, RDFS.RESOURCE, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDFS.DOMAIN, RDF.TYPE, RDFS.RESOURCE, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDF.PROPERTY, RDF.TYPE, RDFS.RESOURCE, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDFS.DOMAIN, RDFS.SUBPROPERTYOF, RDFS.DOMAIN, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDFS.CLASS, RDFS.SUBCLASSOF, RDFS.CLASS, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDF.PROPERTY, RDFS.SUBCLASSOF, RDF.PROPERTY, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDFS.CLASS, RDFS.SUBCLASSOF, RDFS.RESOURCE, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDF.PROPERTY, RDFS.SUBCLASSOF, RDFS.RESOURCE, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDFS.DATATYPE, RDFS.SUBCLASSOF, RDFS.RESOURCE, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDFS.CONTAINERMEMBERSHIPPROPERTY, RDFS.SUBCLASSOF, RDFS.RESOURCE, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDFS.SEEALSO, RDF.TYPE, RDF.PROPERTY, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDFS.RANGE, RDF.TYPE, RDF.PROPERTY, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDFS.SEEALSO, RDF.TYPE, RDFS.RESOURCE, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDFS.RANGE, RDF.TYPE, RDFS.RESOURCE, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDFS.SEEALSO, RDFS.SUBPROPERTYOF, RDFS.SEEALSO, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDFS.RANGE, RDFS.SUBPROPERTYOF, RDFS.RANGE, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDF.STATEMENT, RDF.TYPE, RDFS.CLASS, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDF.STATEMENT, RDF.TYPE, RDFS.RESOURCE, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDF.STATEMENT, RDFS.SUBCLASSOF, RDF.STATEMENT, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDF.STATEMENT, RDFS.SUBCLASSOF, RDFS.RESOURCE, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDFS.LITERAL, RDF.TYPE, RDFS.CLASS, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDFS.LABEL, RDF.TYPE, RDF.PROPERTY, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDFS.LABEL, RDF.TYPE, RDFS.RESOURCE, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDFS.LITERAL, RDF.TYPE, RDFS.RESOURCE, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDFS.LITERAL, RDFS.SUBCLASSOF, RDFS.LITERAL, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDFS.LABEL, RDFS.SUBPROPERTYOF, RDFS.LABEL, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDFS.LITERAL, RDFS.SUBCLASSOF, RDFS.RESOURCE, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDF.XMLLITERAL, RDFS.SUBCLASSOF, RDFS.RESOURCE, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDFS.CONTAINER, RDF.TYPE, RDFS.CLASS, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDF.ALT, RDF.TYPE, RDFS.CLASS, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDFS.SUBCLASSOF, RDF.TYPE, RDF.PROPERTY, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDF.ALT, RDF.TYPE, RDFS.RESOURCE, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDFS.CONTAINER, RDF.TYPE, RDFS.RESOURCE, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDFS.CONTAINER, RDFS.SUBCLASSOF, RDFS.CONTAINER, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDF.ALT, RDFS.SUBCLASSOF, RDF.ALT, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDFS.SUBCLASSOF, RDF.TYPE, RDFS.RESOURCE, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDFS.SUBCLASSOF, RDFS.SUBPROPERTYOF, RDFS.SUBCLASSOF, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDFS.CONTAINER, RDFS.SUBCLASSOF, RDFS.RESOURCE, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDF.ALT, RDFS.SUBCLASSOF, RDFS.RESOURCE, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDF.SEQ, RDFS.SUBCLASSOF, RDFS.RESOURCE, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDF.BAG, RDFS.SUBCLASSOF, RDFS.RESOURCE, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDF.OBJECT, RDFS.SUBPROPERTYOF, RDF.OBJECT, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDFS.MEMBER, RDF.TYPE, RDF.PROPERTY, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDFS.MEMBER, RDF.TYPE, RDFS.RESOURCE, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDFS.MEMBER, RDFS.SUBPROPERTYOF, RDFS.MEMBER, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDFS.ISDEFINEDBY, RDF.TYPE, RDF.PROPERTY, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDFS.SUBPROPERTYOF, RDF.TYPE, RDF.PROPERTY, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDFS.SUBPROPERTYOF, RDF.TYPE, RDFS.RESOURCE, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDFS.ISDEFINEDBY, RDF.TYPE, RDFS.RESOURCE, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( RDFS.ISDEFINEDBY, RDFS.SUBPROPERTYOF, RDFS.ISDEFINEDBY, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDFS.SUBPROPERTYOF, RDFS.SUBPROPERTYOF, RDFS.SUBPROPERTYOF, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDFS.DATATYPE, RDF.TYPE, RDFS.CLASS, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDF.XMLLITERAL, RDF.TYPE, RDFS.CLASS, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDFS.DATATYPE, RDF.TYPE, RDFS.RESOURCE, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDFS.DATATYPE, RDFS.SUBCLASSOF, RDFS.DATATYPE, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDF.XMLLITERAL, RDFS.SUBCLASSOF, RDF.XMLLITERAL, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDF.REST, RDFS.SUBPROPERTYOF, RDF.REST, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDF.SEQ, RDF.TYPE, RDFS.CLASS, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDF.SEQ, RDF.TYPE, RDFS.RESOURCE, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDF.SEQ, RDFS.SUBCLASSOF, RDF.SEQ, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( RDF.LIST, RDF.TYPE, RDFS.CLASS, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDF.LIST, RDF.TYPE, RDFS.RESOURCE, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDF.LIST, RDFS.SUBCLASSOF, RDF.LIST, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDF.LIST, RDFS.SUBCLASSOF, RDFS.RESOURCE, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDFS.COMMENT, RDF.TYPE, RDF.PROPERTY, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDFS.COMMENT, RDF.TYPE, RDFS.RESOURCE, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDFS.COMMENT, RDFS.SUBPROPERTYOF, RDFS.COMMENT, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDF.BAG, RDF.TYPE, RDFS.CLASS, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDF.BAG, RDF.TYPE, RDFS.RESOURCE, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDF.BAG, RDFS.SUBCLASSOF, RDF.BAG, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDF.FIRST, RDFS.SUBPROPERTYOF, RDF.FIRST, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDF.VALUE, RDFS.SUBPROPERTYOF, RDF.VALUE, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDFS.CONTAINERMEMBERSHIPPROPERTY, RDF.TYPE, RDFS.CLASS, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDFS.CONTAINERMEMBERSHIPPROPERTY, RDFS.SUBCLASSOF, RDFS.CONTAINERMEMBERSHIPPROPERTY, null, StatementEnum.Axiom)); 
        axioms.add( valueFactory.createStatement( RDFS.CONTAINERMEMBERSHIPPROPERTY, RDF.TYPE, RDFS.RESOURCE, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( RDF.SUBJECT, RDFS.SUBPROPERTYOF, RDF.SUBJECT, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( RDF.TYPE, RDFS.SUBPROPERTYOF, RDF.TYPE, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( RDF.PREDICATE, RDFS.SUBPROPERTYOF, RDF.PREDICATE, null, StatementEnum.Axiom));         
        
    }

    final public boolean isNone() {
        
        return false;
        
    }
    
    final public boolean isRdfSchema() {
        
        return true;
        
    }
    
    public boolean isOwlSameAs() {
        
        return false;
        
    }
    
}
