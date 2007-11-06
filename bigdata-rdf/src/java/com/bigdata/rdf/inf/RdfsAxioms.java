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
 * Created on Mar 30, 2005
 */
package com.bigdata.rdf.inf;

import org.openrdf.vocabulary.RDF;
import org.openrdf.vocabulary.RDFS;

import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * The axioms for RDF Schema.
 * 
 * @author personickm
 */
public class RdfsAxioms extends BaseAxioms {
    
    protected RdfsAxioms(AbstractTripleStore db)
    {
        
        super(db);
        
        /*
         * RDF AXIOMATIC TRIPLES
         * 
         * @see RDF Model Theory: http://www.w3.org/TR/rdf-mt/ section 3.1
         */ 
        
        addAxiom( RDF.TYPE, RDF.TYPE, RDF.PROPERTY );
        addAxiom( RDF.SUBJECT, RDF.TYPE, RDF.PROPERTY );
        addAxiom( RDF.PREDICATE, RDF.TYPE, RDF.PROPERTY );  
        addAxiom( RDF.OBJECT, RDF.TYPE, RDF.PROPERTY );
        addAxiom( RDF.FIRST, RDF.TYPE, RDF.PROPERTY );
        addAxiom( RDF.REST, RDF.TYPE, RDF.PROPERTY );
        addAxiom( RDF.VALUE, RDF.TYPE, RDF.PROPERTY );
        addAxiom( RDF.NIL, RDF.TYPE, RDF.LIST );

        /*
         * RDFS AXIOMATIC TRIPLES
         * 
         * @see RDF Model Theory: http://www.w3.org/TR/rdf-mt/ section 4.1
         */ 

        addAxiom( RDF.TYPE, RDFS.DOMAIN, RDFS.RESOURCE ); 
        addAxiom( RDFS.DOMAIN, RDFS.DOMAIN, RDF.PROPERTY );
        addAxiom( RDFS.RANGE, RDFS.DOMAIN, RDF.PROPERTY );
        addAxiom( RDFS.SUBPROPERTYOF, RDFS.DOMAIN, RDF.PROPERTY ); 
        addAxiom( RDFS.SUBCLASSOF, RDFS.DOMAIN, RDFS.CLASS );
        addAxiom( RDF.SUBJECT, RDFS.DOMAIN, RDF.STATEMENT );
        addAxiom( RDF.PREDICATE, RDFS.DOMAIN, RDF.STATEMENT );
        addAxiom( RDF.OBJECT, RDFS.DOMAIN, RDF.STATEMENT );
        addAxiom( RDFS.MEMBER, RDFS.DOMAIN, RDFS.RESOURCE );
        addAxiom( RDF.FIRST, RDFS.DOMAIN, RDF.LIST );
        addAxiom( RDF.REST, RDFS.DOMAIN, RDF.LIST );
        addAxiom( RDFS.SEEALSO, RDFS.DOMAIN, RDFS.RESOURCE ); 
        addAxiom( RDFS.ISDEFINEDBY, RDFS.DOMAIN, RDFS.RESOURCE );
        addAxiom( RDFS.COMMENT, RDFS.DOMAIN, RDFS.RESOURCE );
        addAxiom( RDFS.LABEL, RDFS.DOMAIN, RDFS.RESOURCE );
        addAxiom( RDF.VALUE, RDFS.DOMAIN, RDFS.RESOURCE );
        addAxiom( RDF.TYPE, RDFS.RANGE, RDFS.CLASS );
        addAxiom( RDFS.DOMAIN, RDFS.RANGE, RDFS.CLASS );
        addAxiom( RDFS.RANGE, RDFS.RANGE, RDFS.CLASS );
        addAxiom( RDFS.SUBPROPERTYOF, RDFS.RANGE, RDF.PROPERTY );
        addAxiom( RDFS.SUBCLASSOF, RDFS.RANGE, RDFS.CLASS );
        addAxiom( RDF.SUBJECT, RDFS.RANGE, RDFS.RESOURCE );
        addAxiom( RDF.PREDICATE, RDFS.RANGE, RDFS.RESOURCE );
        addAxiom( RDF.OBJECT, RDFS.RANGE, RDFS.RESOURCE );
        addAxiom( RDFS.MEMBER, RDFS.RANGE, RDFS.RESOURCE );
        addAxiom( RDF.FIRST, RDFS.RANGE, RDFS.RESOURCE );
        addAxiom( RDF.REST, RDFS.RANGE, RDF.LIST );
        addAxiom( RDFS.SEEALSO, RDFS.RANGE, RDFS.RESOURCE );
        addAxiom( RDFS.ISDEFINEDBY, RDFS.RANGE, RDFS.RESOURCE );
        addAxiom( RDFS.COMMENT, RDFS.RANGE, RDFS.LITERAL );
        addAxiom( RDFS.LABEL, RDFS.RANGE, RDFS.LITERAL );
        addAxiom( RDF.VALUE, RDFS.RANGE, RDFS.RESOURCE );
        addAxiom( RDF.ALT, RDFS.SUBCLASSOF, RDFS.CONTAINER );
        addAxiom( RDF.BAG, RDFS.SUBCLASSOF, RDFS.CONTAINER );
        addAxiom( RDF.SEQ, RDFS.SUBCLASSOF, RDFS.CONTAINER );
        addAxiom( RDFS.CONTAINERMEMBERSHIPPROPERTY, RDFS.SUBCLASSOF, RDF.PROPERTY ); 
        addAxiom( RDFS.ISDEFINEDBY, RDFS.SUBPROPERTYOF, RDFS.SEEALSO );
        addAxiom( RDF.XMLLITERAL, RDF.TYPE, RDFS.DATATYPE );
        addAxiom( RDF.XMLLITERAL, RDFS.SUBCLASSOF, RDFS.LITERAL ); 
        addAxiom( RDFS.DATATYPE, RDFS.SUBCLASSOF, RDFS.CLASS );
        
        // closure of above axioms yields below semi-axioms
        
        addAxiom( RDFS.RESOURCE, RDF.TYPE, RDFS.CLASS ); 
        addAxiom( RDF.OBJECT, RDF.TYPE, RDFS.RESOURCE ); 
        addAxiom( RDF.REST, RDF.TYPE, RDFS.RESOURCE ); 
        addAxiom( RDF.XMLLITERAL, RDF.TYPE, RDFS.RESOURCE ); 
        addAxiom( RDF.NIL, RDF.TYPE, RDFS.RESOURCE ); 
        addAxiom( RDF.FIRST, RDF.TYPE, RDFS.RESOURCE ); 
        addAxiom( RDF.VALUE, RDF.TYPE, RDFS.RESOURCE ); 
        addAxiom( RDF.SUBJECT, RDF.TYPE, RDFS.RESOURCE );
        addAxiom( RDF.TYPE, RDF.TYPE, RDFS.RESOURCE ); 
        addAxiom( RDF.PREDICATE, RDF.TYPE, RDFS.RESOURCE ); 
        addAxiom( RDFS.RESOURCE, RDF.TYPE, RDFS.RESOURCE  );
        addAxiom( RDFS.DOMAIN, RDF.TYPE, RDF.PROPERTY ); 
        addAxiom( RDFS.RESOURCE, RDFS.SUBCLASSOF, RDFS.RESOURCE ); 
        addAxiom( RDFS.CLASS, RDF.TYPE, RDFS.CLASS ); 
        addAxiom( RDF.PROPERTY, RDF.TYPE, RDFS.CLASS ); 
        addAxiom( RDFS.CLASS, RDF.TYPE, RDFS.RESOURCE ); 
        addAxiom( RDFS.DOMAIN, RDF.TYPE, RDFS.RESOURCE ); 
        addAxiom( RDF.PROPERTY, RDF.TYPE, RDFS.RESOURCE ); 
        addAxiom( RDFS.DOMAIN, RDFS.SUBPROPERTYOF, RDFS.DOMAIN ); 
        addAxiom( RDFS.CLASS, RDFS.SUBCLASSOF, RDFS.CLASS ); 
        addAxiom( RDF.PROPERTY, RDFS.SUBCLASSOF, RDF.PROPERTY ); 
        addAxiom( RDFS.CLASS, RDFS.SUBCLASSOF, RDFS.RESOURCE ); 
        addAxiom( RDF.PROPERTY, RDFS.SUBCLASSOF, RDFS.RESOURCE ); 
        addAxiom( RDFS.DATATYPE, RDFS.SUBCLASSOF, RDFS.RESOURCE ); 
        addAxiom( RDFS.CONTAINERMEMBERSHIPPROPERTY, RDFS.SUBCLASSOF, RDFS.RESOURCE ); 
        addAxiom( RDFS.SEEALSO, RDF.TYPE, RDF.PROPERTY ); 
        addAxiom( RDFS.RANGE, RDF.TYPE, RDF.PROPERTY ); 
        addAxiom( RDFS.SEEALSO, RDF.TYPE, RDFS.RESOURCE ); 
        addAxiom( RDFS.RANGE, RDF.TYPE, RDFS.RESOURCE ); 
        addAxiom( RDFS.SEEALSO, RDFS.SUBPROPERTYOF, RDFS.SEEALSO ); 
        addAxiom( RDFS.RANGE, RDFS.SUBPROPERTYOF, RDFS.RANGE ); 
        addAxiom( RDF.STATEMENT, RDF.TYPE, RDFS.CLASS ); 
        addAxiom( RDF.STATEMENT, RDF.TYPE, RDFS.RESOURCE ); 
        addAxiom( RDF.STATEMENT, RDFS.SUBCLASSOF, RDF.STATEMENT ); 
        addAxiom( RDF.STATEMENT, RDFS.SUBCLASSOF, RDFS.RESOURCE ); 
        addAxiom( RDFS.LITERAL, RDF.TYPE, RDFS.CLASS ); 
        addAxiom( RDFS.LABEL, RDF.TYPE, RDF.PROPERTY ); 
        addAxiom( RDFS.LABEL, RDF.TYPE, RDFS.RESOURCE ); 
        addAxiom( RDFS.LITERAL, RDF.TYPE, RDFS.RESOURCE ); 
        addAxiom( RDFS.LITERAL, RDFS.SUBCLASSOF, RDFS.LITERAL ); 
        addAxiom( RDFS.LABEL, RDFS.SUBPROPERTYOF, RDFS.LABEL ); 
        addAxiom( RDFS.LITERAL, RDFS.SUBCLASSOF, RDFS.RESOURCE ); 
        addAxiom( RDF.XMLLITERAL, RDFS.SUBCLASSOF, RDFS.RESOURCE ); 
        addAxiom( RDFS.CONTAINER, RDF.TYPE, RDFS.CLASS ); 
        addAxiom( RDF.ALT, RDF.TYPE, RDFS.CLASS ); 
        addAxiom( RDFS.SUBCLASSOF, RDF.TYPE, RDF.PROPERTY ); 
        addAxiom( RDF.ALT, RDF.TYPE, RDFS.RESOURCE ); 
        addAxiom( RDFS.CONTAINER, RDF.TYPE, RDFS.RESOURCE ); 
        addAxiom( RDFS.CONTAINER, RDFS.SUBCLASSOF, RDFS.CONTAINER ); 
        addAxiom( RDF.ALT, RDFS.SUBCLASSOF, RDF.ALT ); 
        addAxiom( RDFS.SUBCLASSOF, RDF.TYPE, RDFS.RESOURCE ); 
        addAxiom( RDFS.SUBCLASSOF, RDFS.SUBPROPERTYOF, RDFS.SUBCLASSOF ); 
        addAxiom( RDFS.CONTAINER, RDFS.SUBCLASSOF, RDFS.RESOURCE ); 
        addAxiom( RDF.ALT, RDFS.SUBCLASSOF, RDFS.RESOURCE ); 
        addAxiom( RDF.SEQ, RDFS.SUBCLASSOF, RDFS.RESOURCE ); 
        addAxiom( RDF.BAG, RDFS.SUBCLASSOF, RDFS.RESOURCE ); 
        addAxiom( RDF.OBJECT, RDFS.SUBPROPERTYOF, RDF.OBJECT ); 
        addAxiom( RDFS.MEMBER, RDF.TYPE, RDF.PROPERTY ); 
        addAxiom( RDFS.MEMBER, RDF.TYPE, RDFS.RESOURCE ); 
        addAxiom( RDFS.MEMBER, RDFS.SUBPROPERTYOF, RDFS.MEMBER ); 
        addAxiom( RDFS.ISDEFINEDBY, RDF.TYPE, RDF.PROPERTY ); 
        addAxiom( RDFS.SUBPROPERTYOF, RDF.TYPE, RDF.PROPERTY ); 
        addAxiom( RDFS.SUBPROPERTYOF, RDF.TYPE, RDFS.RESOURCE ); 
        addAxiom( RDFS.ISDEFINEDBY, RDF.TYPE, RDFS.RESOURCE );
        addAxiom( RDFS.ISDEFINEDBY, RDFS.SUBPROPERTYOF, RDFS.ISDEFINEDBY ); 
        addAxiom( RDFS.SUBPROPERTYOF, RDFS.SUBPROPERTYOF, RDFS.SUBPROPERTYOF ); 
        addAxiom( RDFS.DATATYPE, RDF.TYPE, RDFS.CLASS ); 
        addAxiom( RDF.XMLLITERAL, RDF.TYPE, RDFS.CLASS ); 
        addAxiom( RDFS.DATATYPE, RDF.TYPE, RDFS.RESOURCE ); 
        addAxiom( RDFS.DATATYPE, RDFS.SUBCLASSOF, RDFS.DATATYPE ); 
        addAxiom( RDF.XMLLITERAL, RDFS.SUBCLASSOF, RDF.XMLLITERAL ); 
        addAxiom( RDF.REST, RDFS.SUBPROPERTYOF, RDF.REST ); 
        addAxiom( RDF.SEQ, RDF.TYPE, RDFS.CLASS ); 
        addAxiom( RDF.SEQ, RDF.TYPE, RDFS.RESOURCE ); 
        addAxiom( RDF.SEQ, RDFS.SUBCLASSOF, RDF.SEQ  );
        addAxiom( RDF.LIST, RDF.TYPE, RDFS.CLASS ); 
        addAxiom( RDF.LIST, RDF.TYPE, RDFS.RESOURCE ); 
        addAxiom( RDF.LIST, RDFS.SUBCLASSOF, RDF.LIST ); 
        addAxiom( RDF.LIST, RDFS.SUBCLASSOF, RDFS.RESOURCE ); 
        addAxiom( RDFS.COMMENT, RDF.TYPE, RDF.PROPERTY ); 
        addAxiom( RDFS.COMMENT, RDF.TYPE, RDFS.RESOURCE ); 
        addAxiom( RDFS.COMMENT, RDFS.SUBPROPERTYOF, RDFS.COMMENT ); 
        addAxiom( RDF.BAG, RDF.TYPE, RDFS.CLASS ); 
        addAxiom( RDF.BAG, RDF.TYPE, RDFS.RESOURCE ); 
        addAxiom( RDF.BAG, RDFS.SUBCLASSOF, RDF.BAG ); 
        addAxiom( RDF.FIRST, RDFS.SUBPROPERTYOF, RDF.FIRST ); 
        addAxiom( RDF.VALUE, RDFS.SUBPROPERTYOF, RDF.VALUE ); 
        addAxiom( RDFS.CONTAINERMEMBERSHIPPROPERTY, RDF.TYPE, RDFS.CLASS ); 
        addAxiom( RDFS.CONTAINERMEMBERSHIPPROPERTY, RDFS.SUBCLASSOF, RDFS.CONTAINERMEMBERSHIPPROPERTY); 
        addAxiom( RDFS.CONTAINERMEMBERSHIPPROPERTY, RDF.TYPE, RDFS.RESOURCE );
        addAxiom( RDF.SUBJECT, RDFS.SUBPROPERTYOF, RDF.SUBJECT );
        addAxiom( RDF.TYPE, RDFS.SUBPROPERTYOF, RDF.TYPE );
        addAxiom( RDF.PREDICATE, RDFS.SUBPROPERTYOF, RDF.PREDICATE );         
        
    }

}
