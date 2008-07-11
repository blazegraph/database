/*

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
package com.bigdata.rdf.inf;

import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * The axioms for RDF Schema plus a few axioms to support owl:sameAs,
 * owl:equivalentProperty, and owl:equivalentClass.
 * 
 * @author personickm
 */
public class OwlAxioms extends RdfsAxioms {
    
    public OwlAxioms(AbstractTripleStore db)
    {
        
        super( db );
        
        // axioms for owl:equivalentClass
        addAxiom( OWL.EQUIVALENTCLASS, RDFS.SUBPROPERTYOF, RDFS.SUBCLASSOF );
        addAxiom( OWL.EQUIVALENTCLASS, RDFS.SUBPROPERTYOF, OWL.EQUIVALENTCLASS );
        addAxiom( OWL.EQUIVALENTCLASS, RDF.TYPE, RDF.PROPERTY );
        addAxiom( OWL.EQUIVALENTCLASS, RDF.TYPE, RDFS.RESOURCE );

        // axioms for owl:equivalentProperty
        addAxiom( OWL.EQUIVALENTPROPERTY, RDFS.SUBPROPERTYOF, RDFS.SUBPROPERTYOF );
        addAxiom( OWL.EQUIVALENTPROPERTY, RDFS.SUBPROPERTYOF, OWL.EQUIVALENTPROPERTY );
        addAxiom( OWL.EQUIVALENTPROPERTY, RDF.TYPE, RDF.PROPERTY );
        addAxiom( OWL.EQUIVALENTPROPERTY, RDF.TYPE, RDFS.RESOURCE );

        // axioms for owl:sameAs
        addAxiom( OWL.SAMEAS, RDF.TYPE, RDF.PROPERTY );
        addAxiom( OWL.SAMEAS, RDF.TYPE, RDFS.RESOURCE );
        addAxiom( OWL.SAMEAS, RDFS.SUBPROPERTYOF, OWL.SAMEAS );
 
    }

}
