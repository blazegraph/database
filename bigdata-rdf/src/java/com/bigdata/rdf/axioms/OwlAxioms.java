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
package com.bigdata.rdf.axioms;

import java.util.Collection;

import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * The axioms for RDF Schema plus a few axioms to support owl:sameAs,
 * owl:equivalentProperty, and owl:equivalentClass.
 * 
 * @author personickm
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class OwlAxioms extends RdfsAxioms {
    
    /**
     * 
     */
    private static final long serialVersionUID = -5941985779347312838L;

    /**
     * De-serialization ctor.
     */
    public OwlAxioms() {

        super();
        
    }
    
    public OwlAxioms(AbstractTripleStore db) {
        
        super(db);
        
    }
    
    protected void addAxioms(Collection<BigdataStatement> axioms) {

        super.addAxioms(axioms);

        final BigdataValueFactory valueFactory = getValueFactory();
        
        // axioms for owl:equivalentClass
        axioms.add( valueFactory.createStatement( OWL.EQUIVALENTCLASS, RDFS.SUBPROPERTYOF, RDFS.SUBCLASSOF, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( OWL.EQUIVALENTCLASS, RDFS.SUBPROPERTYOF, OWL.EQUIVALENTCLASS, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( OWL.EQUIVALENTCLASS, RDF.TYPE, RDF.PROPERTY, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( OWL.EQUIVALENTCLASS, RDF.TYPE, RDFS.RESOURCE, null, StatementEnum.Axiom));

        // axioms for owl:equivalentProperty
        axioms.add( valueFactory.createStatement( OWL.EQUIVALENTPROPERTY, RDFS.SUBPROPERTYOF, RDFS.SUBPROPERTYOF, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( OWL.EQUIVALENTPROPERTY, RDFS.SUBPROPERTYOF, OWL.EQUIVALENTPROPERTY, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( OWL.EQUIVALENTPROPERTY, RDF.TYPE, RDF.PROPERTY, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( OWL.EQUIVALENTPROPERTY, RDF.TYPE, RDFS.RESOURCE, null, StatementEnum.Axiom));

        // axioms for owl:sameAs
        axioms.add( valueFactory.createStatement( OWL.SAMEAS, RDF.TYPE, RDF.PROPERTY, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( OWL.SAMEAS, RDF.TYPE, RDFS.RESOURCE, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( OWL.SAMEAS, RDFS.SUBPROPERTYOF, OWL.SAMEAS, null, StatementEnum.Axiom));
 
        // axioms for owl:inverseOf
        axioms.add( valueFactory.createStatement( OWL.INVERSEOF, RDF.TYPE, RDF.PROPERTY, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( OWL.INVERSEOF, RDF.TYPE, RDFS.RESOURCE, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( OWL.INVERSEOF, RDFS.SUBPROPERTYOF, OWL.INVERSEOF, null, StatementEnum.Axiom));
 
        // axioms for owl:Class, owl:ObjectProperty, owl:TransitiveProperty, and owl:DatatypeProperty
        axioms.add( valueFactory.createStatement( OWL.CLASS, RDFS.SUBCLASSOF, RDFS.CLASS, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( OWL.CLASS, RDFS.SUBCLASSOF, RDFS.RESOURCE, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( OWL.CLASS, RDFS.SUBCLASSOF, OWL.CLASS, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( OWL.CLASS, RDF.TYPE, RDFS.CLASS, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( OWL.CLASS, RDF.TYPE, RDFS.RESOURCE, null, StatementEnum.Axiom));
        
        axioms.add( valueFactory.createStatement( OWL.OBJECTPROPERTY, RDFS.SUBCLASSOF, RDF.PROPERTY, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( OWL.OBJECTPROPERTY, RDFS.SUBCLASSOF, RDFS.RESOURCE, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( OWL.OBJECTPROPERTY, RDFS.SUBCLASSOF, OWL.OBJECTPROPERTY, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( OWL.OBJECTPROPERTY, RDF.TYPE, RDFS.CLASS, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( OWL.OBJECTPROPERTY, RDF.TYPE, RDFS.RESOURCE, null, StatementEnum.Axiom));

        axioms.add( valueFactory.createStatement( OWL.TRANSITIVEPROPERTY, RDFS.SUBCLASSOF, OWL.OBJECTPROPERTY, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( OWL.TRANSITIVEPROPERTY, RDFS.SUBCLASSOF, RDF.PROPERTY, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( OWL.TRANSITIVEPROPERTY, RDFS.SUBCLASSOF, RDFS.RESOURCE, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( OWL.TRANSITIVEPROPERTY, RDFS.SUBCLASSOF, OWL.TRANSITIVEPROPERTY, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( OWL.TRANSITIVEPROPERTY, RDF.TYPE, RDFS.CLASS, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( OWL.TRANSITIVEPROPERTY, RDF.TYPE, RDFS.RESOURCE, null, StatementEnum.Axiom));

        axioms.add( valueFactory.createStatement( OWL.DATATYPEPROPERTY, RDFS.SUBCLASSOF, RDF.PROPERTY, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( OWL.DATATYPEPROPERTY, RDFS.SUBCLASSOF, RDFS.RESOURCE, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( OWL.DATATYPEPROPERTY, RDFS.SUBCLASSOF, OWL.DATATYPEPROPERTY, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( OWL.DATATYPEPROPERTY, RDF.TYPE, RDFS.CLASS, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( OWL.DATATYPEPROPERTY, RDF.TYPE, RDFS.RESOURCE, null, StatementEnum.Axiom));
        
    }

    final public boolean isOwlSameAs() {
        
        return true;
        
    }
    
}
