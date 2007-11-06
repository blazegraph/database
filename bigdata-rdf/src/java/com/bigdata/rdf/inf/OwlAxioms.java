package com.bigdata.rdf.inf;

import org.openrdf.vocabulary.OWL;
import org.openrdf.vocabulary.RDF;
import org.openrdf.vocabulary.RDFS;

import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * The axioms for RDF Schema plus a few axioms to support owl:sameAs,
 * owl:equivalentProperty, and owl:equivalentClass.
 * 
 * @author personickm
 */
public class OwlAxioms extends RdfsAxioms {
    
    protected OwlAxioms(AbstractTripleStore db)
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
