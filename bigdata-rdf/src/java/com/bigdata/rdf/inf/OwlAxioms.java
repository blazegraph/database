package com.bigdata.rdf.inf;

import org.openrdf.vocabulary.OWL;
import org.openrdf.vocabulary.RDF;
import org.openrdf.vocabulary.RDFS;

/**
 * @author personickm
 */
public class OwlAxioms extends RdfsAxioms {
    
    static public final Axioms INSTANCE = new OwlAxioms();
    
    protected OwlAxioms()
    {
        
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
