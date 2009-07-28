package com.bigdata.rdf.rules;

import com.bigdata.rdf.axioms.Axioms;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.vocab.Vocabulary;

/**
 * Base class for classes that provide closure programs.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class BaseClosure {

    /**
     * The database whose configuration will determine which entailments are
     * to be maintained and which of those entailments are computed by
     * forward closure vs backchained.
     */
    protected final AbstractTripleStore db;
    
    final protected boolean rdfsOnly;
    final protected boolean forwardChainRdfTypeRdfsResource;
    final protected boolean forwardChainOwlSameAsClosure;
    final protected boolean forwardChainOwlSameAsProperties;
    final protected boolean forwardChainOwlEquivalentProperty;
    final protected boolean forwardChainOwlEquivalentClass;
    final protected boolean forwardChainOwlTransitiveProperty;
    final protected boolean forwardChainOwlInverseOf;
    final protected boolean forwardChainOwlHasValue;

    /**
     * The {@link Axioms} declared for the database.
     */
    final Axioms axioms;
    
    /**
     * Various term identifiers that we need to construct the rules.
     */
    final protected Vocabulary vocab;

    /**
     * 
     * @param db
     *            The database whose configuration will determine which
     *            entailments are to be maintained and which of those
     *            entailments are computed by forward closure vs
     *            backchained.
     * 
     * @throws IllegalArgumentException
     *             if the <i>db</i> is <code>null</code>.
     */
    protected BaseClosure(AbstractTripleStore db) {

        if (db == null)
            throw new IllegalArgumentException();
        
        this.db = db;
     
        final InferenceEngine inf = db.getInferenceEngine();
        
        axioms = db.getAxioms();
        
        vocab = db.getVocabulary();
        
        rdfsOnly = axioms.isRdfSchema() && !axioms.isOwlSameAs();
        
        forwardChainRdfTypeRdfsResource = inf.forwardChainRdfTypeRdfsResource;
        
        forwardChainOwlSameAsClosure = inf.forwardChainOwlSameAsClosure;
        
        forwardChainOwlSameAsProperties = inf.forwardChainOwlSameAsProperties;
        
        forwardChainOwlEquivalentProperty = inf.forwardChainOwlEquivalentProperty;

        forwardChainOwlEquivalentClass = inf.forwardChainOwlEquivalentClass;
        
        forwardChainOwlTransitiveProperty = inf.forwardChainOwlTransitiveProperty;
        
        forwardChainOwlInverseOf = inf.forwardChainOwlInverseOf;
        
        forwardChainOwlHasValue = inf.forwardChainOwlHasValue;
        
    }

    /**
     * Return the program that will be used to compute the closure of the
     * database.
     * 
     * @param database
     *            The database whose closure will be updated.
     * @param focusStore
     *            When non-<code>null</code>, the focusStore will be
     *            closed against the database with the entailments written
     *            into the database. When <code>null</code>, the entire
     *            database will be closed (database-at-once closure).
     *            
     * @return The program to be executed.
     * 
     * @todo the returned program can be cached for a given database and
     *       focusStore (or for the database if no focusStore is used).
     */
    abstract public MappedProgram getProgram(String database, String focusStore);
    
}