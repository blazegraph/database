package com.bigdata.rdf.sparql.ast;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.fed.QueryEngineFactory;
import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.service.IBigdataFederation;

/**
 * Convenience class for passing around the various pieces of context necessary
 * to construct the bop pipeline.
 */
public class AST2BOpContext {

	public final QueryRoot query;
	
	public final AtomicInteger idFactory;
	
	public final AbstractTripleStore db;
	
	public final QueryEngine queryEngine;
	
    public final Properties queryHints;

    /**
     * 
     * @param query
     *            The top-level of the bigdata query model.
     * @param idFactory
     *            A factory for assigning identifiers to {@link BOp}s.
     * @param db
     *            The database.
     * @param queryEngine
     *            The query engine.
     * @param queryHints
     *            The query hints.
     * 
     *            TODO We should be passing in the {@link IIndexManager} rather
     *            than the {@link AbstractTripleStore} in order to support cross
     *            kb queries. The AST can be annotated with the namespace of the
     *            default KB instance, which can then be resolved from the
     *            {@link IIndexManager}.
     * 
     *            TODO The queryHints are available from the AST (as generated
     *            from the parseTree) and should be dropped from this class.
     * 
     *            TODO The {@link QueryEngine} can be discovered from the
     *            {@link IIndexManager} using {@link QueryEngineFactory} and
     *            does not really need to be passed in here.
     */
	public AST2BOpContext(final QueryRoot query,
			final AtomicInteger idFactory, final AbstractTripleStore db,
    		final QueryEngine queryEngine, final Properties queryHints) {
		
		this.query = query;
		this.idFactory = idFactory;
		this.db = db;
		this.queryEngine = queryEngine;
		this.queryHints = queryHints;
		
	}
	
	public int nextId() {
		
		return idFactory.incrementAndGet();
		
	}

	/**
	 * Return <code>true</code> if we are running on a cluster.
	 */
    public boolean isCluster() {

        return db.getIndexManager() instanceof IBigdataFederation<?>;

    }

    /**
     * Return the namespace of the lexicon relation.
     */
    public String getLexiconNamespace() {
        
        return db.getLexiconRelation().getNamespace();
        
    }
    
}
