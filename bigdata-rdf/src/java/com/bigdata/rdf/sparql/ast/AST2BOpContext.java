package com.bigdata.rdf.sparql.ast;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IdFactory;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.fed.QueryEngineFactory;
import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.sail.QueryHints;
import com.bigdata.rdf.sparql.ast.optimizers.DefaultOptimizerList;
import com.bigdata.rdf.sparql.ast.optimizers.OptimizerList;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.service.IBigdataFederation;

/**
 * Convenience class for passing around the various pieces of context necessary
 * to construct the bop pipeline.
 */
public class AST2BOpContext implements IdFactory {

    /**
     * The original {@link QueryRoot} to be evaluated.
     */
	public final QueryRoot query;
	
	/**
	 * Factory for assigning unique within query identifiers to {@link BOp}s.
	 * 
	 * @see #nextId()
	 */
	final AtomicInteger idFactory;
	
	/**
	 * The KB instance.
	 */
	public final AbstractTripleStore db;
	
	/**
	 * The {@link QueryEngine}. 
	 */
	public final QueryEngine queryEngine;
	
	/**
	 * The query hints from the original {@link #query}.
	 */
    final Properties queryHints;
    
    /**
     * The unique identifier assigned to this query.
     * 
     * @see QueryHints#QUERYID
     * @see QueryEngine.Annotations#QUERY_ID
     */
    public final UUID queryId;

    /**
     * The query optimizers (this includes both query rewrites for correct
     * semantics, such as a rewrite of a DESCRIBE query into a CONSTRUCT query,
     * and query rewrites for performance optimizations).
     */
    public final OptimizerList optimizers;

    /**
     * The query as rewritten by the query optimizers. 
     */
    public QueryRoot optimizedQuery;
    
    /**
     * When <code>true</code>, will use the version of DISTINCT which operators
     * on the native heap (assuming that the query is not of such obvious low
     * cardinality that the JVM heap version will do better).
     */
    boolean nativeDistinct = true;

    private int varIdFactory = 0;

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
     * @deprecated by the other constructor.
     */
	public AST2BOpContext(final QueryRoot query,
			final AtomicInteger idFactory, final AbstractTripleStore db,
    		final QueryEngine queryEngine, final Properties queryHints) {

        this.query = query;
        this.idFactory = idFactory;
        this.db = db;
        this.optimizers = new DefaultOptimizerList();
        this.queryEngine = queryEngine;
        this.queryHints = queryHints;

        // Use either the caller's UUID or a random UUID.
        final String queryIdStr = queryHints == null ? null : queryHints
                .getProperty(QueryHints.QUERYID);

        final UUID queryId = queryIdStr == null ? UUID.randomUUID() : UUID
                .fromString(queryIdStr);

        this.queryId = queryId;
		
	}

    /**
     * 
     * @param queryRoot
     *            The root of the query.
     * @param db
     *            The KB instance.
     * 
     *            TODO We should be passing in the {@link IIndexManager} rather
     *            than the {@link AbstractTripleStore} in order to support cross
     *            kb queries. The AST can be annotated with the namespace of the
     *            default KB instance, which can then be resolved from the
     *            {@link IIndexManager}. This would also allow us to clear up
     *            the [lex] namespace parameter to the {@link FunctionNode} and
     *            {@link FunctionRegistry}.
     */
    public AST2BOpContext(final QueryRoot queryRoot,
                final AbstractTripleStore db) {

        if (queryRoot == null)
            throw new IllegalArgumentException();

        if (db == null)
            throw new IllegalArgumentException();

        this.query = queryRoot;

        this.db = db;

        this.optimizers = new DefaultOptimizerList();

        /*
         * Note: The ids are assigned using incrementAndGet() so ONE (1) is the
         * first id that will be assigned when we pass in ZERO (0) as the
         * initial state of the AtomicInteger.
         */
        this.idFactory = new AtomicInteger(0);
        
        this.queryEngine = QueryEngineFactory.getQueryController(db
                .getIndexManager());

        this.queryHints = queryRoot.getQueryHints();

        /*
         * Figure out the query UUID that will be used. This will be bound onto
         * the query plan when it is generated. We figure out what it will be up
         * front so we can refer to its UUID in parts of the query plan. For
         * example, this is used to coordinate access to bits of state on a
         * parent IRunningQuery.
         */
        
        // Use either the caller's UUID or a random UUID.
        final String queryIdStr = queryHints == null ? null : queryHints
                .getProperty(QueryHints.QUERYID);

        final UUID queryId = queryIdStr == null ? UUID.randomUUID() : UUID
                .fromString(queryIdStr);

        this.queryId = queryId;
        
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

    /**
     * Create a new variable name which is unique within the scope of this
     * {@link AST2BOpContext}.
     * 
     * @param prefix
     *            The prefix.  The general pattern for a prefix is "-foo-".
     *            
     * @return The unique name.
     */
    public final String createVar(final String prefix) {

        return prefix + (varIdFactory++);

    }

}
