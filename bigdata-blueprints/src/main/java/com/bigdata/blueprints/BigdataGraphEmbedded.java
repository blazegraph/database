/**
Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
package com.bigdata.blueprints;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;
import org.openrdf.model.BNode;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.Sail;

import com.bigdata.blueprints.BigdataGraphEdit.Action;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.fed.QueryEngineFactory;
import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.changesets.ChangeAction;
import com.bigdata.rdf.changesets.ChangeRecord;
import com.bigdata.rdf.changesets.IChangeLog;
import com.bigdata.rdf.changesets.IChangeRecord;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.sail.QueryCancellationHelper;
import com.bigdata.rdf.sail.model.RunningQuery;
import com.bigdata.rdf.sail.webapp.BigdataRDFContext.AbstractQueryTask;
import com.bigdata.rdf.sail.webapp.StatusServlet;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BigdataStatementIterator;
import com.bigdata.striterator.ChunkedArrayIterator;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.TransactionalGraph;

/**
 * This is the most basic possible implementation of the Blueprints Graph API.
 * It wraps an embedded {@link BigdataSailRepository} and holds open an
 * unisolated connection to the database for the lifespan of the Graph (until
 * {@link #shutdown()} is called.
 * 
 * @author mikepersonick
 *
 */
public class BigdataGraphEmbedded extends BigdataGraph implements TransactionalGraph, IChangeLog {
	
    private final transient static Logger log = Logger.getLogger(BigdataGraphEmbedded.class);
    
	final BigdataSailRepository repo;
	
//	transient BigdataSailRepositoryConnection cxn;

	final List<BigdataGraphListener> listeners = 
	        Collections.synchronizedList(new LinkedList<BigdataGraphListener>());

	/**
	 * Create a Blueprints wrapper around a {@link BigdataSail} instance.
	 */
    public BigdataGraphEmbedded(final BigdataSail sail) {
        this(sail, BigdataRDFFactory.INSTANCE);
    }
    
    /**
     * Create a Blueprints wrapper around a {@link BigdataSail} instance with
     * a non-standard {@link BlueprintsValueFactory} implementation.
     */
    public BigdataGraphEmbedded(final BigdataSail sail, 
            final BlueprintsValueFactory factory) {
        this(new BigdataSailRepository(sail), factory, new Properties());
    }
    
    /**
     * Create a Blueprints wrapper around a {@link BigdataSail} instance with
     * a non-standard {@link BlueprintsValueFactory} implementation.
     */
    public BigdataGraphEmbedded(final BigdataSail sail, 
            final BlueprintsValueFactory factory, final Properties props) {
        this(new BigdataSailRepository(sail), factory, props);
    }
    
    /**
     * Create a Blueprints wrapper around a {@link BigdataSailRepository} 
     * instance.
     */
	public BigdataGraphEmbedded(final BigdataSailRepository repo) {
		this(repo, BigdataRDFFactory.INSTANCE, new Properties());
	}
	
    /**
     * Create a Blueprints wrapper around a {@link BigdataSailRepository} 
     * instance with a non-standard {@link BlueprintsValueFactory} implementation.
     */
	public BigdataGraphEmbedded(final BigdataSailRepository repo, 
			final BlueprintsValueFactory factory, final Properties props) {
	    super(factory, props);
	    
	    this.repo = (BigdataSailRepository) repo;
	}
	
	public BigdataSailRepository getRepository() {
	    return repo;
	}
	
    protected final BigdataThreadLocal cxn = new BigdataThreadLocal();
    
    protected class BigdataThreadLocal extends ThreadLocal<BigdataSailRepositoryConnection> {

        protected BigdataSailRepositoryConnection initialValue() {
            try {
                return _initialValue();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
        
        private boolean create = true;
        
        private BigdataSailRepositoryConnection _initialValue() throws Exception {
            if (!create) {
                return null;
            }
            
            final BigdataSailRepositoryConnection cxn = (BigdataSailRepositoryConnection) repo.getUnisolatedConnection();
            try {
                cxn.setAutoCommit(false);
                cxn.addChangeLog(BigdataGraphEmbedded.this);
                return cxn;
            } catch (Exception ex) {
                cxn.close();
                throw ex;
            }
        }
        
        /**
         * Normal semantics - get or create.
         */
        @Override
        public BigdataSailRepositoryConnection get() {
            return get(true);
        }
        
        /**
         * Modified semantics - only create if create is true.
         */
        public BigdataSailRepositoryConnection get(final boolean create) {
            BigdataThreadLocal.this.create = create;
            return super.get();
        }

        /**
         * Test for existence of thread local object without creating.
         */
        public boolean exists() {
            return get(false) != null;
        }

    }

	public BigdataSailRepositoryConnection getWriteConnection() throws Exception {
	    return cxn.get();
	}
	
	public BigdataSailRepositoryConnection getReadConnection() throws Exception {
	    return ((BigdataSailRepository)repo).getReadOnlyConnection();
	}
	
	@Override
	public void commit() {
		try {
            final RepositoryConnection cxn = this.cxn.get(false);
            if (cxn != null) {
                cxn.commit();
                cxn.close();
                this.cxn.remove();
            }
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void rollback() {
		try {
            final RepositoryConnection cxn = this.cxn.get(false);
            if (cxn != null) {
                cxn.rollback();
                cxn.close();
                this.cxn.remove();
            }
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void shutdown() {
		try {
		    // if there is a connection open, commit and close
		    if (cxn.exists()) {
		        commit();
		    }
			repo.shutDown();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	@Deprecated
	public void stopTransaction(Conclusion arg0) {
	}
	
	public StringBuilder dumpStore() {
	    return ((BigdataSailRepository)repo).getDatabase().dumpStore();
	}
	
	
    protected static final Features FEATURES = new Features();

    @Override
    public Features getFeatures() {

        return FEATURES;
        
    }
    
    static {
        
        FEATURES.supportsSerializableObjectProperty = BigdataGraph.FEATURES.supportsSerializableObjectProperty;
        FEATURES.supportsBooleanProperty = BigdataGraph.FEATURES.supportsBooleanProperty;
        FEATURES.supportsDoubleProperty = BigdataGraph.FEATURES.supportsDoubleProperty;
        FEATURES.supportsFloatProperty = BigdataGraph.FEATURES.supportsFloatProperty;
        FEATURES.supportsIntegerProperty = BigdataGraph.FEATURES.supportsIntegerProperty;
        FEATURES.supportsPrimitiveArrayProperty = BigdataGraph.FEATURES.supportsPrimitiveArrayProperty;
        FEATURES.supportsUniformListProperty = BigdataGraph.FEATURES.supportsUniformListProperty;
        FEATURES.supportsMixedListProperty = BigdataGraph.FEATURES.supportsMixedListProperty;
        FEATURES.supportsLongProperty = BigdataGraph.FEATURES.supportsLongProperty;
        FEATURES.supportsMapProperty = BigdataGraph.FEATURES.supportsMapProperty;
        FEATURES.supportsStringProperty = BigdataGraph.FEATURES.supportsStringProperty;
        FEATURES.supportsDuplicateEdges = BigdataGraph.FEATURES.supportsDuplicateEdges;
        FEATURES.supportsSelfLoops = BigdataGraph.FEATURES.supportsSelfLoops;
        FEATURES.isPersistent = BigdataGraph.FEATURES.isPersistent;
        FEATURES.isWrapper = BigdataGraph.FEATURES.isWrapper;
        FEATURES.supportsVertexIteration = BigdataGraph.FEATURES.supportsVertexIteration;
        FEATURES.supportsEdgeIteration = BigdataGraph.FEATURES.supportsEdgeIteration;
        FEATURES.supportsVertexIndex = BigdataGraph.FEATURES.supportsVertexIndex;
        FEATURES.supportsEdgeIndex = BigdataGraph.FEATURES.supportsEdgeIndex;
        FEATURES.ignoresSuppliedIds = BigdataGraph.FEATURES.ignoresSuppliedIds;
//        FEATURES.supportsTransactions = BigdataGraph.FEATURES.supportsTransactions;
        FEATURES.supportsIndices = BigdataGraph.FEATURES.supportsIndices;
        FEATURES.supportsKeyIndices = BigdataGraph.FEATURES.supportsKeyIndices;
        FEATURES.supportsVertexKeyIndex = BigdataGraph.FEATURES.supportsVertexKeyIndex;
        FEATURES.supportsEdgeKeyIndex = BigdataGraph.FEATURES.supportsEdgeKeyIndex;
        FEATURES.supportsEdgeRetrieval = BigdataGraph.FEATURES.supportsEdgeRetrieval;
        FEATURES.supportsVertexProperties = BigdataGraph.FEATURES.supportsVertexProperties;
        FEATURES.supportsEdgeProperties = BigdataGraph.FEATURES.supportsEdgeProperties;
        FEATURES.supportsThreadedTransactions = BigdataGraph.FEATURES.supportsThreadedTransactions;
        
        // override
        FEATURES.supportsTransactions = true; //BigdataGraph.FEATURES.supportsTransactions;
        
    }
    
    public void addListener(final BigdataGraphListener listener) {
        this.listeners.add(listener);
    }

    public void removeListener(final BigdataGraphListener listener) {
        this.listeners.remove(listener);
    }

    /**
     * We need to batch and materialize these.
     */
    private final List<IChangeRecord> removes = new LinkedList<IChangeRecord>();
    
    /**
     * Changed events coming from bigdata.
     */
    @Override
    public void changeEvent(final IChangeRecord record) {
        /*
         * Watch out for history change events.
         */
        if (record.getStatement().getSubject() instanceof BNode) {
            return;
        }
        /*
         * Adds come in already materialized. Removes do not. Batch and
         * materialize at commit or abort notification.
         */
        if (record.getChangeAction() == ChangeAction.REMOVED) {
            synchronized(removes) {
                removes.add(record);
            }
        } else {
            notify(record);
        }
    }
    
    /**
     * Turn a change record into a graph edit and notify the graph listeners.
     * 
     * @param record
     *          Bigdata change record.
     */
    protected void notify(final IChangeRecord record) {
        final BigdataGraphEdit edit = toGraphEdit(record);
        if (edit != null) {
            for (BigdataGraphListener listener : listeners) {
                listener.graphEdited(edit, record.toString());
            }
        }
    }
    
    /**
     * Turn a bigdata change record into a graph edit.
     * 
     * @param record
     *          Bigdata change record
     * @return
     *          graph edit
     */
    protected BigdataGraphEdit toGraphEdit(final IChangeRecord record) {
        
        final Action action;
        if (record.getChangeAction() == ChangeAction.INSERTED) {
            action = Action.Add;
        } else if (record.getChangeAction() == ChangeAction.REMOVED) {
            action = Action.Remove;
        } else {
            /*
             * Truth maintenance.
             */
            return null;
        }
        
        final BigdataGraphAtom atom = super.toGraphAtom(record.getStatement());
        
        return new BigdataGraphEdit(action, atom);
        
    }
    
    /**
     * Materialize a batch of change records.
     * 
     * @param records
     *          Bigdata change records
     * @return
     *          Same records with materialized values
     */
    protected List<IChangeRecord> materialize(final List<IChangeRecord> records) {
        
        try {
            final AbstractTripleStore db = cxn.get().getTripleStore();

            final List<IChangeRecord> materialized = new LinkedList<IChangeRecord>();

            // collect up the ISPOs out of the unresolved change records
            final ISPO[] spos = new ISPO[records.size()];
            int i = 0;
            for (IChangeRecord rec : records) {
                spos[i++] = rec.getStatement();
            }

            // use the database to resolve them into BigdataStatements
            final BigdataStatementIterator it = db
                    .asStatementIterator(new ChunkedArrayIterator<ISPO>(i,
                            spos, null/* keyOrder */));

            /*
             * the BigdataStatementIterator will produce BigdataStatement
             * objects in the same order as the original ISPO array
             */
            for (IChangeRecord rec : records) {
                final BigdataStatement stmt = it.next();
                materialized.add(new ChangeRecord(stmt, rec.getChangeAction()));
            }

            return materialized;
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        
    }

    /**
     * Notification of transaction beginning.
     */
    @Override
    public void transactionBegin() {
        for (BigdataGraphListener listener : listeners) {
            listener.transactionBegin();
        }
    }

    /**
     * Notification of transaction preparing for commit.
     */
    @Override
    public void transactionPrepare() {
        for (BigdataGraphListener listener : listeners) {
            listener.transactionPrepare();
        }
    }

    /**
     * Notification of transaction committed.
     */
    @Override
    public void transactionCommited(final long commitTime) {
        notifyRemoves();
        for (BigdataGraphListener listener : listeners) {
            listener.transactionCommited(commitTime);
        }
    }

    /**
     * Notification of transaction aborted.
     */
    @Override
    public void transactionAborted() {
        notifyRemoves();
        for (BigdataGraphListener listener : listeners) {
            listener.transactionAborted();
        }
    }
    
    @Override
    public void close() {
    }

    /**
     * Materialize and notify listeners of the remove events.
     */
    protected void notifyRemoves() {
        if (listeners.size() > 0) {
            final List<IChangeRecord> removes;
            synchronized(this.removes) {
                removes = materialize(this.removes);
                this.removes.clear();
            }
            for (IChangeRecord remove : removes) {
                notify(remove);
            }
        } else {
            synchronized(this.removes) {
                this.removes.clear();
            }
        }
    }
    
    protected QueryEngine getQueryEngine() {

    	final QueryEngine queryEngine = (QueryEngine) QueryEngineFactory.getInstance()
                .getQueryController(getIndexManager());
    	
    	return queryEngine;
    }

	private IIndexManager getIndexManager() {
	
		final BigdataSailRepository repo = (BigdataSailRepository) this.getRepository();
		
		final IIndexManager indexMgr = repo.getDatabase().getIndexManager();
		
		return indexMgr;
		
	}


//    @Override
//    public synchronized Object getProperty(URI uri, String prop) {
//        return super.getProperty(uri, prop);
//    }
//
//    @Override
//    public synchronized Object getProperty(URI uri, URI prop) {
//        return super.getProperty(uri, prop);
//    }
//
//    @Override
//    public synchronized List<Object> getProperties(URI uri, String prop) {
//        return super.getProperties(uri, prop);
//    }
//
//    @Override
//    public synchronized List<Object> getProperties(URI uri, URI prop) {
//        return super.getProperties(uri, prop);
//    }
//
//    @Override
//    public synchronized Set<String> getPropertyKeys(URI uri) {
//        return super.getPropertyKeys(uri);
//    }
//
//    @Override
//    public synchronized Object removeProperty(URI uri, String prop) {
//        return super.removeProperty(uri, prop);
//    }
//
//    @Override
//    public synchronized Object removeProperty(URI uri, URI prop) {
//        return super.removeProperty(uri, prop);
//    }
//
//    @Override
//    public synchronized void setProperty(URI uri, String prop, Object val) {
//        super.setProperty(uri, prop, val);
//    }
//
//    @Override
//    public synchronized void setProperty(URI uri, URI prop, Literal val) {
//        super.setProperty(uri, prop, val);
//    }
//
//    @Override
//    public synchronized void addProperty(URI uri, String prop, Object val) {
//        super.addProperty(uri, prop, val);
//    }
//
//    @Override
//    public synchronized void addProperty(URI uri, URI prop, Literal val) {
//        super.addProperty(uri, prop, val);
//    }
//
//    @Override
//    public synchronized void loadGraphML(String file) throws Exception {
//        super.loadGraphML(file);
//    }
//
//    @Override
//    public synchronized Edge addEdge(Object key, Vertex from, Vertex to, String label) {
//        return super.addEdge(key, from, to, label);
//    }
//
//    @Override
//    public synchronized Vertex addVertex(Object key) {
//        return super.addVertex(key);
//    }
//
//    @Override
//    public synchronized Edge getEdge(Object key) {
//        return super.getEdge(key);
//    }
//
//    @Override
//    public synchronized Iterable<Edge> getEdges() {
//        return super.getEdges();
//    }
//
//    @Override
//    synchronized Iterable<Edge> getEdges(URI from, URI to, String... labels) {
//        return super.getEdges(from, to, labels);
//    }
//
//    @Override
//    protected synchronized GraphQueryResult getElements(URI from, URI to, String... labels) {
//        return super.getElements(from, to, labels);
//    }
//
//    @Override
//    synchronized Iterable<Edge> getEdges(String queryStr) {
//        return super.getEdges(queryStr);
//    }
//
//    @Override
//    synchronized Iterable<Vertex> getVertices(URI from, URI to, String... labels) {
//        return super.getVertices(from, to, labels);
//    }
//
//    @Override
//    synchronized Iterable<Vertex> getVertices(String queryStr, boolean subject) {
//        return super.getVertices(queryStr, subject);
//    }
//
//    @Override
//    public synchronized Iterable<Edge> getEdges(String prop, Object val) {
//        return super.getEdges(prop, val);
//    }
//
//    @Override
//    public synchronized Vertex getVertex(Object key) {
//        return super.getVertex(key);
//    }
//
//    @Override
//    public synchronized Iterable<Vertex> getVertices() {
//        return super.getVertices();
//    }
//
//    @Override
//    public synchronized Iterable<Vertex> getVertices(String prop, Object val) {
//        return super.getVertices(prop, val);
//    }
//
//    @Override
//    public synchronized GraphQuery query() {
//        return super.query();
//    }
//
//    @Override
//    public synchronized void removeEdge(Edge edge) {
//        super.removeEdge(edge);
//    }
//
//    @Override
//    public synchronized void removeVertex(Vertex vertex) {
//        super.removeVertex(vertex);
//    }
//
//    @Override
//    public Features getFeatures() {
//        return super.getFeatures();
//    }
	
	/**
     * 
	 * <p>
	 * Note: This is also responsible for noticing the time at which the
	 * query begins to execute and storing the {@link RunningQuery} in the
	 * {@link #m_queries} map.
	 * 
     * @param The connection.
     */
	protected UUID setupQuery(final BigdataSailRepositoryConnection cxn,
			ASTContainer astContainer, final QueryType queryType,
			final String extId) {

        // Note the begin time for the query.
        final long begin = System.nanoTime();

        // Figure out the UUID under which the query will execute.
		final UUID queryUuid = setQueryId(astContainer, UUID.randomUUID());
		
		//Set to UUID of internal ID if it is null.
		final String extQueryId = extId == null?queryUuid.toString():extId;
		
		if (log.isDebugEnabled() && extId == null) {
			log.debug("Received null external query ID.  Using "
					+ queryUuid.toString());
		}
		
		final boolean isUpdateQuery = queryType != QueryType.ASK
				&& queryType != QueryType.CONSTRUCT
				&& queryType != QueryType.DESCRIBE
				&& queryType != QueryType.SELECT;
        
        final RunningQuery r = new RunningQuery(extQueryId, queryUuid, begin, isUpdateQuery);

        // Stuff it in the maps of running queries.
        m_queries.put(extQueryId, r);
        m_queries2.put(queryUuid, r);
        
		if (log.isDebugEnabled()) {
			log.debug("Setup Query (External ID, UUID):  ( " + extQueryId
					+ " , " + queryUuid + " )");
			log.debug("External query for " + queryUuid + " is :\n"
					+ getQueryById(queryUuid).getExtQueryId());
			log.debug(runningQueriesToString());
		}

        return queryUuid;
        
    }
     
	/**
	 * Determines the {@link UUID} which will be associated with the
	 * {@link IRunningQuery}. If {@link QueryHints#QUERYID} has already been
	 * used by the application to specify the {@link UUID} then that
	 * {@link UUID} is noted. Otherwise, a random {@link UUID} is generated and
	 * assigned to the query by binding it on the query hints.
	 * <p>
	 * Note: The ability to provide metadata from the {@link ASTContainer} in
	 * the {@link StatusServlet} or the "EXPLAIN" page depends on the ability to
	 * cross walk the queryIds as established by this method.
	 * 
	 * @param query
	 *            The query.
	 * 
	 * @param queryUuid
	 * 
	 * @return The {@link UUID} which will be associated with the
	 *         {@link IRunningQuery} and never <code>null</code>.
	 */
	protected UUID setQueryId(final ASTContainer astContainer, UUID queryUuid) {

		// Figure out the effective UUID under which the query will run.
		final String queryIdStr = astContainer.getQueryHint(QueryHints.QUERYID);
		if (queryIdStr == null) {
			// Not specified, so generate and set on query hint.
			queryUuid = UUID.randomUUID();
		} 

		astContainer.setQueryHint(QueryHints.QUERYID, queryUuid.toString());

		return queryUuid;
	}

	/**
	 * The currently executing queries (does not include queries where a client
	 * has established a connection but the query is not running because the
	 * {@link #queryService} is blocking).
	 * <p>
	 * Note: This includes both SPARQL QUERY and SPARQL UPDATE requests.
	 * However, the {@link AbstractQueryTask#queryUuid} might not yet be bound
	 * since it is not set until the request begins to execute. See
	 * {@link AbstractQueryTask#setQueryId(ASTContainer)}.
	 */
	private static final ConcurrentHashMap<String/* extQueryId */, RunningQuery> m_queries = new ConcurrentHashMap<String, RunningQuery>();

	/**
	 * The currently executing QUERY and UPDATE requests.
	 * <p>
	 * Note: This does not include requests where a client has established a
	 * connection to the SPARQL end point but the request is not executing
	 * because the {@link #queryService} is blocking).
	 * <p>
	 * Note: This collection was introduced because the SPARQL UPDATE requests
	 * are not executed on the {@link QueryEngine} and hence we can not use
	 * {@link QueryEngine#getRunningQuery(UUID)} to resolve the {@link Future}
	 */
	private static final ConcurrentHashMap<UUID/* queryUuid */, RunningQuery> m_queries2 = new ConcurrentHashMap<UUID, RunningQuery>();

	
	public RunningQuery getQueryById(final UUID queryUuid) {

		return m_queries2.get(queryUuid);

	}

	@Override
	public RunningQuery getQueryByExternalId(String extQueryId) {
		
		return m_queries.get(extQueryId);
	}

	/**
	 * 
	 * Remove the query from the internal queues.
	 * 
	 */
	@Override
	protected void tearDownQuery(UUID queryId) {
		
		if (queryId != null) {
			
			if(log.isDebugEnabled()) {
				log.debug("Tearing down query: " + queryId );
				log.debug("m_queries2 has " + m_queries2.size());
			}

			final RunningQuery r = m_queries2.get(queryId);

			if (r != null) {
				m_queries.remove(r.getExtQueryId(), r);
				m_queries2.remove(queryId);

				if (log.isDebugEnabled()) {
					log.debug("Tearing down query: " + queryId);
					log.debug("m_queries2 has " + m_queries2.size());
				}
			}

		}

	}
	
	/**
	 * Helper method to determine if a query was cancelled.
	 * 
	 * @param queryId
	 * @return
	 */
	protected boolean isQueryCancelled(final UUID queryId) {

		if (log.isDebugEnabled()) {
			log.debug(queryId);
		}
		
		RunningQuery q = getQueryById(queryId);

		if (log.isDebugEnabled() && q != null) {
			log.debug(queryId + " isCancelled: " + q.isCancelled());
		}

		if (q != null) {
			return q.isCancelled();
		}

		return false;
	}
	
	public String runningQueriesToString()
	{
		final Collection<RunningQuery> queries = m_queries2.values();
		
		final Iterator<RunningQuery> iter = queries.iterator();
		
		final StringBuffer sb = new StringBuffer();
	
		while(iter.hasNext()){
			final RunningQuery r = iter.next();
			sb.append(r.getQueryUuid() + " : \n" + r.getExtQueryId());
		}
		
		return sb.toString();
		
	}
	
	public Collection<RunningQuery> getRunningQueries() {
		final Collection<RunningQuery> queries = m_queries2.values();

		return queries;
	}

	@Override
	public void cancel(final UUID queryId) {

		assert(queryId != null);
		QueryCancellationHelper.cancelQuery(queryId, this.getQueryEngine());

		RunningQuery q = getQueryById(queryId);

		if(q != null) {
			//Set the status to cancelled in the internal queue.
			q.setCancelled(true);
		}
	}

	@Override
	public void cancel(final String uuid) {
		cancel(UUID.fromString(uuid));
	}

	@Override
	public void cancel(final RunningQuery rQuery) {

		if(rQuery != null) {
			final UUID queryId = rQuery.getQueryUuid();
			cancel(queryId);
		}

	}


}
