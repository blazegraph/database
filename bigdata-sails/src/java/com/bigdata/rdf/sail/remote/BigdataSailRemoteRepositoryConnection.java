/**

Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

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

package com.bigdata.rdf.sail.remote;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.Iteration;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;
import org.openrdf.model.Graph;
import org.openrdf.model.Namespace;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.GraphImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.BooleanQuery;
import org.openrdf.query.Dataset;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.Query;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.TupleQueryResultHandler;
import org.openrdf.query.Update;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;
import org.openrdf.repository.UnknownTransactionStateException;
import org.openrdf.rio.ParserConfig;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;

import com.bigdata.rdf.sail.webapp.client.IPreparedBooleanQuery;
import com.bigdata.rdf.sail.webapp.client.IPreparedGraphQuery;
import com.bigdata.rdf.sail.webapp.client.IPreparedQueryListener;
import com.bigdata.rdf.sail.webapp.client.IPreparedSparqlUpdate;
import com.bigdata.rdf.sail.webapp.client.IPreparedTupleQuery;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository.AddOp;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository.RemoveOp;

/**
 * An implementation of Sesame's RepositoryConnection interface that wraps a
 * bigdata RemoteRepository. This provides SAIL API based client access to a
 * bigdata remote NanoSparqlServer. This implementation operates only in
 * auto-commit mode (each mutation operation results in a commit on the server).
 * It also throws UnsupportedOperationExceptions all over the place due to
 * incompatibilities with our own remoting interface. If there is something
 * important that you need implemented for your application don't be afraid to
 * reach out and contact us.
 * 
 * TODO Implement buffering of adds and removes so that we can turn off
 * 		auto-commit. 
 * TODO Fix all the Query objects (TupleQuery, GraphQuery,
 * 		BooleanQuery) to support the various possible operations on them, such as
 * 		setting a binding. 
 * TODO Support baseURIs
 */
public class BigdataSailRemoteRepositoryConnection 
        implements RepositoryConnection, IPreparedQueryListener {

    private static final transient Logger log = Logger
            .getLogger(BigdataSailRemoteRepositoryConnection.class);

    private final BigdataSailRemoteRepository repo;
    
    private boolean openConn;

    public BigdataSailRemoteRepositoryConnection(
            final BigdataSailRemoteRepository repo) {

        this.repo = repo;
        
        this.openConn = true;

    }
    
    /**
     * A concurrency-managed list of running query ids.
     */
    private final Map<UUID, UUID> queryIds = new ConcurrentHashMap<UUID, UUID>();
    
    /**
     * Manage access to the query ids.
     */
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	
    /**
     * Cancel the specified query.
     * 
     * @param queryId
     *          The query id.
     * @throws Exception
     */
    public void cancel(final UUID queryId) throws Exception {
        
        lock.readLock().lock();
        
        try {
            
            repo.getRemoteRepository().cancel(queryId);
            queryIds.remove(queryId);
            
            if (log.isDebugEnabled()) {
                log.debug("Query cancelled: " + queryId);
                log.debug("Queries running: " + Arrays.toString(queryIds.keySet().toArray()));
            }
            
        } finally {
            lock.readLock().unlock();
        }
        
    }
    
    /**
     * Cancel all queries started by this connection that have not completed
     * yet at the time of this request.
     * 
     * @param queryId
     *          The query id.
     * @throws Exception
     */
    public void cancelAll() throws RepositoryException {
        
        lock.writeLock().lock();
    
        try {
            
            final RemoteRepository repo = this.repo.getRemoteRepository();
            
            for (UUID queryId : queryIds.keySet()) {
                repo.cancel(queryId);
            }
            queryIds.clear();

            if (log.isDebugEnabled()) {
                log.debug("All queries cancelled.");
                log.debug("Queries running: " + Arrays.toString(queryIds.keySet().toArray()));
            }
         
        } catch (RepositoryException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RepositoryException(ex);
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * Return a list of all queries initiated by this connection that have
     * not completed.
     * @return
     */
    public Set<UUID> getQueryIds() {
        
        lock.readLock().lock();
        
        try {
            return Collections.unmodifiableSet(queryIds.keySet());
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * Callback from the query evaluation object that the query result has been
     * closed (the query either completed or was already cancelled).
     * 
     * @param queryId
     *          The query id.
     */
    public void closed(final UUID queryId) {
        
        lock.readLock().lock();
        
        try {
            
            queryIds.remove(queryId);
        
            if (log.isDebugEnabled()) {
                log.debug("Query completed normally: " + queryId);
                log.debug("Queries running: " + Arrays.toString(queryIds.keySet().toArray()));
            }
        
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * Add a newly launched query id.
     * 
     * @param queryId
     *          The query id.
     */
    public void addQueryId(final UUID queryId) {
        
        lock.readLock().lock();
        
        try {
            
            queryIds.put(queryId, queryId);

            if (log.isDebugEnabled()) {
                log.debug("Query started: " + queryId); Thread.dumpStack();
                log.debug("Queries running: " + Arrays.toString(queryIds.keySet().toArray()));
            }
            
        } finally {
            lock.readLock().unlock();
        }
    }
    
	public long count(final Resource s, final URI p, final Value o, 
			final Resource... c) 
			throws RepositoryException {

        assertOpenConn();
        
		try {
		
			final RemoteRepository remote = repo.getRemoteRepository();
	
			return remote.rangeCount(s, p, o, c);
			
		} catch (Exception ex) {
			
			throw new RepositoryException(ex);
			
		}
		
	}
	
	@Override
    public RepositoryResult<Statement> getStatements(final Resource s,
            final URI p, final Value o, final boolean includeInferred,
            final Resource... c) throws RepositoryException {
		
        assertOpenConn();
        
		try {
			
			final RemoteRepository remote = repo.getRemoteRepository();
			
			final IPreparedGraphQuery query = 
					remote.getStatements2(s, p, o, includeInferred, c);

			/*
			 * Add to the list of running queries.  Will be later removed
			 * via the IPreparedQueryListener callback.
			 */
			addQueryId(query.getQueryId());
			
            final GraphQueryResult src = query.evaluate(this);
            
			/*
			 * Well this was certainly annoying.  is there a better way?
			 */
			return new RepositoryResult<Statement>(new CloseableIteration<Statement, RepositoryException>() {

				@Override
				public boolean hasNext() throws RepositoryException {
					try {
						return src.hasNext();
					} catch (Exception ex) {
						throw new RepositoryException(ex);
					}
				}

				@Override
				public Statement next() throws RepositoryException {
					try {
						return src.next();
					} catch (Exception ex) {
						throw new RepositoryException(ex);
					}
				}

				@Override
				public void remove() throws RepositoryException {
					try {
						src.remove();
					} catch (Exception ex) {
						throw new RepositoryException(ex);
					}
				}

				@Override
				public void close() throws RepositoryException {
					try {
						src.close();
					} catch (Exception ex) {
						throw new RepositoryException(ex);
					}
				}
				
			});
			
		} catch (Exception ex) {
			
			throw new RepositoryException(ex);
			
		}

	}

	@Override
    public boolean hasStatement(final Resource s, final URI p, final Value o,
            final boolean includeInferred, final Resource... c)
            throws RepositoryException {

        assertOpenConn();
        
		try {
			
			final RemoteRepository remote = repo.getRemoteRepository();
			
			return remote.hasStatement(s, p, o, includeInferred, c);
			
		} catch (Exception ex) {
			
			throw new RepositoryException(ex);
			
		}
		
	}

	@Override
    public RemoteBooleanQuery prepareBooleanQuery(final QueryLanguage ql,
            final String query) throws RepositoryException,
            MalformedQueryException {
		
        assertOpenConn();
        
		if (ql != QueryLanguage.SPARQL) {
			
			throw new UnsupportedOperationException("unsupported query language: " + ql);
			
		}
		
		try {
			
			final RemoteRepository remote = repo.getRemoteRepository();
			
			final IPreparedBooleanQuery q = remote.prepareBooleanQuery(query);
			
            /*
             * Add to the list of running queries.  Will be later removed
             * via the IPreparedQueryListener callback.
             */
            addQueryId(q.getQueryId());
            
			return new RemoteBooleanQuery(q);
			
		} catch (Exception ex) {
			
			throw new RepositoryException(ex);
			
		}
		
	}

	@Override
    public BooleanQuery prepareBooleanQuery(final QueryLanguage ql,
            final String query, final String baseURI)
            throws RepositoryException, MalformedQueryException {

        if (baseURI != null)
            throw new UnsupportedOperationException("baseURI not supported");
		
        return prepareBooleanQuery(ql, query);
        
	}

	@Override
    public GraphQuery prepareGraphQuery(final QueryLanguage ql,
            final String query) throws RepositoryException,
            MalformedQueryException {

        assertOpenConn();
        
		if (ql != QueryLanguage.SPARQL) {
			
			throw new UnsupportedOperationException("unsupported query language: " + ql);
			
		}
		
		try {
			
			final RemoteRepository remote = repo.getRemoteRepository();
			
			final IPreparedGraphQuery q = remote.prepareGraphQuery(query);

            /*
             * Add to the list of running queries.  Will be later removed
             * via the IPreparedQueryListener callback.
             */
            addQueryId(q.getQueryId());
            
			return new RemoteGraphQuery(q);
		
		} catch (Exception ex) {
			
			throw new RepositoryException(ex);
			
		}

	}

	@Override
    public GraphQuery prepareGraphQuery(final QueryLanguage ql,
            final String query, final String baseURI)
            throws RepositoryException, MalformedQueryException {

        if (baseURI != null)
            throw new UnsupportedOperationException("baseURI not supported.");

        return prepareGraphQuery(ql, query);    
		
	}

	@Override
    public Query prepareQuery(final QueryLanguage ql, final String query)
            throws RepositoryException, MalformedQueryException {

		throw new UnsupportedOperationException("please use the specific operation for your query type: prepare[Boolean/Tuple/Graph]Query");
		
	}

	@Override
    public Query prepareQuery(final QueryLanguage ql, final String query,
            final String baseURI) throws RepositoryException,
            MalformedQueryException {

        if (baseURI != null)
            throw new UnsupportedOperationException("baseURI not supported");

        return prepareQuery(ql, query);

    }

    @Override
    public TupleQuery prepareTupleQuery(final QueryLanguage ql,
            final String query) throws RepositoryException,
            MalformedQueryException {

        assertOpenConn();
        
		if (ql != QueryLanguage.SPARQL) {
			
			throw new UnsupportedOperationException("unsupported query language: " + ql);
			
		}

		try {
			
			final RemoteRepository remote = repo.getRemoteRepository();
			
			final IPreparedTupleQuery q = remote.prepareTupleQuery(query);

            /*
             * Add to the list of running queries.  Will be later removed
             * via the IPreparedQueryListener callback.
             */
            addQueryId(q.getQueryId());
            
			return new RemoteTupleQuery(q);
		
		} catch (Exception ex) {
			
			throw new RepositoryException(ex);
			
		}
		
	}

	@Override
    public TupleQuery prepareTupleQuery(final QueryLanguage ql,
            final String query, final String baseURI)
            throws RepositoryException, MalformedQueryException {

        if (baseURI != null)
            throw new UnsupportedOperationException("baseURI not supported.");

        return prepareTupleQuery(ql, query);	
	}

	@Override
    public boolean hasStatement(final Statement s,
            final boolean includeInferred, final Resource... c)
            throws RepositoryException {
		
		return hasStatement(s.getSubject(), s.getPredicate(), s.getObject(), includeInferred, c);
		
	}

	@Override
    public <E extends Exception> void add(
            final Iteration<? extends Statement, E> stmts, final Resource... c)
            throws RepositoryException, E {
		
		final Graph g = new GraphImpl();
		while (stmts.hasNext()) {
			g.add(stmts.next());
		}
		
		add(g, c);
		
	}

	@Override
    public void add(final Resource s, final URI p, final Value o,
            final Resource... c) throws RepositoryException {
		
		add(new StatementImpl(s, p, o), c);
		
	}

    /**
     * <strong>single statement updates not recommended</strong>
     * <p>
     * {@inheritDoc}
     */
	@Override
	public void add(final Statement stmt, final Resource... c)
			throws RepositoryException {

//		log.warn("single statement updates not recommended");
		
		final Graph g = new GraphImpl();
		g.add(stmt);
		
		add(g, c);

	}

    @Override
    public void add(final Iterable<? extends Statement> stmts,
            final Resource... c) throws RepositoryException {

		final AddOp op = new AddOp(stmts);
		
		add(op, c);
			
	}

	/**
	 * TODO support baseURI
	 */
	@Override
    public void add(final Reader input, final String baseURI,
            final RDFFormat format, final Resource... c) throws IOException,
            RDFParseException, RepositoryException {
		
		final AddOp op = new AddOp(input, format);
		
		add(op, c);

	}

	/**
	 * TODO support baseURI
	 */
	@Override
    public void add(final URL input, final String baseURI,
            final RDFFormat format, final Resource... c) throws IOException,
            RDFParseException, RepositoryException {
		
		final AddOp op = new AddOp(input.toString());
		
		add(op, c);

	}

	/**
	 * TODO support baseURI
	 */
	@Override
    public void add(final File input, final String baseURI,
            final RDFFormat format, final Resource... c) throws IOException,
            RDFParseException, RepositoryException {
		
		final AddOp op = new AddOp(input, format);
		
		add(op, c);

	}

	/**
	 * TODO support baseURI
	 */
    @Override
    public void add(final InputStream input, final String baseURI,
            final RDFFormat format, final Resource... c) throws IOException,
            RDFParseException, RepositoryException {
		
		final AddOp op = new AddOp(input, format);
		
		add(op, c);

	}

	private void add(final AddOp op, final Resource... c)
			throws RepositoryException {

        assertOpenConn();
        
		try {
			
			op.setContext(c);
			
			final RemoteRepository remote = repo.getRemoteRepository();
			
			remote.add(op);
			
		} catch (Exception ex) {
			
			throw new RepositoryException(ex);
			
		}

	}

	@Override
    public <E extends Exception> void remove(
            final Iteration<? extends Statement, E> stmts, final Resource... c)
            throws RepositoryException, E {

		final Graph g = new GraphImpl();
		while (stmts.hasNext())
			g.add(stmts.next());
		
		remove(g, c);

	}

    /**
     * <strong>single statement updates not recommended</strong>
     * <p>
     * {@inheritDoc}
     */
    @Override
	public void remove(final Statement stmt, final Resource... c)
			throws RepositoryException {
		
		log.warn("single statement updates not recommended");
		
		final Graph g = new GraphImpl();
		g.add(stmt);
		
		remove(g, c);

	}

	@Override
    public void remove(final Iterable<? extends Statement> stmts,
            final Resource... c) throws RepositoryException {

		final RemoveOp op = new RemoveOp(stmts);
		
		remove(op, c);
		
	}

	@Override
    public void remove(final Resource s, URI p, Value o, final Resource... c)
            throws RepositoryException {

		final RemoveOp op = new RemoveOp(s, p, o, c);
		
		remove(op, c);

	}

	private void remove(final RemoveOp op, final Resource... c)
			throws RepositoryException {

        assertOpenConn();
        
		try {
			
			op.setContext(c);
			
			final RemoteRepository remote = repo.getRemoteRepository();
			
			remote.remove(op);
			
		} catch (Exception ex) {
			
			throw new RepositoryException(ex);
			
		}

	}

	@Override
	public void setAutoCommit(final boolean autoCommit) throws RepositoryException {
		
        if (autoCommit == false)
            throw new IllegalArgumentException(
                    "only auto-commit is currently supported");

	}

	@Override
	public void close() throws RepositoryException {
		
        if (!openConn) {
            return;
        }

        this.openConn = false;
        
        cancelAll();
        
	}

	@Override
	public boolean isOpen() throws RepositoryException {
		return openConn;
	}

	@Override
	public void commit() throws RepositoryException {
		// noop
	}

	@Override
	public void rollback() throws RepositoryException {
		// noop
	}

	@Override
	public Repository getRepository() {
		return repo;
	}

    protected void assertOpenConn() throws RepositoryException {
        if(!openConn) {
            throw new RepositoryException("Connection closed");
        }
    }
    
    /**
     * Invoke close, which will be harmless if we are already closed. 
     */
    @Override
    protected void finalize() throws Throwable {
        
        close();
        
        super.finalize();

    }


	@Override
	public RepositoryResult<Resource> getContextIDs()
			throws RepositoryException {

	    assertOpenConn();
	    
		try {
			
			final RemoteRepository remote = repo.getRemoteRepository();

			final Iterator<Resource> contexts = remote.getContexts().iterator();
			
			return new RepositoryResult<Resource>(new CloseableIteration<Resource, RepositoryException>() {

				@Override
				public boolean hasNext() throws RepositoryException {
					return contexts.hasNext();
				}

				@Override
				public Resource next() throws RepositoryException {
					return contexts.next();
				}

				@Override
				public void remove() throws RepositoryException {
					contexts.remove();
				}

				@Override
				public void close() throws RepositoryException {
					// noop					
				}
				
			});
			
		} catch (Exception ex) {
			
			throw new RepositoryException(ex);
			
		}
		
	}

	@Override
	public long size(final Resource... c) throws RepositoryException {
		
        assertOpenConn();
        
		try {
			
			final RemoteRepository remote = repo.getRemoteRepository();
			
			return remote.rangeCount(null, null, null, c);
			
		} catch (Exception ex) {
			
			throw new RepositoryException(ex);
			
		}
		
	}

	@Override
	public void clear(final Resource... c) throws RepositoryException {
		
		remove(null, null, null, c);

	}

	@Override
	public void export(final RDFHandler handler, final Resource... c)
			throws RepositoryException, RDFHandlerException {

		exportStatements(null, null, null, true, handler, c);
		
	}

	@Override
	public void exportStatements(Resource s, URI p, Value o,
			boolean includeInferred, RDFHandler handler, Resource... c)
			throws RepositoryException, RDFHandlerException {

        assertOpenConn();
        
		try {
			
			final RemoteRepository remote = repo.getRemoteRepository();
			
			final IPreparedGraphQuery query = 
					remote.getStatements2(s, p, o, includeInferred, c);
			
            final GraphQueryResult src = query.evaluate(this);
                    
            try {
                
    			handler.startRDF();
    			while (src.hasNext()) {
    				handler.handleStatement(src.next());
    			}
    			handler.endRDF();
    			
            } finally {
                src.close();
            }
			
		} catch (Exception ex) {
			
			throw new RepositoryException(ex);
			
		}
		
	}

	@Override
	public boolean isAutoCommit() throws RepositoryException {
		
		return true;
		
	}

	@Override
	public boolean isEmpty() throws RepositoryException {
		
		return size() > 0;
		
	}

	
	@Override
    public Update prepareUpdate(final QueryLanguage ql, final String query)
            throws RepositoryException, MalformedQueryException {

        assertOpenConn();
        
		if (ql != QueryLanguage.SPARQL) {
			
			throw new UnsupportedOperationException("unsupported query language: " + ql);
			
		}
		
		try {
			
			final RemoteRepository remote = repo.getRemoteRepository();
			
			final IPreparedSparqlUpdate update = remote.prepareUpdate(query);
			
			/*
			 * Only execute() is currently supported.
			 */
			return new RemoteUpdate(update);
			
		} catch (Exception ex) {
			
			throw new RepositoryException(ex);
			
		}
		
	}

    @Override
    public Update prepareUpdate(final QueryLanguage ql, final String query,
            final String baseURI) throws RepositoryException,
            MalformedQueryException {

	    if (baseURI != null)
            throw new UnsupportedOperationException("baseURI not supported");
        
        return prepareUpdate(ql, query);

	}
	
	@Override
	public void setNamespace(String arg0, String arg1)
			throws RepositoryException {
		throw new UnsupportedOperationException();
	}

	@Override
	public String getNamespace(String arg0) throws RepositoryException {
		throw new UnsupportedOperationException();
	}

	@Override
	public RepositoryResult<Namespace> getNamespaces()
			throws RepositoryException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void removeNamespace(String arg0) throws RepositoryException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void clearNamespaces() throws RepositoryException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setParserConfig(ParserConfig arg0) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ParserConfig getParserConfig() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ValueFactory getValueFactory() {
		throw new UnsupportedOperationException();
	}
	
	public class RemoteTupleQuery implements TupleQuery {
	    
	    private final IPreparedTupleQuery q;
	    
	    public RemoteTupleQuery(final IPreparedTupleQuery q) {
	        this.q = q;
	    }
	    
	    public UUID getQueryId() {
	        return q.getQueryId();
	    }
	    
        @Override
        public TupleQueryResult evaluate() throws QueryEvaluationException {
            try {
                return q.evaluate(BigdataSailRemoteRepositoryConnection.this);
            } catch (Exception ex) {
                throw new QueryEvaluationException(ex);
            }
        }

        /**
         * @see http://trac.bigdata.com/ticket/914 (Set timeout on
         *      remote query)
         */
        @Override
        public int getMaxQueryTime() {

            final long millis = q.getMaxQueryMillis();

            if (millis == -1) {
                // Note: -1L is returned if the http header is not specified.
                return -1;
                
            }
            
            return (int) TimeUnit.MILLISECONDS.toSeconds(millis);

        }

        /**
         * @see http://trac.bigdata.com/ticket/914 (Set timeout on
         *      remote query)
         */
        @Override
        public void setMaxQueryTime(final int seconds) {
            q.setMaxQueryMillis(TimeUnit.SECONDS.toMillis(seconds));
        }

        @Override
        public void clearBindings() {
            throw new UnsupportedOperationException();
        }

        @Override
        public BindingSet getBindings() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Dataset getDataset() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean getIncludeInferred() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void removeBinding(String arg0) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setBinding(String arg0, Value arg1) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setDataset(Dataset arg0) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setIncludeInferred(boolean arg0) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void evaluate(TupleQueryResultHandler arg0)
                throws QueryEvaluationException {
            throw new UnsupportedOperationException();
        }
        
	}
	
	public class RemoteGraphQuery implements GraphQuery {
	    
        private final IPreparedGraphQuery q;
        
        public RemoteGraphQuery(final IPreparedGraphQuery q) {
            this.q = q;
        }
        
        public UUID getQueryId() {
            return q.getQueryId();
        }
        
        @Override
        public GraphQueryResult evaluate() throws QueryEvaluationException {
            try {
                return q.evaluate(BigdataSailRemoteRepositoryConnection.this);
            } catch (Exception ex) {
                throw new QueryEvaluationException(ex);
            }
        }

        /**
         * @see http://trac.bigdata.com/ticket/914 (Set timeout on
         *      remote query)
         */
        @Override
        public int getMaxQueryTime() {

            final long millis = q.getMaxQueryMillis();

            if (millis == -1) {
                // Note: -1L is returned if the http header is not specified.
                return -1;
                
            }

            return (int) TimeUnit.MILLISECONDS.toSeconds(millis);

        }

        /**
         * @see http://trac.bigdata.com/ticket/914 (Set timeout on
         *      remote query)
         */
        @Override
        public void setMaxQueryTime(final int seconds) {
            q.setMaxQueryMillis(TimeUnit.SECONDS.toMillis(seconds));
        }

        @Override
        public void clearBindings() {
            throw new UnsupportedOperationException();
        }

        @Override
        public BindingSet getBindings() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Dataset getDataset() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean getIncludeInferred() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void removeBinding(String arg0) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setBinding(String arg0, Value arg1) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setDataset(Dataset arg0) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setIncludeInferred(boolean arg0) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void evaluate(RDFHandler arg0)
                throws QueryEvaluationException, RDFHandlerException {
            throw new UnsupportedOperationException();
        }
        
	}

    public class RemoteBooleanQuery implements BooleanQuery {
        
        private final IPreparedBooleanQuery q;
        
        public RemoteBooleanQuery(final IPreparedBooleanQuery q) {
            this.q = q;
        }
        
        public UUID getQueryId() {
            return q.getQueryId();
        }
        
        @Override
        public boolean evaluate() throws QueryEvaluationException {
            try {
                return q.evaluate(BigdataSailRemoteRepositoryConnection.this);
            } catch (Exception ex) {
                throw new QueryEvaluationException(ex);
            }
        }

        /**
         * @see http://trac.bigdata.com/ticket/914 (Set timeout on remote query)
         */
        @Override
        public int getMaxQueryTime() {

            final long millis = q.getMaxQueryMillis();

            if (millis == -1) {
                // Note: -1L is returned if the http header is not specified.
                return -1;
                
            }
            
            return (int) TimeUnit.MILLISECONDS.toSeconds(millis);

        }
        
        /**
         * @see http://trac.bigdata.com/ticket/914 (Set timeout on remote query)
         */
        @Override
        public void setMaxQueryTime(final int seconds) {
            q.setMaxQueryMillis(TimeUnit.SECONDS.toMillis(seconds));
        }

        @Override
        public void clearBindings() {
            throw new UnsupportedOperationException();
        }

        @Override
        public BindingSet getBindings() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Dataset getDataset() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean getIncludeInferred() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void removeBinding(String arg0) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setBinding(String arg0, Value arg1) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setDataset(Dataset arg0) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setIncludeInferred(boolean arg0) {
            throw new UnsupportedOperationException();
        }
        
    }
    
    public class RemoteUpdate implements Update {
        
        private final IPreparedSparqlUpdate q;
        
        public RemoteUpdate(final IPreparedSparqlUpdate q) {
            this.q = q;
        }
        
        public UUID getQueryId() {
            return q.getQueryId();
        }
        
        @Override
        public void execute() throws UpdateExecutionException {
            try {
                q.evaluate(BigdataSailRemoteRepositoryConnection.this);
            } catch (Exception ex) {
                throw new UpdateExecutionException(ex);
            }
        }
        
        @Override
        public void clearBindings() {
            throw new UnsupportedOperationException();
        }

        @Override
        public BindingSet getBindings() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Dataset getDataset() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean getIncludeInferred() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void removeBinding(String arg0) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setBinding(String arg0, Value arg1) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setDataset(Dataset arg0) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setIncludeInferred(boolean arg0) {
            throw new UnsupportedOperationException();
        }

    }

    @Override
    public void begin() throws RepositoryException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isActive() 
            throws UnknownTransactionStateException, RepositoryException {
        throw new UnsupportedOperationException();
    }

}
