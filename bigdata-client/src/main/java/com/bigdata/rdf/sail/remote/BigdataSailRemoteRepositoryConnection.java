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

package com.bigdata.rdf.sail.remote;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.Iteration;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.net.URL;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Logger;
import org.openrdf.IsolationLevel;
import org.openrdf.model.Graph;
import org.openrdf.model.Namespace;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.LinkedHashModel;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.BooleanQuery;
import org.openrdf.query.Dataset;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.Query;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
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

import com.bigdata.rdf.sail.webapp.client.IPreparedSparqlUpdate;
import com.bigdata.rdf.sail.webapp.client.IRemoteTx;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository.AddOp;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository.RemoveOp;
import com.bigdata.rdf.sail.webapp.client.RemoteRepositoryManager;
import com.bigdata.rdf.sail.webapp.client.RemoteTransactionManager;
import com.bigdata.rdf.sail.webapp.client.RemoteTransactionNotFoundException;

/**
 * An implementation of Sesame's RepositoryConnection interface that wraps a
 * bigdata {@link RemoteRepository}. This provides SAIL API based client access
 * to a bigdata remote NanoSparqlServer.
 * 
 * <h2>Transactions</h2>
 * 
 * The database supports read/write transactions since 1.5.2. Transaction
 * manager is at the database layer, not the {@link Repository} or
 * {@link RepositoryConnection}. Therefore a namespace DOES NOT need to be
 * configured for isolatable indices in order to create and manipulate
 * transactions, but it DOES need to be configured with isolatable indices in
 * order for you to WRITE on the namespace using a transaction.
 * 
 * @see com.bigdata.rdf.sail.webapp.client.RemoteTransactionManager
 * @see com.bigdata.rdf.sail.BigdataSail.Options#ISOLATABLE_INDICES
 * @see <a href="http://trac.bigdata.com/ticket/1156"> Support read/write
 *      transactions in the REST API</a>
 * @see <a href="http://trac.bigdata.com/ticket/698">
 *      BigdataSailRemoteRepositoryConnection should implement interface methods
 *      </a>
 * 
 *      FIXME (***) #698 Fix all the Query objects (TupleQuery, GraphQuery,
 *      BooleanQuery) to support the various possible operations on them, such
 *      as setting a binding.
 */
public class BigdataSailRemoteRepositoryConnection implements RepositoryConnection {

   private static final transient Logger log = Logger
         .getLogger(BigdataSailRemoteRepositoryConnection.class);

    private final BigdataSailRemoteRepository repo;

    public BigdataSailRemoteRepositoryConnection(
            final BigdataSailRemoteRepository repo) {

        this.repo = repo;

    }

   /**
    * Return a {@link RemoteRepository} for this connection.
    * 
    * @see RemoteRepositoryManager#new
    */
   protected RemoteRepository getRepositoryForConnection() {

      final IRemoteTx tx = remoteTx.get();

      if (tx != null) {

         /*
          * Return a RemoteRepository that will use a view that is consistent
          * with an isolated view of the transaction.
          */
         final RemoteRepository rtmp = repo.getRemoteRepository();
         final String sparqlEndpointURL = rtmp.getSparqlEndPoint();
         final RemoteRepositoryManager rmgr = rtmp.getRemoteRepositoryManager();
         
         return rmgr.getRepositoryForURL(sparqlEndpointURL, tx);

      }

      /*
       * The returned repository does not add the &timestamp= URL query
       * parameter.
       */
      return repo.getRemoteRepository();

   }
    
   /**
    * Report the fast range count (aka ESTCARD) associated with the specified
    * access path.
    */
	public long count(final Resource s, final URI p, final Value o, 
			final Resource... c) 
			throws RepositoryException {

		try {
		
			final RemoteRepository remote = getRepositoryForConnection();
	
			return remote.rangeCount(s, p, o, c);
			
		} catch (Exception ex) {
			
			throw new RepositoryException(ex);
			
		}
		
	}

   /**
    * {@inheritDoc}
    */
	@Override
	public RepositoryResult<Statement> getStatements(final Resource s,
            final URI p, final Value o, final boolean includeInferred,
            final Resource... c) throws RepositoryException {
		
		try {
			
			final RemoteRepository remote = repo.getRemoteRepository();
			
			final GraphQueryResult src = 
					remote.getStatements(s, p, o, includeInferred, c);
			
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

   /**
    * {@inheritDoc}
    * 
    * @see <a href="http://trac.bigdata.com/ticket/1109"> hasStatements can
    *      overestimate and ignores includeInferred (REST API) </a>
    */
	@Override
	public boolean hasStatement(final Resource s, final URI p, final Value o,
            final boolean includeInferred, final Resource... c)
            throws RepositoryException {

		try {
			
			final RemoteRepository remote = repo.getRemoteRepository();
			
			return remote.hasStatement(s, p, o, includeInferred, c);
			
		} catch (Exception ex) {
			
			throw new RepositoryException(ex);
			
		}
		
	}

	@Override
	public BooleanQuery prepareBooleanQuery(final QueryLanguage ql,
            final String query, final String baseURI) throws RepositoryException,
            MalformedQueryException {
		
		if (ql != QueryLanguage.SPARQL) {
			
			throw new UnsupportedOperationException("unsupported query language: " + ql);
			
		}
		
		try {
			
			return new BigdataRemoteBooleanQuery(repo.getRemoteRepository(), query, baseURI);
		
		} catch (Exception ex) {
			
			throw new RepositoryException(ex);
			
		}
		
	}

	@Override
    public BooleanQuery prepareBooleanQuery(final QueryLanguage ql,
            final String query)
            throws RepositoryException, MalformedQueryException {

        return prepareBooleanQuery(ql, query, null);
        
	}

	@Override
    public GraphQuery prepareGraphQuery(final QueryLanguage ql,
            final String query, final String baseURI) throws RepositoryException,
            MalformedQueryException {

		if (ql != QueryLanguage.SPARQL) {
			
			throw new UnsupportedOperationException("unsupported query language: " + ql);
			
		}
		
		try {
			
			return new BigdataRemoteGraphQuery(repo.getRemoteRepository(), query, baseURI);
		
		} catch (Exception ex) {
			
			throw new RepositoryException(ex);
			
		}

	}

	@Override
    public GraphQuery prepareGraphQuery(final QueryLanguage ql,
            final String query)
            throws RepositoryException, MalformedQueryException {

        return prepareGraphQuery(ql, query, null);    
		
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

        return prepareQuery(ql, query);

    }

	@Override
    public TupleQuery prepareTupleQuery(final QueryLanguage ql,
            final String query, final String baseURI)
            throws RepositoryException, MalformedQueryException {

		if (ql != QueryLanguage.SPARQL) {
			
			throw new UnsupportedOperationException("unsupported query language: " + ql);
			
		}

		try {
			
			final RemoteRepository remote = repo.getRemoteRepository();
			
			return new BigdataRemoteTupleQuery(remote, query, baseURI);
		
		} catch (Exception ex) {
			
			throw new RepositoryException(ex);
			
		}
	}

    @Override
    public TupleQuery prepareTupleQuery(final QueryLanguage ql,
            final String query) throws RepositoryException,
            MalformedQueryException {
    	
        return prepareTupleQuery(ql, query, null);	
		
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
		
		final Graph g = new LinkedHashModel();

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
    * <strong>single statement updates not recommended for performance
    * reasons</strong>. Remember, batch is beautiful.
    * <p>
    * {@inheritDoc}
    */
	@Override
	public void add(final Statement stmt, final Resource... c)
			throws RepositoryException {

//		log.warn("single statement updates not recommended");
		
		final Graph g = new LinkedHashModel();

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

		final Graph g = new LinkedHashModel();

      while (stmts.hasNext()) {

         g.add(stmts.next());

      }
	
		remove(g, c);

	}

   /**
    * <strong>single statement updates not recommended for performance
    * reasons</strong>. Remember, batch is beautiful.
    * <p>
    * {@inheritDoc}
    */
    @Override
	public void remove(final Statement stmt, final Resource... c)
			throws RepositoryException {
		
//		log.warn("single statement updates not recommended");
		
		final Graph g = new LinkedHashModel();
	
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

		try {
			
			op.setContext(c);
			
			final RemoteRepository remote = repo.getRemoteRepository();
			
			remote.remove(op);
			
		} catch (Exception ex) {
			
			throw new RepositoryException(ex);
			
		}

	}

	@Override
	public Repository getRepository() {
		
		return repo;
		
	}

	@Override
	public RepositoryResult<Resource> getContextIDs()
			throws RepositoryException {
		
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

   /**
    * {@inheritDoc}
    * <p>
    * This uses the HASSTMT REST API method to do the minimum amount of work on
    * the server.
    * 
    * @see <a href="http://trac.bigdata.com/ticket/1174" >
    *      Sail/Repository.size() should not count inferred statements </a>
    * 
    *      TODO size() and isEmpty() are defined by openrdf in terms of explicit
    *      statements. However, we have historically always realized them in
    *      terms of statements without respect to filtering out inferences (when
    *      present).
    */
	@Override
   public boolean isEmpty() throws RepositoryException {

      return hasStatement(null/* s */, null/* p */, null/* o */, false/* includeInferred */);
      
   }

	/**
	 * {@inheritDoc}
	 * 
    * @see <a href="http://trac.bigdata.com/ticket/1174" >
    *      Sail/Repository.size() should not count inferred statements </a>
    * 
    *      TODO size() and isEmpty() are defined by openrdf in terms of explicit
    *      statements. However, we have historically always realized them in
    *      terms of statements without respect to filtering out inferences (when
    *      present).
	 */
	@Override
	public long size(final Resource... c) throws RepositoryException {
		
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
    public void exportStatements(final Resource s, final URI p, final Value o,
            final boolean includeInferred, final RDFHandler handler,
            final Resource... c) throws RepositoryException,
            RDFHandlerException {

        try {

            final RemoteRepository remote = repo.getRemoteRepository();

            final GraphQueryResult src = remote.getStatements(s, p, o,
                    includeInferred, c);
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
    public Update prepareUpdate(final QueryLanguage ql, final String query)
            throws RepositoryException, MalformedQueryException {

		if (ql != QueryLanguage.SPARQL) {
			
			throw new UnsupportedOperationException("unsupported query language: " + ql);
			
		}
		
		try {
			
			final RemoteRepository remote = repo.getRemoteRepository();
			
			final IPreparedSparqlUpdate update = remote.prepareUpdate(query);
			
			/*
			 * Only execute() is currently supported.
			 */
			return new Update() {
				private int maxExecutionTime;
				@Override
				public void execute() throws UpdateExecutionException {
					try {
						update.evaluate();
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
				public void removeBinding(String arg0) {
					throw new UnsupportedOperationException();
				}
	
				@Override
				public void setBinding(String arg0, Value arg1) {
					throw new UnsupportedOperationException();
				}
	
            @Override
            public Dataset getDataset() {
               throw new UnsupportedOperationException();
            }
   
				@Override
				public void setDataset(Dataset arg0) {
					throw new UnsupportedOperationException();
				}
	
            @Override
            public boolean getIncludeInferred() {
               throw new UnsupportedOperationException();
            }

				@Override
				public void setMaxExecutionTime(int i) {
					this.maxExecutionTime = i;
				}

				@Override
				public int getMaxExecutionTime() {
					return maxExecutionTime;
				}

				@Override
				public void setIncludeInferred(boolean arg0) {
					throw new UnsupportedOperationException();
				}

			};
			
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
		return repo.getValueFactory();
	}

	/**
	 * <code>true</code> iff the connection is open.
	 */
	private final AtomicBoolean open = new AtomicBoolean(true);
	
   /**
    * The current transaction. This is <code>null</code> before {@link #begin()}
    * is called the first time. It is set to <code>null</code> by both
    * {@link #rollback()} and {@link #commit()}. If it is non-<code>null</code>
    * when {@link #close()} is called, then the associated transaction will be
    * aborted (per the openrdf API all non-committed state is lost on
    * {@link #close()}).
    * <p>
    * Note: The monitor of this object is also used as a synchronization point.
    */
	private final AtomicReference<IRemoteTx> remoteTx = new AtomicReference<IRemoteTx>();
	
   @Override
   public boolean isOpen() throws RepositoryException {

      return open.get();
      
   }

   private void assertOpen() throws RepositoryException {

      if (!open.get())
         throw new RepositoryException("Connection is not open");

   }
   
   /**
    * {@inheritDoc}
    * <p>
    * Note: This is deprecated in openrdf since 2.7.x. The semantics are that a
    * connection without an active transaction is in "auto-commit" mode.
    */
   @Deprecated
   @Override
   public boolean isAutoCommit() throws RepositoryException {

      /*
       * A connection is defined as being in auto-commit mode if no transaction
       * is active.
       */

      return remoteTx.get() == null;

   }

   /**
    * {@inheritDoc}
    * <p>
    * <p>
    * Note: This is deprecated in openrdf since 2.7.x. The semantics are that a
    * connection without an active transaction is in "auto-commit" mode. If
    * there is an open transaction and auto-commit is disabled, the open
    * transaction is committed. This is per the openrdf API.
    */
   @Deprecated
   @Override
   public void setAutoCommit(final boolean autoCommit) throws RepositoryException {
      synchronized (remoteTx) {
         if (autoCommit == false && remoteTx.get() == null) {
            // NOP.
            return;
         }
         if (remoteTx.get() != null) {
            // Convert the connection to autocommit by committing the tx.
            commit();
         }
      }
   }

	@Override
   public void close() throws RepositoryException {
      if (open.compareAndSet(true/* expect */, false/* newValue */)) {
         /*
          * The connection was open and is now closed. We submit a runnable
          * abort the current transaction (if any).
          * 
          * Note: The runnable is not run in the callers thread since otherwise
          * close() will block until it can gain the [remoteTx] monitor.
          */
         repo.getRemoteRepository().getRemoteRepositoryManager().getExecutor().execute(new Runnable() {
            @Override
            public void run() {
               /*
                * Note: This invokes the tx.abort() without regard to whether or
                * not the connection is open (it will be closed since we closed
                * it before submitting this for execution).
                */
               synchronized (remoteTx) {
                  final IRemoteTx tx = remoteTx.get();
                  if (tx != null) {
                     try {
                        tx.abort();
                     } catch (RuntimeException e) {
                        // Log and ignore.
                        log.error(e, e);
                     } catch (Exception e) {
                        // Log and ignore.
                        log.error(e, e);
                     } finally {
                        // Clear the reference since we are closing the conn.
                        remoteTx.set(null/* newValue */);
                     }
                  }
               }
            }
         });
      }
   }

   @Override
   public boolean isActive() throws UnknownTransactionStateException,
         RepositoryException {
      /*
       * First, do some non-blocking tests. If we can prove that the connection
       * is not open or that there is no active transaction with a non-blocking
       * test then we return immediately.
       */
      assertOpen();
      if (remoteTx.get() == null) {
         // Non-blocking test.
         return false;
      }
      /*
       * Now grab the lock and test again.
       */
      synchronized (remoteTx) {
         assertOpen();
         final IRemoteTx tx = remoteTx.get();
         if (tx == null) {
            // no transaction is active.
            return false;
         }
         /*
          * The client has an active transaction.
          * 
          * Note: This DOES NOT indicate that the transaction is still active on
          * the server!
          */
         return true;
      }
   }

   @Override
   public void setIsolationLevel(IsolationLevel isolationLevel) throws IllegalStateException {
   	  // TODO: what do we do here?
   }

   @Override
   public IsolationLevel getIsolationLevel() {
   	  return null;
   }

   @Override
   public void begin() throws RepositoryException {
      assertOpen(); // non-blocking.
      synchronized (remoteTx) {
         assertOpen();
         if (remoteTx.get() != null)
            throw new RepositoryException("Active transaction exists");
         try {
            remoteTx.set(repo.getRemoteRepository()
                  .getRemoteRepositoryManager().getTransactionManager()
                  .createTx(RemoteTransactionManager.UNISOLATED));
         } catch (RuntimeException e) {
            throw new RepositoryException(e);
         }
      }
   }

	@Override
	public void begin(IsolationLevel isolationLevel) throws RepositoryException {
   		// There's only one isolation level supported - snapshot isolation
		begin();
	}

	/**
    * Begin a read-only transaction. Since all read operations have snapshot
    * isolation, this is only necessary when multiple read operations need to
    * read on the same commit point.
    */
   public void beginReadOnly() throws RepositoryException {
      assertOpen(); // non-blocking.
      synchronized (remoteTx) {
         assertOpen();
         if (remoteTx.get() != null)
            throw new RepositoryException("Active transaction exists");
         try {
            remoteTx.set(repo.getRemoteRepository()
                  .getRemoteRepositoryManager().getTransactionManager()
                  .createTx(RemoteTransactionManager.READ_COMMITTED));
         } catch (RuntimeException e) {
            throw new RepositoryException(e);
         }
      }
   }

   /**
    * Begin a read-only transaction that reads against the most recent committed
    * state whose commit timestamp is less than or equal to timestamp.
    * <p>
    * Note: Since all read operations have snapshot isolation, this is only
    * necessary when multiple read operations need to read on the same commit
    * point.
    * 
    * TODO While the ability to do read-only operations against a specified
    * timestamp without a transaction exists, it is not exposed by this
    * interface nor can be accomplished using {@link RemoteRepository} since
    * that interface also lacks mechanisms (e.g.,
    * prepareTupleQuery(String:query,long:commitTime)) to express this request.
    */
   public void beginReadOnly(final long timestamp) throws RepositoryException {
      if (timestamp <= 0)
         throw new IllegalArgumentException();
      assertOpen(); // non-blocking.
      synchronized (remoteTx) {
         assertOpen();
         if (remoteTx.get() != null)
            throw new RepositoryException("Active transaction exists");
         try {
            remoteTx.set(repo.getRemoteRepository()
                  .getRemoteRepositoryManager().getTransactionManager()
                  .createTx(timestamp));
         } catch (RuntimeException e) {
            throw new RepositoryException(e);
         }
      }
   }

   @Override
   public void commit() throws RepositoryException {
      assertOpen(); // non-blocking.
      synchronized (remoteTx) {
         assertOpen(); // non-blocking.
         final IRemoteTx tx = remoteTx.get();
         if (tx != null) {
            try {
               tx.commit();
               remoteTx.set(null/* newValue */);
            } catch (RemoteTransactionNotFoundException e) {
               throw new UnknownTransactionStateException(e);
            } catch (RuntimeException e) {
               throw new UnknownTransactionStateException(e);
            }
         }
      }
   }

   @Override
   public void rollback() throws RepositoryException {
      assertOpen(); // non-blocking.
      synchronized (remoteTx) {
         assertOpen(); // non-blocking.
         final IRemoteTx tx = remoteTx.get();
         if (tx != null) {
            try {
               tx.abort();
               remoteTx.set(null/* newValue */);
            } catch (RemoteTransactionNotFoundException e) {
               throw new UnknownTransactionStateException(e);
            } catch (Exception e) {
               throw new UnknownTransactionStateException(e);
            }
         }
      }
   }

}
