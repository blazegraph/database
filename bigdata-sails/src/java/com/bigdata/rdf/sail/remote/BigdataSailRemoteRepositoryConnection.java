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
import java.util.Iterator;

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
import org.openrdf.rio.ParserConfig;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;

import com.bigdata.rdf.sail.webapp.client.IPreparedBooleanQuery;
import com.bigdata.rdf.sail.webapp.client.IPreparedGraphQuery;
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
public class BigdataSailRemoteRepositoryConnection implements RepositoryConnection {

	private static final transient Logger log = Logger.getLogger(BigdataSailRemoteRepositoryConnection.class);
	
	private final BigdataSailRemoteRepository repo;
	
	public BigdataSailRemoteRepositoryConnection(final BigdataSailRemoteRepository repo) {
		
		this.repo = repo;
		
	}
	
	@Override
	public RepositoryResult<Statement> getStatements(Resource s, URI p,
			Value o, boolean includeInferred, Resource... c)
			throws RepositoryException {
		
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

	@Override
	public boolean hasStatement(Resource s, URI p, Value o,
			boolean includeInferred, Resource... c) throws RepositoryException {

		try {
			
			final RemoteRepository remote = repo.getRemoteRepository();
			
			return remote.hasStatement(s, p, o, includeInferred, c);
			
		} catch (Exception ex) {
			
			throw new RepositoryException(ex);
			
		}
		
	}

	@Override
	public BooleanQuery prepareBooleanQuery(QueryLanguage ql, String query)
			throws RepositoryException, MalformedQueryException {
		
		if (ql != QueryLanguage.SPARQL) {
			
			throw new UnsupportedOperationException("unsupported query language: " + ql);
			
		}
		
		try {
			
			final RemoteRepository remote = repo.getRemoteRepository();
			
			final IPreparedBooleanQuery q = remote.prepareBooleanQuery(query);
			
			/*
			 * Only supports evaluate() right now.
			 */
			return new BooleanQuery() {
	
				@Override
				public boolean evaluate() throws QueryEvaluationException {
					try {
						return q.evaluate();
					} catch (Exception ex) {
						throw new QueryEvaluationException(ex);
					}
				}
	
				@Override
				public int getMaxQueryTime() {
					throw new UnsupportedOperationException();
				}
	
				@Override
				public void setMaxQueryTime(int arg0) {
					throw new UnsupportedOperationException();
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
	
			};
		
		} catch (Exception ex) {
			
			throw new RepositoryException(ex);
			
		}
		
	}

	@Override
	public BooleanQuery prepareBooleanQuery(QueryLanguage ql, String query,
			String baseURI) throws RepositoryException, MalformedQueryException {
		
		throw new UnsupportedOperationException("baseURI not supported");
		
	}

	@Override
	public GraphQuery prepareGraphQuery(QueryLanguage ql, String query)
			throws RepositoryException, MalformedQueryException {

		if (ql != QueryLanguage.SPARQL) {
			
			throw new UnsupportedOperationException("unsupported query language: " + ql);
			
		}
		
		try {
			
			final RemoteRepository remote = repo.getRemoteRepository();
			
			final IPreparedGraphQuery q = remote.prepareGraphQuery(query);
			
			/*
			 * Only supports evaluate() right now.
			 */
			return new GraphQuery() {
	
				@Override
				public GraphQueryResult evaluate() throws QueryEvaluationException {
					try {
						return q.evaluate();
					} catch (Exception ex) {
						throw new QueryEvaluationException(ex);
					}
				}
	
				@Override
				public int getMaxQueryTime() {
					throw new UnsupportedOperationException();
				}
	
				@Override
				public void setMaxQueryTime(int arg0) {
					throw new UnsupportedOperationException();
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
				
			};
		
		} catch (Exception ex) {
			
			throw new RepositoryException(ex);
			
		}

	}

	@Override
	public GraphQuery prepareGraphQuery(QueryLanguage ql, String query,
			String baseURI) throws RepositoryException, MalformedQueryException {

		throw new UnsupportedOperationException("baseURI not supported");
		
	}

	@Override
	public Query prepareQuery(QueryLanguage ql, String query)
			throws RepositoryException, MalformedQueryException {
		
		throw new UnsupportedOperationException("please use the specific operation for your query type: prepare[Boolean/Tuple/Graph]Query");
		
	}

	@Override
	public Query prepareQuery(QueryLanguage ql, String query, String baseURI)
			throws RepositoryException, MalformedQueryException {

		throw new UnsupportedOperationException("baseURI not supported");
		
	}

	@Override
	public TupleQuery prepareTupleQuery(QueryLanguage ql, String query)
			throws RepositoryException, MalformedQueryException {

		if (ql != QueryLanguage.SPARQL) {
			
			throw new UnsupportedOperationException("unsupported query language: " + ql);
			
		}

		try {
			
			final RemoteRepository remote = repo.getRemoteRepository();
			
			final IPreparedTupleQuery q = remote.prepareTupleQuery(query);
			
			/*
			 * Only supports evaluate() right now.
			 */
			return new TupleQuery() {
	
				@Override
				public TupleQueryResult evaluate() throws QueryEvaluationException {
					try {
						return q.evaluate();
					} catch (Exception ex) {
						throw new QueryEvaluationException(ex);
					}
				}
	
				@Override
				public int getMaxQueryTime() {
					throw new UnsupportedOperationException();
				}
	
				@Override
				public void setMaxQueryTime(int arg0) {
					throw new UnsupportedOperationException();
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
				
			};
		
		} catch (Exception ex) {
			
			throw new RepositoryException(ex);
			
		}
		
	}

	@Override
	public TupleQuery prepareTupleQuery(QueryLanguage ql, String query,
			String baseURI) throws RepositoryException, MalformedQueryException {

		throw new UnsupportedOperationException("baseURI not supported");
		
	}

	@Override
	public boolean hasStatement(Statement s, boolean includeInferred, Resource... c)
			throws RepositoryException {
		
		return hasStatement(s.getSubject(), s.getPredicate(), s.getObject(), includeInferred, c);
		
	}

	@Override
	public <E extends Exception> void add(
			Iteration<? extends Statement, E> stmts, Resource... c)
			throws RepositoryException, E {
		
		final Graph g = new GraphImpl();
		while (stmts.hasNext()) {
			g.add(stmts.next());
		}
		
		add(g, c);
		
	}

	@Override
	public void add(Resource s, URI p, Value o, Resource... c)
			throws RepositoryException {
		
		add(new StatementImpl(s, p, o), c);
		
	}

	@Override
	public void add(Statement stmt, Resource... c)
			throws RepositoryException {

		log.warn("single statement updates not recommended");
		
		final Graph g = new GraphImpl();
		g.add(stmt);
		
		add(g, c);

	}

	@Override
	public void add(Iterable<? extends Statement> stmts, Resource... c)
			throws RepositoryException {

		final AddOp op = new AddOp(stmts);
		
		add(op, c);
			
	}

	/**
	 * TODO support baseURI
	 */
	@Override
	public void add(Reader input, String baseURI, RDFFormat format, Resource... c)
			throws IOException, RDFParseException, RepositoryException {
		
		final AddOp op = new AddOp(input, format);
		
		add(op, c);

	}

	/**
	 * TODO support baseURI
	 */
	@Override
	public void add(URL input, String baseURI, RDFFormat format, Resource... c)
			throws IOException, RDFParseException, RepositoryException {
		
		final AddOp op = new AddOp(input.toString());
		
		add(op, c);

	}

	/**
	 * TODO support baseURI
	 */
	@Override
	public void add(File input, String baseURI, RDFFormat format, Resource... c)
			throws IOException, RDFParseException, RepositoryException {
		
		final AddOp op = new AddOp(input, format);
		
		add(op, c);

	}

	/**
	 * TODO support baseURI
	 */
	@Override
	public void add(InputStream input, String baseURI, RDFFormat format, Resource... c)
			throws IOException, RDFParseException, RepositoryException {
		
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
			Iteration<? extends Statement, E> stmts, Resource... c)
			throws RepositoryException, E {

		final Graph g = new GraphImpl();
		while (stmts.hasNext())
			g.add(stmts.next());
		
		remove(g, c);

	}

	@Override
	public void remove(Statement stmt, Resource... c)
			throws RepositoryException {
		
		log.warn("single statement updates not recommended");
		
		final Graph g = new GraphImpl();
		g.add(stmt);
		
		remove(g, c);

	}

	@Override
	public void remove(Iterable<? extends Statement> stmts, Resource... c)
			throws RepositoryException {

		final RemoveOp op = new RemoveOp(stmts);
		
		remove(op, c);
		
	}

	@Override
	public void remove(Resource s, URI p, Value o, Resource... c)
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
	public void setAutoCommit(boolean autoCommit) throws RepositoryException {
		
		if (autoCommit == false)
			throw new IllegalArgumentException("only auto-commit is currently supported");
		
	}

	@Override
	public void close() throws RepositoryException {
		// noop
	}

	@Override
	public boolean isOpen() throws RepositoryException {
		
		return true;
		
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

	@Override
	public long size(Resource... c) throws RepositoryException {
		
		try {
			
			final RemoteRepository remote = repo.getRemoteRepository();
			
			return remote.rangeCount(null, null, null, c);
			
		} catch (Exception ex) {
			
			throw new RepositoryException(ex);
			
		}
		
	}

	@Override
	public void clear(Resource... c) throws RepositoryException {
		
		remove(null, null, null, c);

	}

	@Override
	public void export(RDFHandler handler, Resource... c)
			throws RepositoryException, RDFHandlerException {

		exportStatements(null, null, null, true, handler, c);
		
	}

	@Override
	public void exportStatements(Resource s, URI p, Value o,
			boolean includeInferred, RDFHandler handler, Resource... c)
			throws RepositoryException, RDFHandlerException {

		try {
			
			final RemoteRepository remote = repo.getRemoteRepository();
			
			final GraphQueryResult src = 
					remote.getStatements(s, p, o, includeInferred, c);
			
			handler.startRDF();
			while (src.hasNext()) {
				handler.handleStatement(src.next());
			}
			handler.endRDF();
			
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
	public Update prepareUpdate(QueryLanguage ql, String query)
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

			};
			
		} catch (Exception ex) {
			
			throw new RepositoryException(ex);
			
		}
		
	}

	@Override
	public Update prepareUpdate(QueryLanguage ql, String query, String baseURI)
			throws RepositoryException, MalformedQueryException {

		throw new UnsupportedOperationException("baseURI not supported");
		
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

}
