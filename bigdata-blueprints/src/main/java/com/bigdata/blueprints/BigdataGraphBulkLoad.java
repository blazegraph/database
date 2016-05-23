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
import java.util.UUID;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.query.impl.AbstractQuery;
import org.openrdf.repository.RepositoryConnection;

import com.bigdata.rdf.changesets.IChangeLog;
import com.bigdata.rdf.changesets.IChangeRecord;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.sail.model.RunningQuery;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;

/**
 * Simple bulk loader that will insert graph data without any consistency
 * checking (won't check for duplicate vertex or edge identifiers). Currently
 * does not overwrite old property values, but we may need to change this.
 * <p>
 * Implements {@link IChangeLog} so that we can report a mutation count.
 * 
 * @author mikepersonick
 * 
 */
public class BigdataGraphBulkLoad extends BigdataGraph implements
		TransactionalGraph, IChangeLog {

	private final BigdataSailRepositoryConnection cxn;

	public BigdataGraphBulkLoad(final BigdataSailRepositoryConnection cxn) {
		this(cxn, BigdataRDFFactory.INSTANCE);
	}

	public BigdataGraphBulkLoad(final BigdataSailRepositoryConnection cxn,
			final BlueprintsValueFactory factory) {
		super(factory);

		this.cxn = cxn;
		this.cxn.addChangeLog(this);
	}

    @Override
	public RepositoryConnection cxn() throws Exception {
		return cxn;
	}

	@Override
	public void commit() {
		try {
			cxn.commit();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void rollback() {
		try {
			cxn.rollback();
			cxn.close();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void shutdown() {
		try {
			cxn.close();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	@Deprecated
	public void stopTransaction(Conclusion arg0) {
	}

	static {

		FEATURES.supportsTransactions = true;

	}

	@Override
	public Edge getEdge(Object arg0) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterable<Edge> getEdges() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterable<Edge> getEdges(String arg0, Object arg1) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Vertex getVertex(final Object key) {

		if (key == null)
			throw new IllegalArgumentException();

		final URI uri = factory.toVertexURI(key.toString());

		try {

			try {

				if (cxn.hasStatement(uri, TYPE, VERTEX, false)) {
					return new BigdataVertex(uri, this);
				}

				return null;

			} finally {

			}

		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Iterable<Vertex> getVertices() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterable<Vertex> getVertices(String arg0, Object arg1) {
		throw new UnsupportedOperationException();
	}

	@Override
	public GraphQuery query() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void removeEdge(Edge arg0) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void removeVertex(Vertex arg0) {
		throw new UnsupportedOperationException();
	}

//	/**
//	 * Set a single-value property on an edge or vertex (remove the old value
//	 * first).
//	 * 
//	 * @see {@link BigdataElement}
//	 */
//	@Override
//	public void setProperty(final URI uri, final URI prop, final Literal val) {
//
//		try {
//
//			final RepositoryConnection cxn = getWriteConnection();
//
//			// // remove the old value
//			// cxn.remove(uri, prop, null);
//
//			// add the new value
//			cxn.add(uri, prop, val);
//
//		} catch (RuntimeException e) {
//			throw e;
//		} catch (Exception e) {
//			throw new RuntimeException(e);
//		}
//
//	}

	/**
	 * Set a multi-value property on an edge or vertex (remove the old values
	 * first).
	 * 
	 * @see {@link BigdataElement}
	 */
	@Override
	public void setProperty(final URI uri, final URI prop,
			final Collection<Literal> vals) {

		try {

			final RepositoryConnection cxn = cxn();

			// // remove the old value
			// cxn.remove(uri, prop, null);

			// add the new values
			for (Literal val : vals) {
				cxn.add(uri, prop, val);
			}

		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

	/**
	 * Add a vertex.
	 */
	@Override
	public Vertex addVertex(final Object key) {

		try {

			final String vid = key != null ? key.toString() : UUID.randomUUID()
					.toString();

			final URI uri = factory.toVertexURI(vid);

			// do we need to check this?
			// if (cxn().hasStatement(vertexURI, TYPE, VERTEX, false)) {
			// throw new IllegalArgumentException("vertex " + vid +
			// " already exists");
			// }

			cxn().add(uri, TYPE, VERTEX);

			return new BigdataVertex(uri, this);

		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

	/**
	 * Add an edge.
	 */
	@Override
	public Edge addEdge(final Object key, final Vertex from, final Vertex to,
			final String label) {

		if (label == null) {
			throw new IllegalArgumentException();
		}

		// if (key != null && !laxEdges) {
		//
		// final Edge edge = getEdge(key);
		//
		// if (edge != null) {
		// if (!(edge.getVertex(Direction.OUT).equals(from) &&
		// (edge.getVertex(Direction.IN).equals(to)))) {
		// throw new IllegalArgumentException("edge already exists: " + key);
		// }
		// }
		//
		// }

		final String eid = key != null ? key.toString() : UUID.randomUUID()
				.toString();

		final URI edgeURI = factory.toEdgeURI(eid);

		try {

			// do we need to check this?
			// if (cxn().hasStatement(edgeURI, TYPE, EDGE, false)) {
			// throw new IllegalArgumentException("edge " + eid +
			// " already exists");
			// }

			final URI fromURI = factory.toVertexURI(from.getId().toString());
			final URI toURI = factory.toVertexURI(to.getId().toString());

			final RepositoryConnection cxn = cxn();
			cxn.add(fromURI, edgeURI, toURI);
			cxn.add(edgeURI, TYPE, EDGE);
			cxn.add(edgeURI, LABEL, factory.toLiteral(label));

			return new BigdataEdge(new StatementImpl(fromURI, edgeURI, toURI),
					this);

		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

	private transient long mutationCountTotal = 0;
	private transient long mutationCountCurrentCommit = 0;
	private transient long mutationCountLastCommit = 0;

	@Override
	public void changeEvent(final IChangeRecord record) {
		mutationCountTotal++;
		mutationCountCurrentCommit++;
	}

	@Override
	public void transactionBegin() {
	}

	@Override
	public void transactionPrepare() {
	}

	@Override
	public void transactionCommited(long commitTime) {
		mutationCountLastCommit = mutationCountCurrentCommit;
		mutationCountCurrentCommit = 0;
	}

	@Override
	public void transactionAborted() {
	}

    @Override
    public void close() {
    }

	public long getMutationCountTotal() {
		return mutationCountTotal;
	}

	public long getMutationCountCurrentCommit() {
		return mutationCountCurrentCommit;
	}

	public long getMutationCountLastCommit() {
		return mutationCountLastCommit;
	}

	@Override
	protected UUID setupQuery(BigdataSailRepositoryConnection cxn,
			ASTContainer astContainer, QueryType queryType, String extQueryId) {
		// This is a NOOP for the BigdataGraphBulkLoad
		throw new RuntimeException(
				"Method is not implemented for BigdataGraphBulkLoad.");
	}

	@Override
	protected void tearDownQuery(UUID queryId) {
		// This is a NOOP for the BigdataGraphBulkLoad
		throw new RuntimeException(
				"Method is not implemented for BigdataGraphBulkLoad.");

	}

	@Override
	public Collection<RunningQuery> getRunningQueries() {
		// This is a NOOP for the BigdataGraphBulkLoad
		throw new RuntimeException(
				"Method is not implemented for BigdataGraphBulkLoad.");
	}

	@Override
	public void cancel(UUID queryId) {
		// This is a NOOP for the BigdataGraphBulkLoad
		throw new RuntimeException(
				"Method is not implemented for BigdataGraphBulkLoad.");
	}

	@Override
	public void cancel(String externalQueryId) {
		// This is a NOOP for the BigdataGraphBulkLoad
		throw new RuntimeException(
				"Method is not implemented for BigdataGraphBulkLoad.");
	}

	@Override
	public void cancel(RunningQuery r) {
		// This is a NOOP for the BigdataGraphBulkLoad
		throw new RuntimeException(
				"Method is not implemented for BigdataGraphBulkLoad.");
	}

	@Override
	public RunningQuery getQueryById(UUID queryId2) {
		// This is a NOOP for the BigdataGraphBulkLoad
		throw new RuntimeException(
				"Method is not implemented for BigdataGraphBulkLoad.");
	}

	@Override
	public RunningQuery getQueryByExternalId(String extQueryId) {
		// This is a NOOP for the BigdataGraphBulkLoad
		throw new RuntimeException(
				"Method is not implemented for BigdataGraphBulkLoad.");
	}

	@Override
	protected boolean isQueryCancelled(UUID queryId) {
		// This is a NOOP for the BigdataGraphBulkLoad
		throw new RuntimeException(
				"Method is not implemented for BigdataGraphBulkLoad.");
	}
	
	@Override
	public boolean isReadOnly() {
	    return false;
	}

}
