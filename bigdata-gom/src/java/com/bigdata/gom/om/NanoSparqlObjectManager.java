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
/*
 * Created on Mar 19, 2012
 */
package com.bigdata.gom.om;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryException;

import com.bigdata.gom.gpo.GPO;
import com.bigdata.gom.gpo.IGPO;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.rdf.sail.webapp.client.IPreparedGraphQuery;
import com.bigdata.rdf.sail.webapp.client.IPreparedTupleQuery;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository.AddOp;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository.RemoveOp;
import com.bigdata.striterator.CloseableIteratorWrapper;
import com.bigdata.striterator.ICloseableIterator;

/**
 * Remote {@link IObjectManager} using the <a href=
 * "https://sourceforge.net/apps/mediawiki/bigdata/index.php?title=NanoSparqlServer"
 * > NanoSparqlServer REST API </a> to communicate with the database.
 * 
 * @author <a href="mailto:martyncutcher@users.sourceforge.net">Martyn Cutcher</a>
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class NanoSparqlObjectManager extends ObjectMgrModel {

    private static final Logger log = Logger
            .getLogger(NanoSparqlObjectManager.class);

    private final RemoteRepository m_repo;
	
	public NanoSparqlObjectManager(final RemoteRepository repo, final String namespace) {
		
        super(repo.getSparqlEndPoint(), BigdataValueFactoryImpl
                .getInstance(namespace));

		m_repo = repo;
	}

	@Override
	public void close() {
	    super.close();
		// m_repo.close();
	}

	@Override
	public ICloseableIterator<BindingSet> evaluate(String query) {
		try {
			final IPreparedTupleQuery q = m_repo.prepareTupleQuery(query);
			final TupleQueryResult res = q.evaluate();
			return  new CloseableIteratorWrapper<BindingSet>(new Iterator<BindingSet>() {

				@Override
				public boolean hasNext() {
					try {
						return res.hasNext();
					} catch (QueryEvaluationException e) {
						throw new RuntimeException(e);
					}
				}

				@Override
				public BindingSet next() {
					try {
						return res.next();
					} catch (QueryEvaluationException e) {
						throw new RuntimeException(e);
					}
				}

				@Override
				public void remove() {
					throw new UnsupportedOperationException();
				}
				
			});
		} catch (RepositoryException e1) {
			e1.printStackTrace();
		} catch (MalformedQueryException e1) {
			e1.printStackTrace();
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return null;
	}

	@Override
	public void execute(String updateStr) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean isPersistent() {
		return true;
	}

	@Override
	public void materialize(IGPO gpo) {
		if (gpo == null || gpo.getId() == null)
			throw new IllegalArgumentException("Materialization requires an identity");
		
		if (log.isTraceEnabled())
			log.trace("Materializing: " + gpo.getId());
		
		((GPO) gpo).reset();
		
		// At present the DESCRIBE query will simply return a set of
		//	statements equivalent to a TupleQuery <id, ?, ?>
//		final String query = "DESCRIBE <" + gpo.getId().toString() + ">";
//		final ICloseableIterator<Statement> stmts = evaluateGraph(query);
//
//		while (stmts.hasNext()) {
//			final Statement stmt = stmts.next();
//			((GPO) gpo).initValue(stmt.getPredicate(), stmt.getObject());				
//		}
		final String query = "SELECT ?p ?v WHERE {<" + gpo.getId().toString() + "> ?p ?v}";
		final ICloseableIterator<BindingSet> res = evaluate(query);
		
		while (res.hasNext()) {
			final BindingSet bs = res.next();
			((GPO) gpo).initValue((URI) bs.getValue("p"), bs.getValue("v"));				
		}
	}

	@Override
	public void insert(final Resource id, final URI key, final Value val) {
		if (log.isTraceEnabled())
			log.trace("Inserting statement: " + id.stringValue() + " " + key.stringValue() + " " + val.stringValue());
		
			final Statement statement = m_valueFactory.createStatement(id, key, val);
			final ArrayList<Statement> batch = new ArrayList<Statement>(1);
			batch.add(statement);
			try {
				m_repo.add(new AddOp(batch));
			} catch (Exception e) {
				throw new RuntimeException("Unable to insert statement", e);
			}
	}

	@Override
	public void retract(final Resource id, final URI key, final Value val) {
		if (false && log.isTraceEnabled())
			log.trace("Removing statement: " + id.stringValue() + " " + key.stringValue() + " " + val.stringValue());
		
			try {
				m_repo.remove(new RemoveOp((URI) id, key, val, null));
			} catch (Exception e) {
				throw new RuntimeException("Unable to remove statement", e);
			}
	}

	@Override
	protected void doCommit() {
		// FIXME: The current NanoSparqlServer commits each update.  This
		//	needs to change to associate with an IsolatedTransaction with
		//	an additional commit/rollback protocol
	}

	@Override
	protected void doRollback() {
		// FIXME: see comment above for doCommit()
	}

	@Override
	public ICloseableIterator<Statement> evaluateGraph(String query) {
		try {
			final IPreparedGraphQuery q = m_repo.prepareGraphQuery(query);
			final GraphQueryResult res = q.evaluate();
			return  new CloseableIteratorWrapper<Statement>(new Iterator<Statement>() {

				@Override
				public boolean hasNext() {
					try {
						return res.hasNext();
					} catch (QueryEvaluationException e) {
						throw new RuntimeException(e);
					}
				}

				@Override
				public Statement next() {
					try {
						return res.next();
					} catch (QueryEvaluationException e) {
						throw new RuntimeException(e);
					}
				}

				@Override
				public void remove() {
					throw new UnsupportedOperationException();
				}
				
			});
		} catch (RepositoryException e1) {
			e1.printStackTrace();
		} catch (MalformedQueryException e1) {
			e1.printStackTrace();
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return null;
	}

	@Override
	public void remove(IGPO gpo) {
		// TODO Auto-generated method stub
		
	}

	@Override
	void flushTerms() {
		// TODO Auto-generated method stub
		
	}

	@Override
	void flushStatements() {
		// handle batch removes
		try {
			final RemoveOp rop = m_removes.size() > 0 ? new RemoveOp(m_removes) : null;
			final AddOp iop = m_inserts.size() > 0 ? new AddOp(m_inserts) : null;
			
			if (rop != null && iop != null) {
				m_repo.update(rop, iop);
			} else if (iop != null) {
				m_repo.add(iop);
			} else if (rop != null) {
				m_repo.remove(rop);
			}
			
			m_inserts.clear();
			m_removes.clear();
		} catch (Exception e) {
			throw new RuntimeException("Unable to flush statements", e);
		}
	}

}
