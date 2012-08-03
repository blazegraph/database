package com.bigdata.gom.om;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.UUID;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.BindingSet;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryException;

import com.bigdata.gom.gpo.GPO;
import com.bigdata.gom.gpo.IGPO;
import com.bigdata.rdf.model.BigdataResource;
import com.bigdata.rdf.model.BigdataStatementImpl;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.sail.webapp.client.IPreparedGraphQuery;
import com.bigdata.rdf.sail.webapp.client.IPreparedTupleQuery;
import com.bigdata.rdf.sail.webapp.client.IRemoteRepository;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository.AddOp;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository.RemoveOp;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.striterator.CloseableIteratorWrapper;
import com.bigdata.striterator.ICloseableIterator;

public class NanoSparqlObjectManager extends ObjectMgrModel {
	final RemoteRepository m_repo;
	
	public NanoSparqlObjectManager(final UUID uuid, final RemoteRepository repo, final String namespace) {
		super(uuid, BigdataValueFactoryImpl.getInstance(namespace));
		
		m_repo = repo;
	}

	@Override
	public void close() {
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
	void doCommit() {
		// FIXME: The current NanoSparqlServer commits each update.  This
		//	needs to change to associate with an IsolatedTransaction with
		//	an additional commit/rollback protocol
	}

	@Override
	void doRollback() {
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
