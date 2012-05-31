package com.bigdata.gom.om;

import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
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
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataStatementImpl;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.striterator.CloseableIteratorWrapper;
import com.bigdata.striterator.ICloseableIterator;

public class ObjectManager extends ObjectMgrModel {
	private static final Logger log = Logger.getLogger(IObjectManager.class);
	
	final BigdataSailRepositoryConnection m_cxn;
	
	public ObjectManager(final BigdataSailRepositoryConnection cxn) {
		super(cxn.getTripleStore().getValueFactory());
		m_cxn = cxn;
	}
	
	@Override
	public void close() {
		try {
			m_cxn.close();
		} catch (RepositoryException e) {
			log.warn("Problem with close", e);
		}
		m_dict.clear();
	}

	@Override
	public ICloseableIterator<BindingSet> evaluate(final String query) {
		try {
			final TupleQuery q = m_cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
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
		}
		
		return null;
	}

	public ICloseableIterator<Statement> evaluateGraph(final String query) {
		try {
			final GraphQuery q = m_cxn.prepareGraphQuery(QueryLanguage.SPARQL, query);
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
		}
		
		return null;
	}
	@Override
	public void execute(String updateStr) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean isPersistent() {
		return true; //
	}

	@Override
	public void materialize(IGPO gpo) {
		if (false && log.isTraceEnabled())
			log.trace("Materializing: " + gpo.getId());
		
		((GPO) gpo).reset();
		
		// At present the DESCRIBE query will simply return a set of
		//	statements equivalent to a TupleQuery <id, ?, ?>
		final String query = "DESCRIBE <" + gpo.getId().toString() + ">";
		final ICloseableIterator<Statement> stmts = evaluateGraph(query);

		while (stmts.hasNext()) {
			final Statement stmt = stmts.next();
			((GPO) gpo).initValue(stmt.getPredicate(), stmt.getObject());				
		}
	}

	@Override
	public void insert(Resource id, URI key, Value val) throws RepositoryException {
		if (false && log.isTraceEnabled())
			log.trace("Inserting statement: " + id.stringValue() + " " + key.stringValue() + " " + val.stringValue());
		
		// experiment with adding using batch syntax
		if (false) {
			// m_cxn.getTripleStore().addStatement(id, key, val);
			m_cxn.add(id, key, val);
		} else {
			final ISPO spo = new BigdataStatementImpl((BigdataResource) id, 
					(BigdataURI) key, 
					(BigdataValue) val, null, StatementEnum.Explicit, false);
			
			m_cxn.getTripleStore().addStatements(new ISPO[] {spo}, 1);
		}
	}

	@Override
	public void retract(Resource id, URI key, Value val) throws RepositoryException {
		if (log.isTraceEnabled())
			log.trace("Removing statement: " + id.stringValue() + " " + key.stringValue() + " " + val.stringValue());
		m_cxn.remove(id, key, val);
	}

	@Override
	void doCommit() {
		m_cxn.getTripleStore().commit();
	}

	/**
	 * doRollback handles the "partial" updates written to maintain referential integrity and also
	 * incremental updates of "dirty" objects.
	 */
	@Override
	void doRollback() {
		m_cxn.getTripleStore().abort();
	}

	@Override
	public void remove(IGPO gpo) {
		try {
			// Removes all references
			m_cxn.remove(gpo.getId(), null, null);
			m_cxn.remove((Resource) null, null, gpo.getId());
		} catch (RepositoryException e) {
			throw new RuntimeException("Unable to remove object", e);
		}
	}

	@Override
	public ValueFactory getValueFactory() {
		return m_valueFactory;
	}

	@Override
	void flushTerms() {
		if (m_terms.size() > 0) {
			final BigdataValue[] terms = new BigdataValue[m_terms.size()];
			m_terms.toArray(terms);
			m_terms.clear();
			final long start = System.currentTimeMillis();
			m_cxn.getTripleStore().addTerms(terms);
			if (log.isTraceEnabled())
				log.trace("Added " + terms.length + " terms: " + (System.currentTimeMillis()-start) + "ms");
		}
	}

	@Override
	void flushStatements() {
		// handle batch removes
		if (m_removes.size() > 0) {
			final ISPO[] spos = statementsToSPO(m_removes);
			m_removes.clear();			
			m_cxn.getTripleStore().removeStatements(spos, spos.length);
		}
		
		// handle batch inserts
		if (m_inserts.size() > 0) {
			final ISPO[] spos = statementsToSPO(m_inserts);
			m_inserts.clear();
			m_cxn.getTripleStore().addStatements(spos, spos.length);
			
		}
	}
	
	SPO[] statementsToSPO(List<Statement> statements) {
		final int size = statements.size();
		SPO[] ret = new SPO[size];
		
		for (int i = 0; i < size; i++) {
			final BigdataStatement s = (BigdataStatement) statements.get(i);
			ret[i] = new SPO(s.getSubject().getIV(), s.getPredicate().getIV(), s.getObject().getIV(), StatementEnum.Explicit);
			if (ret[i].s == null || ret[i].p == null || ret[i].o == null) {
				throw new IllegalStateException("Values must be bound");
			}
		}
		
		return ret;
	}

}
