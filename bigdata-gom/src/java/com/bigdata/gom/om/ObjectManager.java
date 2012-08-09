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

import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
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
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.striterator.CloseableIteratorWrapper;
import com.bigdata.striterator.ICloseableIterator;

/**
 * An {@link IObjectManager} for use with an embedded database, including JSP
 * pages running in the same webapp as the NanoSparqlServer and applications
 * that do not expose a public web interface.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class ObjectManager extends ObjectMgrModel {
    
	private static final Logger log = Logger.getLogger(ObjectManager.class);
	
	final private BigdataSailRepository m_repo;
	private BigdataSailRepositoryConnection  m_cxn;
	
    /**
     * 
     * @param endpoint
     *            A SPARQL endpoint that may be used to communicate with the
     *            database.
     * @param cxn
     *            A connection to the database.
     */
    public ObjectManager(final String endpoint, final BigdataSailRepository cxn) {

        super(endpoint, cxn.getValueFactory());

        m_repo = cxn;

    }
	
	/**
	 * @return direct repository connection
	 */
	public BigdataSailRepository getRepository() {
		return m_repo;
	}
	
	@Override
	public void close() {
		try {
            if (m_repo.getSail().isOpen())
                m_repo.shutDown();
		} catch (RepositoryException e) {
			log.error("Problem with close", e);
		}
		super.close();
	}
	
	@Override
	public ICloseableIterator<BindingSet> evaluate(final String query) {
		try {
			final TupleQuery q = getUnisolatedConnection().prepareTupleQuery(QueryLanguage.SPARQL, query);
			final TupleQueryResult res = q.evaluate();
			return new CloseableIteratorWrapper<BindingSet>(new Iterator<BindingSet>() {

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
			final GraphQuery q = getUnisolatedConnection().prepareGraphQuery(QueryLanguage.SPARQL, query);
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

	public void materializeWithDescribe(IGPO gpo) {
		if (log.isTraceEnabled())
			log.trace("Materializing: " + gpo.getId());
		
		((GPO) gpo).reset();
		
		// At present the DESCRIBE query will simply return a set of
		//	statements equivalent to a TupleQuery <id, ?, ?>
		final String query = "DESCRIBE <" + gpo.getId().toString() + ">";
		final ICloseableIterator<Statement> stmts = evaluateGraph(query);

		int statements = 0;
		while (stmts.hasNext()) {
			final Statement stmt = stmts.next();
			((GPO) gpo).initValue(stmt.getPredicate(), stmt.getObject());	
			statements++;
		}
		
		if (log.isTraceEnabled())
			log.trace("Materializing: " + gpo.getId() + " with " + statements + " statements");
	}

	@Override
	public void materialize(IGPO gpo) {
		if (false) {
			materializeWithDescribe(gpo);
			return;
		}
		
		if (log.isTraceEnabled())
			log.trace("Materializing: " + gpo.getId());
		
		((GPO) gpo).reset();
		
		// At present the DESCRIBE query will simply return a set of
		//	statements equivalent to a TupleQuery <id, ?, ?>
		// final String query = "DESCRIBE <" + gpo.getId().toString() + ">";
		final String query = "SELECT ?p ?v WHERE {<" + gpo.getId().toString() + "> ?p ?v}";
		final ICloseableIterator<BindingSet> res = evaluate(query);
		
		while (res.hasNext()) {
			final BindingSet bs = res.next();
			((GPO) gpo).initValue((URI) bs.getValue("p"), bs.getValue("v"));				
		}
	}

	@Override
	public void insert(Resource id, URI key, Value val) throws RepositoryException {
		if (log.isTraceEnabled())
			log.trace("Inserting statement: " + id.stringValue() + " " + key.stringValue() + " " + val.stringValue());
		
		// experiment with adding using batch syntax
		if (false) {
			// m_cxn.getTripleStore().addStatement(id, key, val);
			getUnisolatedConnection().add(id, key, val);
		} else {
			final ISPO spo = new BigdataStatementImpl((BigdataResource) id, 
					(BigdataURI) key, 
					(BigdataValue) val, null, StatementEnum.Explicit, false);
			
			getUnisolatedConnection().getTripleStore().addStatements(new ISPO[] {spo}, 1);
		}
	}

	@Override
	public void retract(Resource id, URI key, Value val) throws RepositoryException {
		if (log.isTraceEnabled())
			log.trace("Removing statement: " + id.stringValue() + " " + key.stringValue() + " " + val.stringValue());
		getUnisolatedConnection().remove(id, key, val);
	}

    private BigdataSailRepositoryConnection getUnisolatedConnection() {
        if (m_cxn == null) {
            try {
                m_cxn = m_repo.getUnisolatedConnection();
                m_cxn.setAutoCommit(false);
            } catch (RepositoryException e) {
                throw new RuntimeException("Unable to establish unisolated connection", e);
            }
        }
        
        return m_cxn;
    }

	@Override
	protected void doCommit() {
		getUnisolatedConnection().getTripleStore().commit();
		try {
			m_cxn.close();
		} catch (RepositoryException e) {
			throw new RuntimeException("Problem closing connection", e);
		} finally {
			m_cxn = null;
		}
	}

    /**
     * doRollback handles the "partial" updates written to maintain referential
     * integrity and also incremental updates of "dirty" objects.
     */
    @Override
    protected void doRollback() {
		getUnisolatedConnection().getTripleStore().abort();
		try {
			m_cxn.close();
		} catch (RepositoryException e) {
			throw new RuntimeException("Problem closing connection", e);
		} finally {
			m_cxn = null;
		}
	}

    @Override
    public void remove(final IGPO gpo) {
        try {
            // Removes all references
            final BigdataSailRepositoryConnection cxn = getUnisolatedConnection();
            cxn.remove(gpo.getId(), null, null);
            cxn.remove((Resource) null, null, gpo.getId());
            // TODO This is not marking the IGPO as removed?
        } catch (RepositoryException e) {
            throw new RuntimeException("Unable to remove object", e);
        }
    }

	@Override
	void flushTerms() {
		if (m_terms.size() > 0) {
			final BigdataValue[] terms = new BigdataValue[m_terms.size()];
			m_terms.toArray(terms);
			m_terms.clear();
			final long start = System.currentTimeMillis();
			getUnisolatedConnection().getTripleStore().addTerms(terms);
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
			getUnisolatedConnection().getTripleStore().removeStatements(spos, spos.length);
        }

        // handle batch inserts
        if (m_inserts.size() > 0) {
            
            final ISPO[] spos = statementsToSPO(m_inserts);
            
            m_inserts.clear();
            
            getUnisolatedConnection().getTripleStore().addStatements(
                    spos, spos.length);

		}

	}
	
    SPO[] statementsToSPO(final List<Statement> statements) {

        final int size = statements.size();
        final SPO[] ret = new SPO[size];

        for (int i = 0; i < size; i++) {

            final BigdataStatement s = (BigdataStatement) statements.get(i);

            ret[i] = new SPO(s.getSubject().getIV(), s.getPredicate().getIV(),
                    s.getObject().getIV(), StatementEnum.Explicit);

            if (ret[i].s == null || ret[i].p == null || ret[i].o == null) {

                throw new IllegalStateException("Values must be bound");

            }

        }

        return ret;

    }

}
