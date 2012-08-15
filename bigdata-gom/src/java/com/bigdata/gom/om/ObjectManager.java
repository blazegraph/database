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
import org.openrdf.model.Graph;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryException;

import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.gom.gpo.GPO;
import com.bigdata.gom.gpo.IGPO;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataResource;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.sparql.ast.cache.CacheConnectionFactory;
import com.bigdata.rdf.sparql.ast.cache.ICacheConnection;
import com.bigdata.rdf.sparql.ast.cache.IDescribeCache;
import com.bigdata.rdf.store.AbstractTripleStore;
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
	final private boolean readOnly;
	final private IDescribeCache m_describeCache;
	
    /**
     * 
     * @param endpoint
     *            A SPARQL endpoint that may be used to communicate with the
     *            database.
     * @param cxn
     *            A connection to the database.
     */
    public ObjectManager(final String endpoint, final BigdataSailRepository cxn) {

        super(endpoint, (BigdataValueFactory) cxn.getValueFactory());

        m_repo = cxn;

        final AbstractTripleStore tripleStore = cxn.getDatabase();

        this.readOnly = tripleStore.isReadOnly();
        
        final QueryEngine queryEngine = cxn.getSail().getQueryEngine();

        final ICacheConnection cacheConn = CacheConnectionFactory
                .getExistingCacheConnection(queryEngine);

        if (cacheConn != null) {

            m_describeCache = cacheConn.getDescribeCache(
                    tripleStore.getNamespace(), tripleStore.getTimestamp());

        } else {

            m_describeCache = null;

        }

    }
	
	/**
	 * @return direct repository connection
	 */
	public BigdataSailRepository getRepository() {
		return m_repo;
	}
	
	@Override
	public void close() {
        super.close();
        try {
            if (m_repo.getSail().isOpen())
                m_repo.shutDown();
        } catch (RepositoryException e) {
            // Per the API.
            throw new IllegalStateException(e);
        }
	}
	
	@Override
	public ICloseableIterator<BindingSet> evaluate(final String query) {

	    BigdataSailRepositoryConnection cxn = null;
		
	    try {

		    cxn = getQueryConnection();
        
		    final TupleQuery q = cxn.prepareTupleQuery(QueryLanguage.SPARQL,
                    query);
            
		    final TupleQueryResult res = q.evaluate();
            
		    return new CloseableIteratorWrapper<BindingSet>(
                    new Iterator<BindingSet>() {

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

	    } catch (Exception ex) {
        
	        throw new RuntimeException(ex);
	        
        } finally {
            
            if (cxn != null) {
                try {
                    cxn.close();
                } catch (RepositoryException e) {
                    log.error(e, e);
                }
            }
            
        }
	    
    }

	public ICloseableIterator<Statement> evaluateGraph(final String query) {

	    BigdataSailRepositoryConnection cxn = null;
        
	    try {
        
	        cxn = getQueryConnection();
            
	        final GraphQuery q = cxn.prepareGraphQuery(QueryLanguage.SPARQL,
                    query);
            
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

	    } catch (Exception t) {
        
	        throw new RuntimeException(t);
	        
        } finally {
            
            if (cxn != null) {
                try {
                    cxn.close();
                } catch (RepositoryException e) {
                    log.error(e, e);
                }
            }
            
        }

	}

	@Override
	public void execute(String updateStr) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isPersistent() {
		return true; //
	}

	private void materializeWithDescribe(final IGPO gpo) {

        if (gpo == null)
            throw new IllegalArgumentException();
	    
	    if (log.isTraceEnabled())
			log.trace("Materializing: " + gpo.getId());
		
		((GPO) gpo).dematerialize();
		
        /*
         * At present the DESCRIBE query will simply return a set of statements
         * equivalent to a TupleQuery <id, ?, ?>.
         */

        if (m_describeCache != null) {

            final IV<?, ?> iv = addResolveIV(gpo);

            final Graph g = m_describeCache.lookup(iv);

            if (g != null) {

                initGPO((GPO) gpo, g.iterator());

                return;

            }

        }

        final String query = "DESCRIBE <" + gpo.getId().toString() + ">";

        final ICloseableIterator<Statement> stmts = evaluateGraph(query);

        initGPO((GPO) gpo, stmts);
  
	}
	
    /**
     * Initialize a {@link IGPO} from a collection of statements.
     * 
     * @param gpo
     *            The gpo.
     * @param stmts
     *            The statements.
     */
    private void initGPO(final GPO gpo, final Iterator<Statement> stmts) {

		int statements = 0;

		while (stmts.hasNext()) {
		
		    final Statement stmt = stmts.next();
			final Resource subject = stmt.getSubject();
			final URI predicate = stmt.getPredicate();
			final Value value = stmt.getObject();
						
			if (subject.equals(gpo.getId())) {

			    // property
                gpo.initValue(predicate, value);

			} else { // links in - add to LinkSet
			    
                gpo.initLinkValue(predicate, subject);
                
            }
			
			statements++;

		}
		
		if (log.isTraceEnabled())
			log.trace("Materialized: " + gpo.getId() + " with " + statements + " statements");
    }

    /**
     * Attempt to add/resolve the {@link IV} for the {@link IGPO}.
     * 
     * @param gpo
     *            The {@link IGPO}.
     *            
     * @return The {@link IV} -or- <code>null</code> iff this is a read-only
     *         connection and the {@link BigdataResource} associated with that
     *         {@link IGPO} is not in the lexicon.
     */
    private IV<?, ?> addResolveIV(final IGPO gpo) {

        final BigdataResource id = gpo.getId();

        IV<?, ?> iv = id.getIV();

        if (iv == null) {

            /*
             * Attempt to resolve the IV. If the connection allows updates then
             * this will cause an IV to be assigned if the Resource was not
             * already in the lexicon.
             */
            
            final BigdataValue[] values = new BigdataValue[] { id };

            m_repo.getDatabase().getLexiconRelation()
                    .addTerms(values, values.length, readOnly);

            // Note: MAY still be null!
            iv = id.getIV();

        }

        // May be null.
        return iv;

	}

    @Override
	public void materialize(final IGPO gpo) {
	    
        if (gpo == null)
            throw new IllegalArgumentException();

		if (true) {
			materializeWithDescribe(gpo);
			return;
		}
		
		if (log.isTraceEnabled())
			log.trace("Materializing: " + gpo.getId());
		
		((GPO) gpo).dematerialize();

        /**
         * At present the DESCRIBE query will simply return a set of statements
         * equivalent to a TupleQuery <id, ?, ?>
         * 
         * <pre>
         * final String query = "DESCRIBE <"; + gpo.getId().toString() + ">";
         * </pre>
         * 
         * TODO URL encoding of the URI in the query?
         */
		
		final String query = "SELECT ?p ?v WHERE {<" + gpo.getId().toString() + "> ?p ?v}";
	
		final ICloseableIterator<BindingSet> res = evaluate(query);
		
		while (res.hasNext()) {

		    final BindingSet bs = res.next();
		    
            ((GPO) gpo).initValue((URI) bs.getValue("p"), bs.getValue("v"));

		}

    }

    @Override
    protected void flushStatements(final List<Statement> m_inserts,
            final List<Statement> m_removes) {

        BigdataSailRepositoryConnection cxn = null;
        try {
            
            // Connection supporting updates.
            cxn = getConnection();

            // handle batch removes
            for (Statement stmt : m_removes) {

                cxn.remove(stmt);

            }

            // handle batch inserts
            for (Statement stmt : m_inserts) {

                cxn.add(stmt);

            }
            
            // Atomic commit.
            cxn.commit();

        } catch (Throwable t) {
        
            if (cxn != null) {
                try {
                    cxn.rollback();
                } catch (RepositoryException e) {
                    log.error(e, e);
                }
            }
        
        } finally {
        
            if (cxn != null) {
                try {
                    cxn.close();
                } catch (RepositoryException e) {
                    log.error(e, e);
                }
            }
            
        }

    }
	
    /**
     * Return an updatable connection.
     * 
     * @throws RepositoryException
     */
    private BigdataSailRepositoryConnection getConnection()
            throws RepositoryException {

        final BigdataSailRepositoryConnection c = m_repo.getConnection();

        c.setAutoCommit(false);

        return c;

    }

    /**
     * Return a read-only connection.
     * 
     * @throws RepositoryException
     */
    private BigdataSailRepositoryConnection getQueryConnection()
            throws RepositoryException {

        final BigdataSailRepositoryConnection c = m_repo
                .getReadOnlyConnection();

        return c;

    }

}
