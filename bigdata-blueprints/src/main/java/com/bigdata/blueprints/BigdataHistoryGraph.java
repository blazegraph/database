/**
Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.openrdf.model.URI;
import org.openrdf.repository.RepositoryConnection;

import com.bigdata.blueprints.BigdataGraphListener.BigdataGraphEdit;
import com.bigdata.blueprints.BigdataGraphListener.BigdataGraphEdit.Action;
import com.bigdata.rdf.changesets.ChangeAction;
import com.bigdata.rdf.changesets.ChangeRecord;
import com.bigdata.rdf.changesets.IChangeLog;
import com.bigdata.rdf.changesets.IChangeRecord;
import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
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
public class BigdataHistoryGraph extends BigdataGraph implements TransactionalGraph, IChangeLog {

    private static transient final Logger log = Logger.getLogger(BigdataHistoryGraph.class);

	final BigdataSailRepository repo;
	
//	transient BigdataSailRepositoryConnection cxn;
	
	final List<BigdataGraphListener> listeners = 
	        Collections.synchronizedList(new LinkedList<BigdataGraphListener>());
	
	/**
	 * Write the graph history back using RDF*.
	 */
	private final boolean history;
	
	/**
	 * Create a Blueprints wrapper around a {@link BigdataSail} instance.
	 */
    public BigdataHistoryGraph(final BigdataSail sail) {
        this(sail, BigdataRDFFactory.INSTANCE);
    }
    
    /**
     * Create a Blueprints wrapper around a {@link BigdataSail} instance with
     * a non-standard {@link BlueprintsValueFactory} implementation.
     */
    public BigdataHistoryGraph(final BigdataSail sail, 
            final BlueprintsValueFactory factory) {
        this(new BigdataSailRepository(sail), factory, new Properties());
    }
    
    /**
     * Create a Blueprints wrapper around a {@link BigdataSail} instance with
     * a non-standard {@link BlueprintsValueFactory} implementation.
     */
    public BigdataHistoryGraph(final BigdataSail sail, 
            final BlueprintsValueFactory factory, final Properties props) {
        this(new BigdataSailRepository(sail), factory, props);
    }
    
    /**
     * Create a Blueprints wrapper around a {@link BigdataSailRepository} 
     * instance.
     */
	public BigdataHistoryGraph(final BigdataSailRepository repo) {
		this(repo, BigdataRDFFactory.INSTANCE, new Properties());
	}
	
    /**
     * Create a Blueprints wrapper around a {@link BigdataSailRepository} 
     * instance with a non-standard {@link BlueprintsValueFactory} implementation.
     */
	public BigdataHistoryGraph(final BigdataSailRepository repo, 
			final BlueprintsValueFactory factory, final Properties props) {
	    super(factory, props);
	    
	    this.repo = repo;
	    this.history = repo.getDatabase().isStatementIdentifiers();
	}
	
	public BigdataSailRepository getRepository() {
	    return repo;
	}
	
    protected final ThreadLocal<BigdataSailRepositoryConnection> cxn = new ThreadLocal<BigdataSailRepositoryConnection>() {
        protected BigdataSailRepositoryConnection initialValue() {
            BigdataSailRepositoryConnection cxn = null;
            try {
                cxn = repo.getUnisolatedConnection();
                cxn.setAutoCommit(false);
                cxn.addChangeLog(BigdataHistoryGraph.this);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            return cxn;
        }
    };

	protected BigdataSailRepositoryConnection getWriteConnection() throws Exception {
//	    if (cxn == null) {
//	        cxn = repo.getUnisolatedConnection();
//	        cxn.setAutoCommit(false);
//	    }
	    return cxn.get();
	}
	
	protected BigdataSailRepositoryConnection getReadConnection() throws Exception {
	    return repo.getReadOnlyConnection();
	}
	
	@Override
	public void commit() {
		try {
//		    if (cxn != null)
//		        cxn.commit();
            final RepositoryConnection cxn = this.cxn.get();
            cxn.commit();
            cxn.close();
            this.cxn.remove();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void rollback() {
		try {
//		    if (cxn != null) {
//    			cxn.rollback();
//    			cxn.close();
//    			cxn = null;
//		    }
		    final RepositoryConnection cxn = this.cxn.get();
		    cxn.rollback();
		    cxn.close();
		    this.cxn.remove();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void shutdown() {
		try {
//		    if (cxn != null) {
//		        cxn.close();
//		    }
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
	    return repo.getDatabase().dumpStore();
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
     * Save these for notification until we have a commit time.
     */
    private final List<IChangeRecord> adds = //Collections.synchronizedList(
            new LinkedList<IChangeRecord>();
    
    /**
     * We need to batch and materialize these.
     */
    private final List<IChangeRecord> removes = //Collections.synchronizedList(
            new LinkedList<IChangeRecord>();
    
    /**
     * Changed events coming from bigdata.
     */
    @Override
    public void changeEvent(final IChangeRecord record) {
        if (log.isDebugEnabled()) {
            log.debug(record + ": " + record.getStatement().s().isStatement());
        }
        /*
         * Ignore statements about statements.
         */
        if (record.getStatement().s().isStatement()) {
            return;
        }
        /*
         * Adds come in already materialized. Removes do not. Batch and
         * materialize at commit or abort notification.
         */
        if (record.getChangeAction() == ChangeAction.REMOVED) {
            removes.add(record);
        } else if (record.getChangeAction() == ChangeAction.INSERTED) {
            adds.add(record);
        }
    }
    
    /**
     * Turn a change record into a graph edit and notify the graph listeners.
     * 
     * @param record
     *          Bigdata change record.
     */
    protected void notify(final IChangeRecord record, final BigdataGraphEdit edit) {
        for (BigdataGraphListener listener : listeners) {
            listener.graphEdited(edit, record.toString());
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
    protected BigdataGraphEdit toGraphEdit(final IChangeRecord record,
            final long timestamp) {
        
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
        
        if (log.isTraceEnabled()) {
            log.trace(records);
        }
        
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
        processAdds(commitTime, true);
        processRemoves(commitTime, true);
        for (BigdataGraphListener listener : listeners) {
            listener.transactionCommited(commitTime);
        }
    }

    /**
     * Notification of transaction aborted.
     */
    @Override
    public void transactionAborted() {
        processAdds(System.currentTimeMillis(), false);
        processRemoves(System.currentTimeMillis(), false);
        for (BigdataGraphListener listener : listeners) {
            listener.transactionAborted();
        }
    }
    
    @Override
    public void close() {
    }

    /**
     * Notify listeners of the add events.
     */
    protected void processAdds(final long timestamp, final boolean committed) {
//        synchronized(adds) {
            final List<IChangeRecord> adds = new LinkedList<IChangeRecord>(this.adds);
            this.adds.clear();
            if (listeners.size() > 0) {
                notify(adds, timestamp, committed);
            }
//        }
    }

    /**
     * Materialize and notify listeners of the remove events.
     */
    protected void processRemoves(final long timestamp, final boolean committed) {
//        synchronized(removes) {
            final List<IChangeRecord> removes = materialize(this.removes);
            this.removes.clear();
            if (listeners.size() > 0) {
                notify(removes, timestamp, committed);
            }
//        }
    }

    /**
     * Notify listeners and write the history back to the database.
     */
    protected void notify(final List<IChangeRecord> records, 
            final long timestamp, final boolean committed) {
        
        final BigdataValueFactory vf = (BigdataValueFactory) 
                repo.getValueFactory();
        final List<BigdataStatement> history = 
                new LinkedList<BigdataStatement>();
        
        if (listeners.size() > 0) {
            for (IChangeRecord record : records) {
                final BigdataGraphEdit edit = toGraphEdit(record, timestamp);
                if (edit != null) {
                    notify(record, edit);
                    if (committed && this.history) {
                        final BigdataStatement stmt = 
                                (BigdataStatement) record.getStatement();
                        final BigdataBNode sid = vf.createBNode(stmt);
                        final URI action =
                                record.getChangeAction() == ChangeAction.REMOVED 
                                    ? super.factory.getRemovedURI() 
                                            : super.factory.getAddedURI();
                        history.add(vf.createStatement(sid, action, 
                                vf.createXSDDateTime(timestamp)));
                    }
                }
            }
        }
        if (committed && this.history && !history.isEmpty()) {
            // write history
            final Runnable task = new Runnable() {
                public void run() {
                    try {
                        _run();
                    } catch (RuntimeException ex) {
                        throw ex;
                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                }
                private void _run() throws Exception {
                    final BigdataSailRepositoryConnection cxn = BigdataHistoryGraph.this.cxn.get();
                    try {
                        if (log.isDebugEnabled()) {
                            log.debug("writing history:");
                            for (BigdataStatement stmt : history) {
                                log.debug(stmt);
                            }
                        }
                        cxn.add(history);
                        cxn.commit();
                        if (log.isDebugEnabled()) {
                            log.debug("done");
                        }
                    } finally {
                        cxn.rollback();
                    }
                }
            };
            task.run();
//            repo.getDatabase().getExecutorService().submit(task);
        }
        
    }

//    final List<IChangeRecord> history = 
//            Collections.synchronizedList(new LinkedList<IChangeRecord>());
//    
//    private final HistoryCapture history = new HistoryCapture();
//    
//    private static class HistoryCapture {
//        
//        private final List<IChangeRecord> records = new LinkedList<IChangeRecord>();
//        
//        public HistoryCapture() {
//        }
//        
//    }

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
    
}
