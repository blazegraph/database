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
/*
 * Created on Mar 19, 2012
 */

package com.bigdata.rdf.sparql.ast.eval;

import java.util.concurrent.atomic.AtomicLong;

import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.sail.SailException;

import com.bigdata.bop.IBindingSet;
import com.bigdata.journal.ITx;
import com.bigdata.rdf.internal.IVCache;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.store.BD;

/**
 * Extended to expose the connection used to execute the SPARQL UPDATE request.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AST2BOpUpdateContext extends AST2BOpContext {

    public final BigdataSail sail;
    
    public final BigdataValueFactory f;

    public final BigdataSailRepositoryConnection conn;
    
    private boolean includeInferred;
    
    private QueryBindingSet qbs;
    
    private IBindingSet[] bindingSets;

    private Dataset dataset;

    /**
     * The timestamp associated with the commit point for the update and
     * <code>-1</code> until there is a commit.
     */
    private AtomicLong commitTime = new AtomicLong(-1);

    public final boolean isIncludeInferred() {
        
        return includeInferred;
        
    }
    
    public final void setIncludeInferred(final boolean includeInferred) {
        
        this.includeInferred = includeInferred;
        
    }
    
    /**
     * The timestamp associated with the commit point for the update and
     * <code>-1</code> if until there is a commit.
     */
    public long getCommitTime() {
        
        return commitTime.get();
        
    }

    public void setCommitTime(final long commitTime) {

        this.commitTime.set(commitTime);

    }
    
    /**
     * @param astContainer
     * @param db
     * 
     * @throws SailException
     */
    public AST2BOpUpdateContext(final ASTContainer astContainer,
            final BigdataSailRepositoryConnection conn) throws SailException {

        super(astContainer, conn.getTripleStore());

        this.conn = conn;
        
        this.sail = conn.getSailConnection().getBigdataSail();
       
        this.f = (BigdataValueFactory) sail.getValueFactory();
        
    }

    /**
     * The timestamp associated with the update operation (either a read/write
     * transaction or {@link ITx#UNISOLATED}.
     */
    @Override
    public long getTimestamp() {

        return conn.getTripleStore().getTimestamp();

    }

    /**
     * Return the {@link BD#NULL_GRAPH} with the {@link IVCache} resolved and
     * set.
     * 
     * FIXME This should always be part of the Vocabulary and the IVCache should
     * be set (which is always true for the vocabulary).
     */
    @SuppressWarnings("unchecked")
    public BigdataURI getNullGraph() {

        if (nullGraph == null) {

            nullGraph = db.getValueFactory().asValue(BD.NULL_GRAPH);
            
            db.addTerm(nullGraph);
            
            nullGraph.getIV().setValue(nullGraph);
            
        }

        return nullGraph;
        
    }
    
    public IBindingSet[] getBindings() {
        
        return bindingSets;
        
    }

    public void setBindings(final IBindingSet[] bindingSets) {
        
        this.bindingSets = bindingSets;
        
    }

    public QueryBindingSet getQueryBindingSet() {
        
        return qbs;
        
    }

    public void setQueryBindingSet(final QueryBindingSet qbs) {
        
        this.qbs = qbs;
        
    }

    private BigdataURI nullGraph = null;

    public void setDataset(Dataset dataset) {
        this.dataset = dataset;
    }

    public Dataset getDataset() {
        return dataset;
    }
}
