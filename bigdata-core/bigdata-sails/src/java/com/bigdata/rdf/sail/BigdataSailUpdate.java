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

package com.bigdata.rdf.sail;

import org.openrdf.query.UpdateExecutionException;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.parser.ParsedUpdate;
import org.openrdf.repository.sail.SailUpdate;

import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.eval.ASTEvalHelper;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Extension API for bigdata.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class BigdataSailUpdate extends SailUpdate implements
        BigdataSailOperation {

    private final ASTContainer astContainer;

    @Override
    public ASTContainer getASTContainer() {
        
        return astContainer;
        
    }

    public BigdataSailUpdate(final ASTContainer astContainer,
            final BigdataSailRepositoryConnection con) {

        super(null/* tupleQuery */, con);

        if (astContainer == null)
            throw new IllegalArgumentException();

        this.astContainer = astContainer;

    }

    @Override
    public ParsedUpdate getParsedUpdate() {
        
        throw new UnsupportedOperationException();
        
    }

    @Override
    public String toString() {

        return astContainer.toString();
        
    }

    @Override
    public AbstractTripleStore getTripleStore() {

        return ((BigdataSailRepositoryConnection) getConnection())
                .getTripleStore();

    }

//    /**
//     * {@inheritDoc}
//     * <p>
//     * The openrdf API here is somewhat at odds with the current LCWD for SPARQL
//     * UPDATE. In order to align them, setting the {@link Dataset} here causes
//     * it to be applied to each {@link DeleteInsertGraph} operation in the
//     * {@link UpdateRoot}. Note that the {@link Dataset} has no effect exception
//     * for the {@link DeleteInsertGraph} operation in SPARQL 1.1 UPDATE (that is
//     * the only operation which has a WHERE clause and which implements the
//     * {@link IDataSetNode} interface).
//     * 
//     * @see <a href="http://www.openrdf.org/issues/browse/SES-963"> Dataset
//     *      assignment in update sequences not properly scoped </a>
//     * 
//     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/593">
//     *      Upgrade to Sesame 2.6.9 </a>
//     */
//    // @Override
//    @Deprecated // Remove once we upgrade to Sesame 2.6.9
//    public void setDataset(final Dataset dataset) {
//
//        this.dataset = dataset;
//
//    }
//
//    /**
//     * {@inheritDoc}
//     * 
//     * @see <a href="http://www.openrdf.org/issues/browse/SES-963"> Dataset
//     *      assignment in update sequences not properly scoped </a>
//     * 
//     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/593">
//     *      Upgrade to Sesame 2.6.9 </a>
//     */
////    @Override
//    @Deprecated // Remove once we upgrade to Sesame 2.6.9
//    public Dataset getActiveDataset() {
//
//        return dataset;
//        
//    }

    @Override
    public void execute() throws UpdateExecutionException {
        
        execute2();
        
    }

    /**
     * Execute a SPARQL UPDATE request.
     * 
     * @return The timestamp of the commit point for that UPDATE.
     * 
     * @throws UpdateExecutionException
     */
    public long execute2() throws UpdateExecutionException {

//        final QueryRoot originalQuery = astContainer.getOriginalAST();
//
////        if (getMaxQueryTime() > 0)
////            originalQuery.setTimeout(TimeUnit.SECONDS
////                    .toMillis(getMaxQueryTime()));
//
//        originalQuery.setIncludeInferred(getIncludeInferred());

        return ASTEvalHelper.executeUpdate(
                ((BigdataSailRepositoryConnection) getConnection()),
                astContainer,//
                dataset,//
                getIncludeInferred(),//
                new QueryBindingSet(getBindings())
                );

    }

//    @Override
//    public BindingSet getBindings() {
//
//    	return super.getBindings();
//    	
//    }

}
