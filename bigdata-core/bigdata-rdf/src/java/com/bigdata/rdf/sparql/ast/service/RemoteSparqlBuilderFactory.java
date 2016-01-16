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
 * Created on Mar 3, 2012
 */

package com.bigdata.rdf.sparql.ast.service;

import java.util.HashSet;
import java.util.Set;

import org.openrdf.model.BNode;
import org.openrdf.model.Value;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;



/**
 * Factory encapsulates the logic required to decide on the manner in which
 * solutions will be vectored into the remote service end point and in which the
 * solutions flowing back from that service will be interpreted.
 * <p>
 * The matter of interpretation is staightforward when the BINDINGS clause is
 * accepted by the remote end point and there are no variables which are
 * correlated within a solution through shared blank nodes.
 * <p>
 * Query generation and solution interpretation is significantly more complex
 * when any of those things is not true. There are several strategies which may
 * be used in these cases, including:
 * <ol>
 * <li>Issue one remote query per solution, imposing a FILTER to enforce the
 * variable corrlation since blank nodes can not be send via the BINDINGS
 * clause. (CONS: Too many queries are issued.)</li>
 * <li>Issue one remote query without specifying any BINDINGS. (CONS: The query
 * could be very underconstrained, however we can not detect this case up front
 * since (a) we do not know whether there will be correlated blank nodes until
 * we are ready to generate the query; and (b) we can not know whether the end
 * point supports BINDINGS until we try it at least once (but we could test for
 * that capability and cache knowledge about whether the end point supports that
 * or use the end points service description for this information.)</li>
 * <li>Vector the remote query through a rewrite using a UNION with distinct
 * variables for each presentation of the original graph pattern and values
 * substituted for those variables via BIND() or the like. (CONS: You have to be
 * careful to get the SPARQL correct when the original SERVICE graph pattern is
 * not used exactly as given.)</li>
 * </ol>
 * In fact, we wind up using mixture of (1) and (3).
 * <p>
 * If there is only one source solution and it does not have any bindings, then
 * we send the original SERVICE clause and DO NOT use the BINDINGS clause. This
 * pattern works whether or not the the service end point supports bindings and
 * covers the case where the service is run without vectoring in any solutions.
 * <p>
 * When there one or more non-empty solutions to be vectored and variable
 * correlation is not present (or it is present but there is only one solution),
 * and the service end point supports the BINDINGS clause, then we use the
 * BINDINGS clause to vector the query against the remote SERVICE end point.
 * <p>
 * If there are multiple solutions and correlated variables -or- if the service
 * does not support BINDINGS, then we use the UNION rewrite approach and BIND()
 * the bindings within each alternative of the UNION (as a special case, no
 * UNION is required if there is only one source solution (we can just BIND()
 * the bindings) or if the source solution has no bound variables (we do not
 * need to send any bindings).
 * <p>
 * Note: We do not need to use a rowId to correlate the source solutions and the
 * service's solution. We will always do a hash join of the service solutions
 * with the source solutions. The only time the hash join could be a problem is
 * when there are blank nodes and multiple source solutions. However, we are
 * already required to rewrite the SERVICE clause using a UNION pattern in this
 * case so we can maintain the correlation of the blank nodes through the unique
 * variable names in each alternative of the UNION.
 * 
 * TODO ASK query optimization when there is a single triple pattern which is
 * fully bound AND there is only one solution flowing into the service. (This is
 * a pretty minor optimization and a very special case since we are more likely
 * to have a vector of fully bound soltuions.)
 * 
 * TODO If any of the source solutions is fully unbound, then the other source
 * solutions could be eliminated since we will be running the service fully
 * unbound anyway.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: RemoteSparqlBuilderFactory.java 6068 2012-03-03 21:34:31Z
 *          thompsonbry $
 */
public class RemoteSparqlBuilderFactory {

    /**
     * @param serviceOptions
     *            The configuration options for the service.
     * @param serviceNode
     *            The SERVICE clause.
     * @param bindingSets
     *            The source solutions. These will be used to create a BINDINGS
     *            clause for the query.
     */
    static public IRemoteSparqlQueryBuilder get(
            final IServiceOptions serviceOptions,
            final ServiceNode serviceNode, final BindingSet[] bindingSets) {

        if (serviceOptions == null)
            throw new IllegalArgumentException();

        if (serviceNode == null)
            throw new IllegalArgumentException();

        /*
         * When 'SPARQLVersion.SPARQL_10', there is only one binding set to be vectored and it is
         * empty. We DO NOT use the BINDINGS clause for this case in order to be
         * compatible with services which do and do not support BINDINGS.
         */

        if(serviceOptions.getSPARQLVersion().equals(SPARQLVersion.SPARQL_10)) {
            
            return new RemoteSparql10QueryBuilder(serviceNode);

        }
                
        final boolean singleEmptyBindingSet = (bindingSets.length == 0)
                || (bindingSets.length == 1 && bindingSets[0].size() == 0);
        
        if (!singleEmptyBindingSet && hasCorrelatedBlankNodeBindings(bindingSets)) {

            return new RemoteSparql10QueryBuilder(serviceNode);

        }
        
        if(serviceOptions.getSPARQLVersion().equals(SPARQLVersion.SPARQL_11_DRAFT_BINDINGS)) {
            
            return new RemoteSparql11DraftQueryBuilder(serviceNode);

        }

        return new RemoteSparql11QueryBuilder(serviceNode);

    }

    /**
     * Return <code>true</code> iff (a) there is more than one solution; and (b)
     * any of the solutions has a blank node which is bound for more than one
     * variable. We need to use a different strategy for vectoring the solutions
     * to the remote service when this is true.
     * 
     * @param bindingSets
     *            The solutions.
     */
    static private boolean hasCorrelatedBlankNodeBindings(
            final BindingSet[] bindingSets) {
        
        if (bindingSets.length <= 1) {
            /*
             * Correlation in the variables through shared blank nodes is Ok as
             * long as there is only one solution flowing into the service end
             * point.
             */
            return false;
        }
        
        for (BindingSet bindingSet : bindingSets) {
            Set<BNode> bnodes = null;
            for (Binding b : bindingSet) {
                final Value v = b.getValue();
                if (!(v instanceof BNode))
                    continue;
                if (bnodes == null)
                    bnodes = new HashSet<BNode>();
                final BNode t = (BNode) v;
                if (bnodes.add(t)) {
                    /*
                     * This solution has at least two variable bindings for the
                     * same blank node.
                     */
                    return true;
                }
            }
        }
     
        /*
         * No solution has two or more variables which are bound in that
         * solution to the same blank node.
         */
        return false;
        
    }

}
