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

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.model.BNode;
import org.openrdf.model.Value;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;

import com.bigdata.bop.IVariable;
import com.bigdata.rdf.sail.webapp.client.AST2SPARQLUtil;

/**
 * Utility class constructs a valid SPARQL query for a remote
 * <code>SPARQL 1.1</code> using the <code>VALUES</code> clause to vector
 * solutions into that remote end point.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: RemoteSparqlQueryBuilder.java 6071 2012-03-04 18:08:57Z
 *          thompsonbry $
 */
public class RemoteSparql11QueryBuilder implements IRemoteSparqlQueryBuilder {

    private static final Logger log = Logger
            .getLogger(RemoteSparql10QueryBuilder.class);

//    private final ServiceNode serviceNode;

    /** The text "image" of the SERVICE clause. */
    protected final String exprImage;

    /**
     * The prefix declarations used within the SERVICE clause (from the original
     * query).
     */
    protected final Map<String, String> prefixDecls;

    protected final AST2SPARQLUtil util;
    
    /**
     * The distinct variables "projected" by the SERVICE group graph pattern.
     * The order of this set is not important, but the variables must be
     * distinct.
     * */
    protected final Set<IVariable<?>> projectedVars;

//    private final BindingSet[] bindingSets;

//    /**
//     * This is a vectored implementation.
//     */
//    @Override
//    public boolean isVectored() {
//        
//        return true;
//        
//    }
    
    /**
     * 
     * @param serviceNode
     *            The SERVICE clause.
     */
    public RemoteSparql11QueryBuilder(final ServiceNode serviceNode) {

        if (serviceNode == null)
            throw new IllegalArgumentException();
        
        this.exprImage = serviceNode.getExprImage();
        
        this.prefixDecls = serviceNode.getPrefixDecls();
        
        this.projectedVars = serviceNode.getProjectedVars();

        if (exprImage == null)
            throw new IllegalArgumentException();

        if (projectedVars == null)
            throw new IllegalArgumentException();

        this.util = new AST2SPARQLUtil(prefixDecls) {

            @Override
            public String toExternal(final BNode val) {

                /*
                 * Note: The SPARQL 1.1 GRAMMAR does not permit blank nodes in
                 * the BINDINGS clause. Blank nodes are sent across as an
                 * unbound variable (UNDEF). If there is more than one variable
                 * which takes on the same blank node in a given solution, then
                 * there must be a FILTER imposed when verifies that those
                 * variables have the same bound value for *that* solution (the
                 * constraint can not apply across solutions in which that
                 * correlation is not present).
                 */
                return "UNDEF";

            }
        };

    }

    /**
     * Return an ordered collection of the distinct variable names used in the
     * given caller's solution set.
     * 
     * @param bindingSets
     *            The solution set.
     * 
     * @return The distinct, ordered collection of variables used.
     */
    protected LinkedHashSet<String> getDistinctVars(final BindingSet[] bindingSets) {

        final LinkedHashSet<String> vars = new LinkedHashSet<String>();

        for (BindingSet bindingSet : bindingSets) {

            for (Binding binding : bindingSet) {

                vars.add(binding.getName());

            }

        }

        return vars;

    }

    public String getSparqlQuery(final BindingSet[] bindingSets) {

        final StringBuilder sb = new StringBuilder();

        /*
         * Prefix declarations.
         * 
         * Note: The prefix declarations need to be harvested and passed along
         * since we are using the text image of the SERVICE group graph pattern
         * in the WHERE clause.
         */
        if (prefixDecls != null) {

            for (Map.Entry<String, String> e : prefixDecls.entrySet()) {

                sb.append("\n");
                sb.append("prefix ");
                sb.append(e.getKey());
                sb.append(":");
                sb.append(" <");
                sb.append(e.getValue());
                sb.append(">");
                sb.append("\n");
                
            }
            
        }

        /*
         * SELECT clause.
         */
        {
            sb.append("SELECT ");
            if (projectedVars.isEmpty()) {
                /*
                 * Note: This is a dubious hack for openrdf federated query
                 * testEmptyServiceBlock. Since there are no variables in the
                 * service clause, it was sending an invalid SELECT expression.
                 * It is now hacked to send a "*" instead.
                 */
                sb.append("*");
            } else {
                for (IVariable<?> v : projectedVars) {
                    sb.append(" ?");
                    sb.append(v.getName());
                }
            }
            sb.append("\n");
        }

        /*
         * When true, there is only one binding set to be vectored and it is
         * empty. We DO NOT use the BINDINGS clause for this case in order to be
         * compatible with services which do and do not support BINDINGS.
         */
        final boolean singleEmptyBindingSet = (bindingSets.length == 0)
                || (bindingSets.length == 1 && bindingSets[0].size() == 0);
        
        /*
         * Correlated blank node / variables map.
         * 
         * Note: This map is used outside of the loop over the binding sets
         * because we can only handle correlated variables via the BINDINGS
         * clause when there is a single solution being flowed into the
         * remote service.
         */
        Map<BNode,Set<String/*vars*/>> bnodes = null;
        if (!singleEmptyBindingSet) {
            bnodes = getCorrelatedVariables(bindingSets);
        }

        /*
         * WHERE clause.
         * 
         * Note: This uses the actual SPARQL text image for the SERVICE's graph
         * pattern.
         * 
         * Note: If there are variables which are correlated through shared
         * variables (and there is only one solution to be vectored) then we
         * will impose a same-term constraint on those variables as part of the
         * generated query.
         */
        {
            /*
             * Clip out the graph pattern from the SERVICE clause.
             * 
             * Note: This leaves off the outer curly brackets in case we need to
             * add some FILTERS, BINDS, etc. into the WHERE clause.
             */
            final int beginIndex = exprImage.indexOf("{") + 1 ;
            if (beginIndex < 0)
                throw new RuntimeException();
            final int endIndex = exprImage.lastIndexOf("}");
            if (endIndex < beginIndex)
                throw new RuntimeException();
            final String tmp = exprImage.substring(beginIndex, endIndex);
            sb.append("WHERE {\n");
            if (bnodes != null) {
                /*
                 * Impose a same-term constraint for all variables which are
                 * bound to the same blank node.
                 */
                for(Set<String> sameTermVars : bnodes.values()) {
                    final int nSameTerm = sameTermVars.size();
                    if (nSameTerm < 2)
                        continue;
                    final String[] names = sameTermVars
                            .toArray(new String[nSameTerm]);
                    sb.append("FILTER (");
                    for (int i = 1; i < names.length; i++) {
                        if (i > 1) {
                            sb.append(" &&");
                        }
                        sb.append(" sameTerm( ?" + names[0] + ", ?" + names[i]
                                + ")");
                    }
                    sb.append(" ).\n");
                }
            }
            sb.append(tmp); // append SERVICE's graph pattern.
            sb.append("\n}\n");
        }

        /*
         * VALUES clause.
         * 
         * Note: The VALUES clause is used to vector the SERVICE request.
         * 
         * VALUES (?book ?title) { (:book1 :title1) (:book2 UNDEF) }
         */
        if (!singleEmptyBindingSet) {

            // Variables in a known stable order.
            final LinkedHashSet<String> vars = getDistinctVars(bindingSets);

            sb.append("VALUES");
            sb.append(" (");

            // Variable declarations.
            {

                for (String v : vars) {
                    sb.append(" ?");
                    sb.append(v);
                }
            }
            
            sb.append(")");

            // Bindings.
            {
                sb.append(" {\n"); //
                for (BindingSet bindingSet : bindingSets) {
                    sb.append("(");
                    for (String v : vars) {
                        sb.append(" ");
                        final Binding b = bindingSet.getBinding(v);
                        if (b == null) {
                            sb.append("UNDEF");
                        } else {
                            final Value val = b.getValue();
                            final String ext = util.toExternal(val);
                            sb.append(ext);
                        }
                    }
                    sb.append(" )");
                    sb.append("\n");
                }
                sb.append("}\n");
            }

        }

        final String q = sb.toString();

        if (log.isInfoEnabled())
            log.info("\n" + q);

        return q;
        
    }

    /**
     * Return a correlated blank node / variables map.
     * <p>
     * Note: This is necessary because the BINDINGS clause does not permit blank
     * nodes.
     * 
     * @return The correlated variable bindings map -or- <code>null</code> iff
     *         there are no variables which are correlated through shared blank
     *         nodes.
     * 
     * @throws UnsupportedOperationException
     *             If there are correlated variables and there is more than one
     *             source solution (for this case you need to use the SPARQL 1.0
     *             compatible query generator).
     * 
     * @see RemoteSparql10QueryBuilder
     */
    protected static Map<BNode, Set<String>> getCorrelatedVariables(
           final BindingSet[] bindingSets) {
        Map<BNode, Set<String/* vars */>> bnodes = null;
        for (BindingSet bindingSet : bindingSets) {
            for (Binding b : bindingSet) {
                final Value v = b.getValue();
                if (!(v instanceof BNode))
                    continue;
                if (bnodes == null)
                    bnodes = new LinkedHashMap<BNode, Set<String>>();
                final BNode bnd = (BNode) v;
                // Set of correlated variables.
                Set<String> cvars = bnodes.get(bnd);
                if (cvars == null) {
                    bnodes.put(bnd, cvars = new LinkedHashSet<String>());
                } else {
                    /*
                     * Correlated. This blank node is already the binding
                     * for some other variable in this solution.
                     * 
                     * Note: A FILTER can be used to enforce a same-term
                     * constraint for variables correlated via blank nodes,
                     * but only for a single solution. If there is more than
                     * one solution then you CAN NOT use the BINDINGS clause
                     * to communicate the binding sets without also
                     * rewriting the SERVICE clause as a UNION of the
                     * original SERVICE class for each source binding set.
                     */
                    if (bindingSets.length > 1)
                        throw new UnsupportedOperationException();
                }
                if (!cvars.add(b.getName())) {
                    /*
                     * This would imply the same variable was bound more
                     * than once in the solution.
                     */
                    throw new AssertionError();
                }
            }
        }
        return bnodes;
    }
    
//    /**
//     * {@inheritDoc}
//     * <p>
//     * This implementation returns it's argument.
//     */
//    @Override
//    public BindingSet[] getSolutions(final BindingSet[] serviceSolutions) {
//
//        return serviceSolutions;
//
//    }

}
