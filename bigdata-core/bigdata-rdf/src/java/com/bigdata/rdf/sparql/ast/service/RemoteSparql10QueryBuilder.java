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
 * Created on Mar 5, 2012
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
import org.openrdf.query.impl.EmptyBindingSet;

import com.bigdata.bop.IVariable;
import com.bigdata.rdf.sail.webapp.client.AST2SPARQLUtil;

/**
 * Utility class constructs a valid SPARQL query for a remote
 * <code>SPARQL 1.0</code> end point (no <code>BINDINGS</code> clause and does
 * not support SELECT expressions or BIND()).
 * <p>
 * Note: This class functions by adding
 * <code>FILTER (sameTerm(?var,bound-value))</code> instances for each variable
 * binding inside of the SERVICE's graph pattern.
 * <p>
 * Note: Unlike the {@link RemoteSparql11QueryBuilder}, this class does not need
 * to impose a correlation on variables which are bound to the same blank node
 * since we can communicate blank nodes within a <code>FILTER</code>.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RemoteSparql10QueryBuilder implements IRemoteSparqlQueryBuilder {

    private static final Logger log = Logger
            .getLogger(RemoteSparql10QueryBuilder.class);

    // private final ServiceNode serviceNode;

    /** The text "image" of the SERVICE clause. */
    private final String exprImage;

    /**
     * The prefix declarations used within the SERVICE clause (from the original
     * query).
     */
    private final Map<String, String> prefixDecls;

    private final AST2SPARQLUtil util;

    /**
     * The distinct variables "projected" by the SERVICE group graph pattern.
     * The order of this set is not important, but the variables must be
     * distinct.
     * */
    private final Set<IVariable<?>> projectedVars;

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
    public RemoteSparql10QueryBuilder(final ServiceNode serviceNode) {

        if (serviceNode == null)
            throw new IllegalArgumentException();

        // this.serviceNode = serviceNode;

        this.exprImage = serviceNode.getExprImage();

        this.prefixDecls = serviceNode.getPrefixDecls();

        this.projectedVars = serviceNode.getProjectedVars();

        if (exprImage == null)
            throw new IllegalArgumentException();

        if (projectedVars == null)
            throw new IllegalArgumentException();

        this.util = new AST2SPARQLUtil(prefixDecls);

    }

    public String getSparqlQuery(BindingSet[] bindingSets) {

//        /*
//         * When true, there is only one binding set to be vectored and it is
//         * empty.
//         */
//        
//        final boolean singleEmptyBindingSet;
//        
        /*
         * Normalize NO source solutions into a single empty binding set.
         */
        if (bindingSets.length == 0) {

            bindingSets = new BindingSet[] { new EmptyBindingSet() };

//            singleEmptyBindingSet = true;

        } else if (bindingSets.length == 1 && bindingSets[0].size() == 0) {
        
//            singleEmptyBindingSet = true;
            
        } else {
            
//            singleEmptyBindingSet = false;
            
        }

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
         * WHERE clause.
         * 
         * Note: This uses the actual SPARQL text image for the SERVICE's graph
         * pattern.
         * 
         * Note: A FILTER is used to bind each variable.
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
            final int beginIndex = exprImage.indexOf("{") + 1;
            if (beginIndex < 0)
                throw new RuntimeException();
            final int endIndex = exprImage.lastIndexOf("}");
            if (endIndex < beginIndex)
                throw new RuntimeException();
            final String tmp = exprImage.substring(beginIndex, endIndex);
            sb.append("WHERE {\n");
            for (int k = 0; k < bindingSets.length; k++) {
                final BindingSet bset = bindingSets[k];
                if (bindingSets.length > 1) {
                    /*
                     * UNION of SERVICE patterns.
                     */
                    if (k == 0) {
                        // Open the first UNION.
                        sb.append("{\n");
                    } else {
                        // Close / Open UNION.
                        sb.append("\n} UNION {\n");
                    }
                }
                /*
                 * Set of variables which are correlated through shared blank 
                 * nodes -or- null  if there are no such correlated variables.
                 */
                final Map<BNode, Set<String/* vars */>> bnodes = getCorrelatedVarsMap(bset);
                if (bnodes != null) {
                    /*
                     * Impose a same-term constraint for all variables which are
                     * bound to the same blank node.
                     */
                    for (Set<String> sameTermVars : bnodes.values()) {
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
                            sb.append(" sameTerm( ?" + names[0] + ", ?"
                                    + names[i] + ")");
                        }
                        sb.append(" ).\n");
                    }
                }
                for (Binding b : bset) {
                    /*
                     * Add FILTER to bind each (non-blank node) value.
                     */
                    final String name = b.getName();
                    final Value v = b.getValue();
//                    Set<String> sameTermVars;
//                    if (v instanceof BNode && bnodes != null
//                            && ((sameTermVars = bnodes.get((BNode) v)) != null)) {
                    if(!(v instanceof BNode)) {
                        /*
                         * Blank nodes are not permitted a function arguments.
                         * Therefore we need to handle them through sameTerm()
                         * for the correlated variables.
                         * 
                         * Note: If there are no variables which are correlated
                         * because of this blank node, then we just ignore this
                         * blank node. An unbound variable and a blank node have
                         * the same semantics.
                         */
//                        final int nSameTerm = sameTermVars.size();
//                        if (nSameTerm < 2)
//                            continue;
//                        final String[] names = sameTermVars
//                                .toArray(new String[nSameTerm]);
//                        sb.append("FILTER (");
//                        for (int i = 1; i < names.length; i++) {
//                            if (i > 1) {
//                                sb.append(" &&");
//                            }
//                            sb.append(" sameTerm( ?" + names[0] + ", ?"
//                                    + names[i] + ")");
//                        }
//                        sb.append(" ).\n");
//                        /*
//                         * Replace the entry with an empty set so we do not
//                         * regenerate the FILTER when we see the other variables
//                         * which are correlated with this binding.
//                         */
//                        bnodes.put((BNode) v, (Set) Collections.emptySet());
//                    } else {
                        /*
                         * Blank nodes are not permitted a function arguments.
                         * Therefore we need to handle them through sameTerm()
                         * for the correlated variables (the code block above
                         * handles this).
                         */
                        sb.append("FILTER (");
                        sb.append(" sameTerm( ?" + name + ", "
                                + util.toExternal(v) + ")");
                        sb.append(" ).\n");
                    }
                }
                sb.append(tmp); // append SERVICE's graph pattern.
            } // next solution
            if (bindingSets.length > 1) {
                sb.append("\n}\n"); // close the last UNION.
            }
            sb.append("\n}\n");
        }

//        /*
//         * BINDINGS clause.
//         * 
//         * Note: The BINDINGS clause is used to vector the SERVICE request.
//         * 
//         * BINDINGS ?book ?title { (:book1 :title1) (:book2 UNDEF) }
//         */
//        if (!singleEmptyBindingSet) {
//
//            // Variables in a known stable order.
//            final LinkedHashSet<String> vars = getDistinctVars(bindingSets);
//
//            sb.append("BINDINGS");
//
//            // Variable declarations.
//            {
//
//                for (String v : vars) {
//                    sb.append(" ?");
//                    sb.append(v);
//                }
//            }
//
//            // Bindings.
//            {
//                sb.append(" {\n"); //
//                for (BindingSet bindingSet : bindingSets) {
//                    sb.append("(");
//                    for (String v : vars) {
//                        sb.append(" ");
//                        final Binding b = bindingSet.getBinding(v);
//                        if (b == null) {
//                            sb.append("UNDEF");
//                        } else {
//                            final Value val = b.getValue();
//                            final String ext = util.toExternal(val);
//                            sb.append(ext);
//                        }
//                    }
//                    sb.append(" )");
//                    sb.append("\n");
//                }
//                sb.append("}\n");
//            }
//
//        }

        final String q = sb.toString();

        if (log.isInfoEnabled())
            log.info("\n" + q);

        return q;

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

    /**
     * Return a correlated blank node / variables map.
     * <p>
     * Note: This is necessary because we can not have a blank node in a
     * FunctionCall in SPARQL. However, unlike with the BINDINGS clause, we have
     * to do it for each solution because we can vector more than one solution
     * involving correlated blank nodes.
     * 
     * @return The correlated variable bindings map -or- <code>null</code> iff
     *         there are no variables which are correlated through shared blank
     *         nodes.
     */
    static private Map<BNode, Set<String/* vars */>> getCorrelatedVarsMap(
            final BindingSet bindingSet) {

        Map<BNode, Set<String/* vars */>> bnodes = null;
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
                 * Correlated. This blank node is already the binding for some
                 * other variable in this solution.
                 * 
                 * Note: A FILTER can be used to enforce a same-term constraint
                 * for variables correlated via blank nodes, but only for a
                 * single solution.
                 */
            }
            if (!cvars.add(b.getName())) {
                /*
                 * This would imply the same variable was bound more
                 * than once in the solution.
                 */
                throw new AssertionError();
            }
        }
        return bnodes;
    }

}
