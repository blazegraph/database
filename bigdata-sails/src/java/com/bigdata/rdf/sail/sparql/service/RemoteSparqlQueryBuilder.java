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
 * Created on Mar 3, 2012
 */

package com.bigdata.rdf.sail.sparql.service;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;

import com.bigdata.bop.IVariable;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;

/**
 * Utility class constructs a valid SPARQL query for a remote SERVICE end point.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RemoteSparqlQueryBuilder implements IRemoteSparqlQueryBuilder {

    private final ServiceNode serviceNode;

    /** The text "image" of the SERVICE clause. */
    private final String exprImage;

    /**
     * The prefix declarations used within the SERVICE clause (from the original
     * query).
     */
    private final Map<String, String> prefixDecls;

    /** Reverse map for {@link #prefixDecls}. */
    private final Map<String, String> namespaces;

    /**
     * The distinct variables "projected" by the SERVICE group graph pattern.
     * The order of this set is not important, but the variables must be
     * distinct.
     * */
    private final Set<IVariable<?>> projectedVars;

    private final BindingSet[] bindingSets;

    /**
     * 
     * @param serviceNode
     *            The SERVICE clause.
     * @param bindingSets
     *            The source solutions. These will be used to create a BINDINGS
     *            clause for the query.
     */
    public RemoteSparqlQueryBuilder(
            final ServiceNode serviceNode,
            final BindingSet[] bindingSets) {

        if (serviceNode == null)
            throw new IllegalArgumentException();

        if (bindingSets == null)
            throw new IllegalArgumentException();

        this.serviceNode = serviceNode;
        
        this.bindingSets = bindingSets;
        
        this.exprImage = serviceNode.getExprImage();
        
        this.prefixDecls = serviceNode.getPrefixDecls();
        
        this.projectedVars = serviceNode.getProjectedVars();

        if (exprImage == null)
            throw new IllegalArgumentException();

        if (projectedVars == null)
            throw new IllegalArgumentException();

        if (prefixDecls != null) {

            /*
             * Build up a reverse map from namespace to prefix.
             */
            
            namespaces = new HashMap<String, String>();

            for (Map.Entry<String, String> e : prefixDecls.entrySet()) {
   
                namespaces.put(e.getValue(), e.getKey());
                
            }
            
        } else {
            
            namespaces = null;
            
        }

    }

//    /**
//     * Return the distinct variables "projected" by the SERVICE group graph
//     * pattern. The order of this set is not important, but the variables must
//     * be distinct.
//     * 
//     * Note: This is not quite correct. It will find variables inside of
//     * subqueries which are not visible. We should compute the projected
//     * variables in the query plan generator and just attach them to the
//     * ServiceCall JOIN and then pass them into this class (yet another
//     * attribute for the service call and the {@link ServiceFactory}).
//     * <p>
//     * Develop a unit test for this. It would wind up flowing back undefined
//     * bindings for variable which were not in scope. That should be Ok so long
//     * as the correct projection of those solutions is performed by the
//     * {@link ServiceCallJoin}, but we need an AST evaluation test suite to
//     * verify that.
//     */
//    private Set<IVariable<?>> getProjectedVars() {
//
//        final LinkedHashSet<IVariable<?>> projectedVars = new LinkedHashSet<IVariable<?>>();
//        
//        final Iterator<IVariable<?>> itr = BOpUtility
//                .getSpannedVariables((BOp) groupNode);
//        
//        while (itr.hasNext()) {
//        
//            projectedVars.add(itr.next());
//            
//        }
//
//        return projectedVars;
//        
//    }

    /**
     * Return an ordered collection of the distinct variable names used in the
     * given caller's solution set.
     * 
     * @param bindingSets
     *            The solution set.
     * 
     * @return The distinct, ordered collection of variables used.
     */
    private LinkedHashSet<String> getDistinctVars(final BindingSet[] bindingSets) {

        final LinkedHashSet<String> vars = new LinkedHashSet<String>();

        for (BindingSet bindingSet : bindingSets) {

            for (Binding binding : bindingSet) {

                vars.add(binding.getName());

            }

        }

        return vars;

    }

    public String getSparqlQuery() {

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
            for (IVariable<?> v : projectedVars) {
                sb.append(" ?");
                sb.append(v.getName());
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
                        if (i > 1)
                            sb.append(" &&");
//                        sb.append(" ?" + names[0] + " = ?" + names[i]);
                        sb.append(" sameTerm( ?" + names[0] + ", ?" + names[i]
                                + ")");
                    }
                    sb.append(" ).\n");
                }
            }
            sb.append(tmp);
            sb.append("\n}\n");
        }

        /*
         * BINDINGS clause.
         * 
         * Note: The BINDINGS clause is used to vector the SERVICE request.
         * 
         * BINDINGS ?book ?title { (:book1 :title1) (:book2 UNDEF) }
         */
        if (!singleEmptyBindingSet) {

            // Variables in a known stable order.
            final LinkedHashSet<String> vars = getDistinctVars(bindingSets);

            sb.append("BINDINGS");

            // Variable declarations.
            {

                for (String v : vars) {
                    sb.append(" ?");
                    sb.append(v);
                }
            }

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
                            final String ext = toExternal(val);
                            sb.append(ext);
                        }
                    }
                    sb.append(" )");
                    sb.append("\n");
                }
                sb.append("}\n");
            }

        }

        return sb.toString();

    }
    
    /**
     * Return an external form for the {@link Value} suitable for direct
     * embedding into a SPARQL query.
     * 
     * @param val
     *            The value.
     * 
     * @return The external form.
     */
    private String toExternal(final Value val) {
        
        if (val instanceof URI) {

            return toExternal((URI) val);
        
        } else if (val instanceof Literal) {
        
            return toExternal((Literal)val);
            
        } else if (val instanceof BNode) {
            
            /*
             * Note: The SPARQL 1.1 GRAMMAR does not permit blank nodes in the
             * BINDINGS clause. Blank nodes are sent across as an unbound
             * variable (UNDEF). If there is more than one variable which takes
             * on the same blank node in a given solution, then there must be a
             * FILTER imposed when verifies that those variables have the same
             * bound value for *that* solution (the constraint can not apply
             * across solutions in which that correlation is not present).
             */
            return "UNDEF";
//            throw new UnsupportedOperationException(
//                    "Blank node not permitted in BINDINGS");
            
        } else {
            
            throw new AssertionError();
            
        }

    }
    
    private String toExternal(final URI uri) {

        if (prefixDecls != null) {

            final String prefix = namespaces.get(uri.getNamespace());

            if (prefix != null) {

                return prefix + ":" + uri.getLocalName();

            }

        }

        return "<" + uri.stringValue() + ">";

    }
    
    private String toExternal(final Literal lit) {

        final String label = lit.getLabel();
        
        final String languageCode = lit.getLanguage();
        
        final URI datatypeURI = lit.getDatatype();

        final String datatypeStr = datatypeURI == null ? null
                : toExternal(datatypeURI);

        final StringBuilder sb = new StringBuilder((label.length() + 2)
                + (languageCode != null ? (languageCode.length() + 1) : 0)
                + (datatypeURI != null ? datatypeStr.length() + 2 : 0));

        sb.append('"');
        sb.append(label);
        sb.append('"');

        if (languageCode != null) {
            sb.append('@');
            sb.append(languageCode);
        }

        if (datatypeURI != null) {
            sb.append("^^");
            sb.append(datatypeStr);
        }

        return sb.toString();

    }

    /**
     * {@inheritDoc}
     * <p>
     * This implementation returns it's argument.
     */
    @Override
    public BindingSet[] getSolutions(final BindingSet[] serviceSolutions) {

        return serviceSolutions;

    }

}
