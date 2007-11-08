/*
 * Copyright SYSTAP, LLC 2006-2007.  All rights reserved.
 * 
 * Contact:
 *      SYSTAP, LLC
 *      4501 Tower Road
 *      Greensboro, NC 27410
 *      phone: +1 202 462 9888
 *      email: licenses@bigdata.com
 *
 *      http://www.systap.com/
 *      http://www.bigdata.com/
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package com.bigdata.rdf.sail;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.sesame.sail.RdfRepository;
import org.openrdf.sesame.sail.query.BooleanExpr;
import org.openrdf.sesame.sail.query.DirectSubClassOf;
import org.openrdf.sesame.sail.query.DirectSubPropertyOf;
import org.openrdf.sesame.sail.query.DirectType;
import org.openrdf.sesame.sail.query.GraphPattern;
import org.openrdf.sesame.sail.query.GraphPatternQuery;
import org.openrdf.sesame.sail.query.PathExpression;
import org.openrdf.sesame.sail.query.Query;
import org.openrdf.sesame.sail.query.QueryOptimizer;
import org.openrdf.sesame.sail.query.SetOperator;
import org.openrdf.sesame.sail.query.TriplePattern;
import org.openrdf.sesame.sail.query.ValueCompare;
import org.openrdf.sesame.sail.query.ValueExpr;
import org.openrdf.sesame.sail.query.Var;

/**
 * This file contains code based on the Sesame 1.2.3 {@link QueryOptimizer}.
 * The methods of that class have been made into instance methods in order to
 * allow access to the database so that we can compute the range counts for
 * triple patterns based on the data at query time and thus choose a join order
 * that is less expensive. While this improves high-level query performance it
 * is not possible to truely optimize JOINs for the Sesame 1.x framework.
 * <p>
 * Note: Since this file contains code from the Sesame 1.2.3 distribution it
 * falls under the LGPL license which was used by that distribution. This is the
 * ONLY file in this project that contains code from the Sesame 1.x
 * distribution.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractRdfRepository implements RdfRepository {

    abstract int getRangeCount(PathExpression pe,Hashtable<PathExpression,Integer> rangeCounts);
    
    abstract int rangeCount(TriplePattern tp);
    
    /**
     * An attempt to get the Sesame query optimizer to choose the join
     * order based on the actual selectivity of the triple patterns.
     * 
     * @param qc
     */
    void optimizeQuery2(Query qc) {
        if (qc instanceof GraphPatternQuery) {
            GraphPatternQuery gpQuery = (GraphPatternQuery)qc;
            _optimizeGraphPattern(gpQuery.getGraphPattern(), new HashSet());
        }
        else if (qc instanceof SetOperator) {
            SetOperator setOp = (SetOperator)qc;
            optimizeQuery( setOp.getLeftArg() );
            optimizeQuery( setOp.getRightArg() );
        }
    }

    private void _optimizeGraphPattern(GraphPattern graphPattern, Set boundVars) {
        // Optimize any optional child graph patterns:
        Iterator iter = graphPattern.getOptionals().iterator();

        if (iter.hasNext()) {
            // Build set of variables that are bound in this scope
            Set scopeVars = new HashSet(boundVars);
            graphPattern.getLocalVariables(scopeVars);

            // Optimize recursively
            while (iter.hasNext()) {
                GraphPattern optionalGP = (GraphPattern)iter.next();
                _optimizeGraphPattern(optionalGP, new HashSet(scopeVars));
            }
        }

        // Optimize the GraphPattern itself:
        _inlineVarAssignments(graphPattern);
        _orderExpressions(graphPattern, boundVars);
    }
    
    /**
     * Inlines as much of the "variable assignments" (comparison between a
     * variable and fixed value) that are found in the list of conjunctive
     * constraints as possible, and removes them from the query. Only variable
     * assignments for variables that are used in <tt>graphPattern</tt> itself
     * are processed. Inlining variable assignments for variables that are
     * (only) used in optional child graph patterns leads to incorrect query
     * evaluation.
     **/
    private void _inlineVarAssignments(GraphPattern graphPattern) {
        Set localVars = new HashSet();
        graphPattern.getLocalVariables(localVars);

        boolean constraintsModified = false;

        List conjunctiveConstraints =
                new ArrayList(graphPattern.getConjunctiveConstraints());

        Iterator iter = conjunctiveConstraints.iterator();

        while (iter.hasNext()) {
            BooleanExpr boolExpr = (BooleanExpr)iter.next();

            if (boolExpr instanceof ValueCompare) {
                ValueCompare valueCompare = (ValueCompare)boolExpr;

                if (valueCompare.getOperator() != ValueCompare.EQ) {
                    continue;
                }

                ValueExpr arg1 = valueCompare.getLeftArg();
                ValueExpr arg2 = valueCompare.getRightArg();

                Var varArg = null;
                Value value = null;

                if (arg1 instanceof Var && arg1.getValue() == null && // arg1 is an unassigned var 
                    arg2.getValue() != null) // arg2 has a value
                {
                    varArg = (Var)arg1;
                    value = arg2.getValue();
                }
                else if (arg2 instanceof Var && arg2.getValue() == null && // arg2 is an unassigned var
                    arg1.getValue() != null) // arg1 has a value
                {
                    varArg = (Var)arg2;
                    value = arg1.getValue();
                }

                if (varArg != null && localVars.contains(varArg)) {
                    // Inline this variable assignment
                    varArg.setValue(value);

                    // Remove the (now redundant) constraint
                    iter.remove();

                    constraintsModified = true;
                }
            }
        }

        if (constraintsModified) {
            graphPattern.setConstraints(conjunctiveConstraints);
        }
    }
    
    /**
     * Merges the boolean constraints and the path expressions in one single
     * list. The order of the path expressions is not changed, but the boolean
     * constraints are inserted between them. The separate boolean constraints
     * are moved to the start of the list as much as possible, under the
     * condition that all variables that are used in the constraint are
     * instantiated by the path expressions that are earlier in the list. An
     * example combined list might be:
     * <tt>[(A,B,C), A != foo:bar, (B,E,F), C != F, (F,G,H)]</tt>.
     **/
    private void _orderExpressions(GraphPattern graphPattern, Set boundVars) {
        List expressions = new ArrayList();
        List conjunctiveConstraints = new LinkedList(graphPattern.getConjunctiveConstraints());

        // First evaluate any constraints that don't depend on any variables:
        _addVerifiableConstraints(conjunctiveConstraints, boundVars, expressions);

        // Then evaluate all path expressions from graphPattern
        List pathExpressions = new LinkedList(graphPattern.getPathExpressions());
        Hashtable<PathExpression,Integer> rangeCounts = new Hashtable<PathExpression, Integer>();
        while (!pathExpressions.isEmpty()) {
            PathExpression pe = _getMostSpecificPathExpression(pathExpressions, boundVars, rangeCounts);

            pathExpressions.remove(pe);
            expressions.add(pe);

            pe.getVariables(boundVars);

            _addVerifiableConstraints(conjunctiveConstraints, boundVars, expressions);
        }

        // Finally, evaluate any optional child graph pattern lists
        List optionals = new LinkedList(graphPattern.getOptionals());
        while (!optionals.isEmpty()) {
            PathExpression pe = _getMostSpecificPathExpression(optionals, boundVars, rangeCounts);

            optionals.remove(pe);
            expressions.add(pe);

            pe.getVariables(boundVars);

            _addVerifiableConstraints(conjunctiveConstraints, boundVars, expressions);
        }

        // All constraints should be verifiable when all path expressions are
        // evaluated, but add any remaining constraints anyway
        expressions.addAll(conjunctiveConstraints);

        graphPattern.setExpressions(expressions);
    }

    /**
     * Gets the most specific path expression from <tt>pathExpressions</tt>
     * given that the variables in <tt>boundVars</tt> have already been assigned
     * values. The most specific path expressions is the path expression with
     * the least number of unbound variables.
     **/
    private PathExpression _getMostSpecificPathExpression(
            List pathExpressions, Set boundVars, Hashtable<PathExpression,Integer> rangeCounts)
    {
        int minVars = Integer.MAX_VALUE;
        int minRangeCount = Integer.MAX_VALUE;
        PathExpression result = null;
        ArrayList vars = new ArrayList();

        for (int i = 0; i < pathExpressions.size(); i++) {
            PathExpression pe = (PathExpression)pathExpressions.get(i);

            /*
             * The #of results for this PathException or -1 if not a
             * TriplePattern.
             * 
             * @todo if zero (0), then at least order it first.
             */
            int rangeCount = getRangeCount(pe,rangeCounts);
            
            // Get the variables that are used in this path expression
            vars.clear();
            pe.getVariables(vars);

            // Count unbound variables
            int varCount = 0;
            for (int j = 0; j < vars.size(); j++) {
                Var var = (Var)vars.get(j);

                if (!var.hasValue() && !boundVars.contains(var)) {
                    varCount++;
                }
            }

            // A bit of hack to make sure directType-, directSubClassOf- and
            // directSubPropertyOf patterns get sorted to the back because these
            // are potentially more expensive to evaluate.
            if (pe instanceof DirectType ||
                pe instanceof DirectSubClassOf ||
                pe instanceof DirectSubPropertyOf)
            {
                varCount++;
            }

            if (rangeCount != -1) {
                // rangeCount is known for this path expression.
                if (rangeCount < minRangeCount) {
                    // More specific path expression found
                    minRangeCount = rangeCount;
                    result = pe;
                }
            } else {
                // rangeCount is NOT known.
                if (varCount < minVars) {
                    // More specific path expression found
                    minVars = varCount;
                    result = pe;
                }
            }
        }

        return result;
    }

    /**
     * Adds all verifiable constraints (constraint for which every variable has
     * been bound to a specific value) from <tt>conjunctiveConstraints</tt> to
     * <tt>expressions</tt>.
     *
     * @param conjunctiveConstraints A List of BooleanExpr objects.
     * @param boundVars A Set of Var objects that have been bound.
     * @param expressions The list to add the verifiable constraints to.
     **/
    private void _addVerifiableConstraints(
        List conjunctiveConstraints, Set boundVars, List expressions)
    {
        Iterator iter = conjunctiveConstraints.iterator();

        while (iter.hasNext()) {
            BooleanExpr constraint = (BooleanExpr)iter.next();

            Set constraintVars = new HashSet();
            constraint.getVariables(constraintVars);

            if (boundVars.containsAll(constraintVars)) {
                // constraint can be verified
                expressions.add(constraint);
                iter.remove();
            }
        }
    }

    /**
     * Replace all {@link Value} objects stored in variables with the
     * corresponding objects using our {@link ValueFactory}.
     * <p>
     * Note: This can cause unknown terms to be inserted into store.
     */
    void replaceValuesInQuery(Query query) {

        ValueFactory valueFactory = getValueFactory();
        
        final List varList = new ArrayList();

        query.getVariables(varList);

        for (int i = 0; i < varList.size(); i++) {

            final Var var = (Var) varList.get(i);

            if (var.hasValue()) {

                final Value value = var.getValue();

                if (value instanceof URI) {

                    // URI substitution.

                    String uriString = ((URI) value).getURI();

                    var.setValue(valueFactory.createURI(uriString));

                } else if (value instanceof BNode) {

                    // BNode substitution.

                    final String id = ((BNode) value).getID();

                    if (id == null) {

                        var.setValue(valueFactory.createBNode());

                    } else {

                        var.setValue(valueFactory.createBNode(id));

                    }

                } else if (value instanceof Literal) {

                    // Literal substitution.

                    Literal lit = (Literal) value;

                    final String lexicalForm = lit.getLabel();

                    final String language = lit.getLanguage();

                    if (language != null) {

                        lit = valueFactory.createLiteral(lexicalForm, language);

                    } else {

                        URI datatype = lit.getDatatype();

                        if (datatype != null) {

                            lit = valueFactory.createLiteral(lexicalForm,
                                    datatype);

                        } else {

                            lit = valueFactory.createLiteral(lexicalForm);

                        }

                    }

                    var.setValue(lit);

                } // if( literal )

            } // if( hasValue )

        } // next variable.

    }

}
