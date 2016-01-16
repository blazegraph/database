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
 * Created on Aug 25, 2011
 */

package com.bigdata.rdf.sparql.ast;

import java.util.Map;
import java.util.Properties;

import com.bigdata.bop.BOp;
import com.bigdata.bop.ModifiableBOpBase;
import com.bigdata.bop.PipelineOp;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpUtility;
import com.bigdata.rdf.sparql.ast.explainhints.ExplainHints;
import com.bigdata.rdf.sparql.ast.explainhints.IExplainHint;
import com.bigdata.rdf.sparql.ast.optimizers.ASTQueryHintOptimizer;
import com.bigdata.rdf.sparql.ast.optimizers.IASTOptimizer;

/**
 * Base class for the AST.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ASTBase extends ModifiableBOpBase {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends ModifiableBOpBase.Annotations {

        /**
         * An optional {@link Properties} object specifying query hints which
         * apply to <i>this</i> AST node.
         * <p>
         * The relationship between SPARQL level query hints, the annotations on
         * the {@link ASTBase} and {@link PipelineOp} nodes is a bit complex,
         * and this {@link Properties} object is a bit complex. Briefly, this
         * {@link Properties} object contains property values which will be
         * applied (copied) onto generated {@link PipelineOp}s while annotations
         * on the {@link ASTBase} nodes are interpreted by {@link IASTOptimizer}
         * s and {@link AST2BOpUtility}. SPARQL level query hints map into one
         * or another of these things, or may directly set attributes on the
         * {@link AST2BOpContext}.
         * 
         * @see ASTQueryHintOptimizer
         * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/791" >
         *      Clean up query hints </a>
         */
        String QUERY_HINTS = "queryHints";
        
        /**
         * An optional {@link ExplainHints} object specifying hints to be
         * exposed to the user when requesting an explanation of the query.
         */
        String EXPLAIN_HINTS = "explainHints";
    }

    /**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
     */
    public ASTBase(final ASTBase op) {
        super(op);
    }

    /**
     * Required shallow copy constructor.
     */
    public ASTBase(final BOp[] args, final Map<String, Object> annotations) {

        super(args, annotations);
        
    }

    /**
     * Return the query hints for this AST node.
     * 
     * @return The query hints -or- <code>null</code> if none have been
     *         declared.
     */
    public Properties getQueryHints() {

        return (Properties) getProperty(Annotations.QUERY_HINTS);

    }

    /**
     * Return the explain hints associated with this AST node
     * 
     * @return The explain hints -or- <code>null</code> if none have been
     *         declared.
     */
    public ExplainHints getExplainHints() {
        return (ExplainHints) getProperty(Annotations.EXPLAIN_HINTS);
    }
    
    /**
     * Return the value of the query hint.
     * 
     * @param name
     *            The name of the query hint.
     *            
     * @return The value of the query hint and <code>null</code> if the query
     *         hint is not defined.
     */
    public String getQueryHint(final String name) {

        return getQueryHint(name, null/*defaultValue*/);

    }
    
    /**
     * Return the value of the query hint.
     * 
     * @param name
     *            The name of the query hint.
     * @param defaultValue
     *            The default value to use if the query hint is not defined.
     * 
     * @return The value of the query hint and <i>defaultValue</i> if the query
     *         hint is not defined.
     */
    public String getQueryHint(final String name, final String defaultValue) {

        final Properties queryHints = (Properties) getProperty(Annotations.QUERY_HINTS);

        if (queryHints == null)
            return defaultValue;

        return queryHints.getProperty(name, defaultValue);

    }

    /**
     * Return the boolean value of the query hint.
     * 
     * @param name
     *            The name of the query hint.
     * @param defaultValue
     *            The default value to use if the query hint is not defined.
     */
    public boolean getQueryHintAsBoolean(final String name,
            final String defaultValue) {

        return Boolean.valueOf(getQueryHint(name, defaultValue));

    }
    
    /**
     * Return the boolean value of the query hint.
     * 
     * @param name
     *            The name of the query hint.
     * @param defaultValue
     *            The default value to use if the query hint is not defined.
     */
    public boolean getQueryHintAsBoolean(final String name,
            final boolean defaultValue) {

        return Boolean.valueOf(getQueryHint(name,
                Boolean.toString(defaultValue)));

    }

    /**
     * Return the Integer value of the query hint.
     * 
     * @param name
     *            The name of the query hint.
     * @param defaultValue
     *            The default value to use if the query hint is not defined.
     */
    public Integer getQueryHintAsInteger(final String name,
            final Integer defaultValue) {

        return Integer.valueOf(getQueryHint(name, 
              Integer.toString(defaultValue)));

    }

    

    /**
     * Set the query hints.
     * 
     * @param queryHints
     *            The query hints (may be <code>null</code>).
     * 
     * @see QueryHints
     */
    public void setQueryHints(final Properties queryHints) {

        setProperty(Annotations.QUERY_HINTS, queryHints);
        
    }

    /**
     * Add an explain hint to the query.
     * 
     * @param explainHint the hint to add, ignored if null
     */
    public void addExplainHint(IExplainHint explainHint) {
       
       if (explainHint==null) {
          return;
       }
       
       // add the hint, creating a new object if required
       final ExplainHints explainHints = getExplainHints();
       if (explainHints==null) {
          setProperty(Annotations.EXPLAIN_HINTS, new ExplainHints(explainHint));
       } else {
          explainHints.addExplainHint(explainHint);
       }
    }
    
    /**
     * Set a query hint.
     * 
     * @param name
     *            The property name for the query hint.
     * @param value
     *            The property value for the query hint.
     */
    public void setQueryHint(final String name, final String value) {

        if (name == null)
            throw new IllegalArgumentException();

        if (value == null)
            throw new IllegalArgumentException();

        Properties queryHints = getQueryHints();

        if (queryHints == null) {

            // Lazily allocate the map.
            setProperty(Annotations.QUERY_HINTS, queryHints = new Properties());

        }

        queryHints.setProperty(name, value);

    }
    
    /**
     * Replace all occurrences of the old value with the new value in both the
     * arguments and annotations of this operator (recursive). A match is
     * identified by reference {@link #equals(Object)}. 
     * 
     * @param oldVal
     *            The old value.
     * @param newVal
     *            The new value.
     * 
     * @return The #of changes made.
     */
    public int replaceAllWith(final BOp oldVal, final BOp newVal) {

        int n = 0;

        final int arity = arity();

        for (int i = 0; i < arity; i++) {

            final BOp child = get(i);

            if (child != null && child.equals(oldVal)) {

                setArg(i, newVal);

                n++;

            }

            if (child instanceof ASTBase) {

                n += ((ASTBase) child).replaceAllWith(oldVal, newVal);

            }

        }

        for (Map.Entry<String, Object> e : annotations().entrySet()) {

            if (e.getValue() instanceof ASTBase) {

                n += ((ASTBase) e.getValue()).replaceAllWith(oldVal, newVal);

            }
            
        }
        
        return n;

    }

}
