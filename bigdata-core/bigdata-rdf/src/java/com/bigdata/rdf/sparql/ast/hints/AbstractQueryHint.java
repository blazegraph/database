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
 * Created on Nov 27, 2011
 */

package com.bigdata.rdf.sparql.ast.hints;

import java.util.Properties;

import com.bigdata.rdf.sparql.ast.ASTBase;
import com.bigdata.rdf.sparql.ast.eval.IEvaluationContext;
import com.bigdata.rdf.sparql.ast.optimizers.IASTOptimizer;

/**
 * Base class for query hints.
 */
public abstract class AbstractQueryHint<T> implements IQueryHint<T> {

    private final String name;

    private final T defaultValue;

    /**
     * 
     * @param name
     *            The name of the query hint (required).
     * @param defaultValue
     *            The default value (optional).
     */
    protected AbstractQueryHint(final String name, final T defaultValue) {

        if (name == null)
            throw new IllegalArgumentException();

//        if (defaultValue == null)
//            throw new IllegalArgumentException();

        this.name = name;

        this.defaultValue = defaultValue;

    }

    @Override
    final public String getName() {
        return name;
    }

    @Override
    final public T getDefault() {
        return defaultValue;
    }

    // @Override
    // public void attach(final AST2BOpContext ctx,
    // final QueryHintScope scope, final ASTBase op,
    // final T value) {
    //
    // op.setQueryHint(getName(), value.toString());
    //
    // }

    /**
     * Set the query hint.
     * <p>
     * Note: Query hints are {@link Properties} objects and their values are
     * {@link String}s. The <i>value</i> will be converted to a String.
     * <p>
     * Note: Unlike annotations, query hints are propagated en-mass from an AST
     * node to the generated pipeline operator.
     * 
     * @param name
     *            The name of the query hint.
     * @param value
     *            The value for the query hint.
     */
    protected final void _setQueryHint(final IEvaluationContext ctx,
            final QueryHintScope scope, final ASTBase op, final String name,
            final T value) {

        op.setQueryHint(name, value.toString());

    }

    /**
     * Set an annotation on the AST node.
     * <p>
     * Note: Annotations are attached directly to the AST node. They are
     * interpreted during query plan generation. Unlike the query hints, the
     * annotations are not automatically transferred to the generated pipeline
     * operators. Instead, they typically control the behavior of the
     * {@link IASTOptimizer}s.
     * 
     * @param op
     *            The AST node.
     * @param name
     *            The name of the annotation.
     * @param value
     *            The value of the annotation.
     */
    protected final void _setAnnotation(final IEvaluationContext ctx,
            final QueryHintScope scope, final ASTBase op, final String name,
            final T value) {

        op.setProperty(name, value);

    }

    /*
     * I have taken this out (it was never committed with the code enabled). The
     * AST2BOpContext.queryHints field is from the original query and gets
     * applied onto AST nodes and then copied over onto PipelineOps. It should
     * not be populated when a query hint is given with Scope:=Query. Instead,
     * the query hint should be applied to all AST nodes in the query (at least
     * those of the appropriate type).
     */
//    /**
//     * Conditionally set the property in the global scope.
//     * 
//     * @param context
//     *            The evaluation context.
//     * @param key
//     *            The property name.
//     * @param val
//     *            The property value.
//     * 
//     * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/791" >
//     *      Clean up query hints </a>
//     */
//    protected void conditionalSetGlobalProperty(final AST2BOpContext context,
//            final String key, final T val) {
//
//        if (context.queryHints != null) {
//
//            /*
//             * Note: conditional check avoids String conversion for every AST
//             * node that we visit for a globally scoped query hint.
//             */
//            if (context.queryHints.getProperty(key) == null) {
//
//                context.queryHints.setProperty(key, val.toString());
//
//            }
//
//        }
//
//    }

}
