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
 * Created on Mar 16, 2012
 */

package com.bigdata.rdf.sparql.ast.eval;

import java.util.Map;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContextBase;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.NV;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.ILexiconConfiguration;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.constraints.INeedsMaterialization;
import com.bigdata.rdf.internal.constraints.IVValueExpression;
import com.bigdata.rdf.internal.constraints.XSDBooleanIVValueExpression;
import com.bigdata.rdf.sparql.ast.FunctionRegistry;
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;
import com.bigdata.rdf.sparql.ast.ValueExpressionNode;

/**
 * Test suite for registering and evaluating custom functions.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/513"> Expose
 *      the LexiconConfiguration to Function BOPs </a>
 * @see https
 *      ://sourceforge.net/apps/mediawiki/bigdata/index.php?title=CustomFunction
 */
public class TestCustomFunction extends AbstractDataDrivenSPARQLTestCase {

    /**
     * 
     */
    public TestCustomFunction() {
    }

    /**
     * @param name
     */
    public TestCustomFunction(String name) {
        super(name);
    }
    
    /**
     * Unit test for a simple custom function extending
     * {@link AbstractLiteralBOp}, including access to the
     * {@link ILexiconConfiguration}.
     */
    public void test_custom_function_1() throws Exception {

        final URI myFunctionUri = new URIImpl(
                "http://www.bigdata.com/myFunction");
        
        final FunctionRegistry.Factory myFactory = new FunctionRegistry.Factory() {

            @Override
            public IValueExpression<? extends IV> create(
                    final BOpContextBase context,//
                    final GlobalAnnotations globals,//
                    final Map<String, Object> scalarValues,//
                    final ValueExpressionNode... args) {

                FunctionRegistry.checkArgs(args, ValueExpressionNode.class);

                final IValueExpression<? extends IV> ve = AST2BOpUtility.toVE(
                        context, globals, args[0]);

                return new MyFunctionBOp(ve, globals);

            }

        };

        FunctionRegistry.add(myFunctionUri, myFactory);
        
        try {

            new TestHelper("custom-function-1").runTest();
            
        } finally {

            FunctionRegistry.remove(myFunctionUri);
            
        }

    }

    /**
     * Unit test for a simple custom function extending
     * {@link XSDBooleanIVValueExpression}, including access to the
     * {@link ILexiconConfiguration}.
     */
    public void test_custom_function_2() throws Exception {

        final URI myFunctionUri = new URIImpl(
                "http://www.bigdata.com/myFunction2");
        
        final FunctionRegistry.Factory myFactory = new MyFilterFactory();

        FunctionRegistry.add(myFunctionUri, myFactory);
        
        try {

            new TestHelper("custom-function-2").runTest();
            
        } finally {

            FunctionRegistry.remove(myFunctionUri);
            
        }
                
    }
    
    /**
     * Factory for {@link MyFunctionBOp}.
     */
    private static class MyFunctionFactory implements FunctionRegistry.Factory {

        @Override
        public IValueExpression<? extends IV> create(
                final BOpContextBase context,//
                final GlobalAnnotations globals,//
                final Map<String, Object> scalarValues,//
                final ValueExpressionNode... args) {

            FunctionRegistry.checkArgs(args, ValueExpressionNode.class);

            final IValueExpression<? extends IV> ve = AST2BOpUtility.toVE(
                    context, globals, args[0]);

            return new MyFunctionBOp(ve, globals);

        }

    }

    /**
     * This is a variant of {@link #test_custom_function_1()} where the function
     * is evaluated against a constant.
     */
    public void test_custom_function_3() throws Exception {

        final URI myFunctionUri = new URIImpl(
                "http://www.bigdata.com/myFunction");
        
        final FunctionRegistry.Factory myFactory = new MyFunctionFactory();

        FunctionRegistry.add(myFunctionUri, myFactory);
        
        try {

            new TestHelper("custom-function-3").runTest();
            
        } finally {

            FunctionRegistry.remove(myFunctionUri);
            
        }

    }

    /**
     * Simple function concatenates its argument with itself.
     */
    private static class MyFunctionBOp extends IVValueExpression<IV> implements INeedsMaterialization {

        /**
         * 
         */
        private static final long serialVersionUID = 1L;

        /**
         * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
         * 
         * @param op
         */
        public MyFunctionBOp(final MyFunctionBOp op) {
            super(op);
        }

        /**
         * Required shallow copy constructor.
         * @param args
         *            The function arguments.
         * @param anns
         *            The function annotations.
         */
        public MyFunctionBOp(final BOp[] args, final Map<String, Object> anns) {
            super(args, anns);
        }

        /**
         * @param x
         *            The function argument.
         * @param lex
         *            The namespace of the lexicon relation.
         */
        public MyFunctionBOp(IValueExpression<? extends IV> x, GlobalAnnotations globals) {
            super(x, globals);
        }

        @Override
        public IV get(final IBindingSet bset) {

            // Evaluate a function argument.
            final IV arg = getAndCheckLiteral(0, bset);
            
            // Convert into an RDF Value.
            final Literal lit = asLiteral(arg);

            // Concat with self.
            final Literal lit2 = new LiteralImpl(lit.getLabel() + "-"
                    + lit.getLabel());
            
            // Convert into an IV.
            final IV ret = asIV(lit2, bset);
            
            // Return the function result.
            return ret;

        }

		@Override
		public Requirement getRequirement() {
			return Requirement.SOMETIMES;
		}

    }

    /**
     * Factory for {@link MyFilterBOp}.
     */
    private static class MyFilterFactory implements FunctionRegistry.Factory {

        @Override
        public IValueExpression<? extends IV> create(
                final BOpContextBase context,//
                final GlobalAnnotations globals,//
                final Map<String, Object> scalarValues,//
                final ValueExpressionNode... args) {
            
            FunctionRegistry.checkArgs(args, ValueExpressionNode.class);

            final IValueExpression<? extends IV> ve = AST2BOpUtility.toVE(
                    context, globals, args[0]);

            return new MyFilterBOp(ve);

        }

    }

    /**
     * Simple boolean function returns <code>true</code> iff the argument is
     * <code>Mike</code>
     */
    private static class MyFilterBOp extends XSDBooleanIVValueExpression
            implements INeedsMaterialization
    {

        /**
         * 
         */
        private static final long serialVersionUID = 1L;

        /**
         * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
         * 
         * @param op
         */
        public MyFilterBOp(final MyFilterBOp op) {
            super(op);
        }

        /**
         * Required shallow copy constructor.
         * 
         * @param args
         *            The function arguments.
         * @param anns
         *            The function annotations.
         */
        public MyFilterBOp(final BOp[] args, final Map<String, Object> anns) {
            super(args, anns);
        }

        public MyFilterBOp(final IValueExpression<? extends IV> x) {

            this(new BOp[] { x }, BOp.NOANNS);

        }

        @Override
        protected boolean accept(final IBindingSet bset) {

            // Evaluate a value expression argument.
            final IV arg0 = get(0).get(bset);

            if (arg0 == null || !arg0.isLiteral()) {
                // Shortcut for "SOMETIMES" evaluation.
                throw new SparqlTypeErrorException();
            }
            
            // Convert into an RDF Value.
            final Literal lit = asLiteral(arg0);

            return lit.getLabel().equals("Mike");

        }

        @Override
        public Requirement getRequirement() {
            
            return Requirement.SOMETIMES;
            
        }

    }

}
