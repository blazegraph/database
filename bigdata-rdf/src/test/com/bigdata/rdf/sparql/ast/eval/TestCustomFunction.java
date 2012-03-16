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
 * Created on Mar 16, 2012
 */

package com.bigdata.rdf.sparql.ast.eval;

import java.util.Map;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.rdf.internal.ILexiconConfiguration;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.constraints.AbstractLiteralBOp;
import com.bigdata.rdf.sparql.ast.FunctionRegistry;
import com.bigdata.rdf.sparql.ast.ValueExpressionNode;
import com.bigdata.rdf.sparql.ast.eval.AbstractDataDrivenSPARQLTestCase.TestHelper;

/**
 * Test suite for registering and evaluating custom functions.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
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
     * Unit test for access to the {@link ILexiconConfiguration} from a
     * "function bop".
     * 
     * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/513">
     *      Expose the LexiconConfiguration to Function BOPs </a>
     */
    public void test_ticket_513() throws Exception {

        final URI myFunctionUri = new URIImpl(
                "http://www.bigdata.com/myFunction");
        
        final FunctionRegistry.Factory myFactory = new FunctionRegistry.Factory() {

            @Override
            public IValueExpression<? extends IV> create(String lex,
                    Map<String, Object> scalarValues,
                    ValueExpressionNode... args) {
                
                FunctionRegistry.checkArgs(args, ValueExpressionNode.class);

              final IValueExpression<? extends IV> ve = AST2BOpUtility.toVE(lex, args[0]);
              
              return new MyFunctionBOp(ve, lex);

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
     * Simple function concatenates its argument with itself.
     */
    private static class MyFunctionBOp extends AbstractLiteralBOp<IV> {

        /**
         * 
         */
        private static final long serialVersionUID = 1L;

        /**
         * Required deep copy constructor.
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
        public MyFunctionBOp(IValueExpression<? extends IV> x, String lex) {
            super(x, lex);
        }

        @Override
        protected IV _get(final IBindingSet bset) {

            // Evaluate a function argument.
            final IV arg = getAndCheck(0, bset);
            
            // Convert into an RDF Value.
            final Literal lit = literalValue(arg);

            // Concat with self.
            final Literal lit2 = new LiteralImpl(lit.getLabel() + "-"
                    + lit.getLabel());
            
            // Convert into an IV.
            final IV ret = asValue(lit2, bset);
            
            // Return the function result.
            return ret;

        }

    };

}
