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
package com.bigdata.bop.rdf.aggregate;

import java.util.LinkedHashSet;
import java.util.Set;

import junit.framework.TestCase2;

import com.bigdata.bop.BOpContext;
import com.bigdata.bop.Constant;
import com.bigdata.bop.ContextBindingSet;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.TestMockUtility;
import com.bigdata.bop.Var;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.journal.ITx;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.constraints.MathBOp;
import com.bigdata.rdf.internal.impl.literal.XSDNumericIV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Unit tests for {@link SAMPLE}.
 * 
 * @author thompsonbry
 */
public class TestSAMPLE extends TestCase2 {

	public TestSAMPLE() {
	}

	public TestSAMPLE(String name) {
		super(name);
	}

    public void test_sample() {
        
        final IVariable<IV> org = Var.var("org");
        final IVariable<IV> auth = Var.var("auth");
        final IVariable<IV> book = Var.var("book");
        final IVariable<IV> lprice = Var.var("lprice");

        final IConstant<String> org1 = new Constant<String>("org1");
        final IConstant<String> org2 = new Constant<String>("org2");
        final IConstant<String> auth1 = new Constant<String>("auth1");
        final IConstant<String> auth2 = new Constant<String>("auth2");
        final IConstant<String> auth3 = new Constant<String>("auth3");
        final IConstant<String> book1 = new Constant<String>("book1");
        final IConstant<String> book2 = new Constant<String>("book2");
        final IConstant<String> book3 = new Constant<String>("book3");
        final IConstant<String> book4 = new Constant<String>("book4");
        final IConstant<XSDNumericIV<BigdataLiteral>> price5 = new Constant<XSDNumericIV<BigdataLiteral>>(
                new XSDNumericIV<BigdataLiteral>(5));
        final IConstant<XSDNumericIV<BigdataLiteral>> price7 = new Constant<XSDNumericIV<BigdataLiteral>>(
                new XSDNumericIV<BigdataLiteral>(7));
        final IConstant<XSDNumericIV<BigdataLiteral>> price9 = new Constant<XSDNumericIV<BigdataLiteral>>(
                new XSDNumericIV<BigdataLiteral>(9));

        /**
         * The test data:
         * 
         * <pre>
         * ?org  ?auth  ?book  ?lprice
         * org1  auth1  book1  9
         * org1  auth1  book3  5
         * org1  auth2  book3  7
         * org2  auth3  book4  7
         * </pre>
         */
        final IBindingSet data [] = new IBindingSet []
        {
            new ListBindingSet ( new IVariable<?> [] { org, auth, book, lprice }, new IConstant [] { org1, auth1, book1, price9 } )
          , new ListBindingSet ( new IVariable<?> [] { org, auth, book, lprice }, new IConstant [] { org1, auth1, book2, price5 } )
          , new ListBindingSet ( new IVariable<?> [] { org, auth, book, lprice }, new IConstant [] { org1, auth2, book3, price7 } )
          , new ListBindingSet ( new IVariable<?> [] { org, auth, book, lprice }, new IConstant [] { org2, auth3, book4, price7 } )
        };
        
        final IValueExpression<IV> expr = lprice;
        final SAMPLE op = new SAMPLE(false/* distinct */, expr);
        assertFalse(op.isDistinct());
        assertFalse(op.isWildcard());

        final Set<IV<?,?>> values = new LinkedHashSet<IV<?,?>>();
        for (IBindingSet bs : data) {
            values.add(expr.get(bs));
        }
        
        op.reset();
        for (IBindingSet bs : data) {
            op.get(bs);
        }
        
        assertTrue(values.contains(op.done()));

    }

    public void test_sample_with_complex_inner_value_expression() {
        
       AbstractTripleStore kb = TestMockUtility.mockTripleStore(getName());
       try {
           final BOpContext<IBindingSet> context = TestMockUtility.mockContext(kb);
           final String lexiconNamespace = kb.getLexiconRelation().getNamespace();
           
           final IVariable<IV> org = Var.var("org");
           final IVariable<IV> auth = Var.var("auth");
           final IVariable<IV> book = Var.var("book");
           final IVariable<IV> lprice = Var.var("lprice");
   
           final IConstant<String> org1 = new Constant<String>("org1");
           final IConstant<String> org2 = new Constant<String>("org2");
           final IConstant<String> auth1 = new Constant<String>("auth1");
           final IConstant<String> auth2 = new Constant<String>("auth2");
           final IConstant<String> auth3 = new Constant<String>("auth3");
           final IConstant<String> book1 = new Constant<String>("book1");
           final IConstant<String> book2 = new Constant<String>("book2");
           final IConstant<String> book3 = new Constant<String>("book3");
           final IConstant<String> book4 = new Constant<String>("book4");
           final IConstant<XSDNumericIV<BigdataLiteral>> price5 = new Constant<XSDNumericIV<BigdataLiteral>>(
                   new XSDNumericIV<BigdataLiteral>(5));
           final IConstant<XSDNumericIV<BigdataLiteral>> price7 = new Constant<XSDNumericIV<BigdataLiteral>>(
                   new XSDNumericIV<BigdataLiteral>(7));
           final IConstant<XSDNumericIV<BigdataLiteral>> price9 = new Constant<XSDNumericIV<BigdataLiteral>>(
                   new XSDNumericIV<BigdataLiteral>(9));
   
           /**
            * The test data:
            * 
            * <pre>
            * ?org  ?auth  ?book  ?lprice
            * org1  auth1  book1  9
            * org1  auth1  book3  5
            * org1  auth2  book3  7
            * org2  auth3  book4  7
            * </pre>
            */
           final IBindingSet data [] = new IBindingSet []
           {
               new ContextBindingSet(context, new ListBindingSet ( new IVariable<?> [] { org, auth, book, lprice }, new IConstant [] { org1, auth1, book1, price9 } ))
             , new ContextBindingSet(context, new ListBindingSet ( new IVariable<?> [] { org, auth, book, lprice }, new IConstant [] { org1, auth1, book2, price5 } ))
             , new ContextBindingSet(context, new ListBindingSet ( new IVariable<?> [] { org, auth, book, lprice }, new IConstant [] { org1, auth2, book3, price7 } ))
             , new ContextBindingSet(context, new ListBindingSet ( new IVariable<?> [] { org, auth, book, lprice }, new IConstant [] { org2, auth3, book4, price7 } ))
           };
   
           final IValueExpression<IV> expr = new MathBOp(lprice, new Constant<IV>(
                   new XSDNumericIV(1)), MathBOp.MathOp.PLUS, new GlobalAnnotations(lexiconNamespace, ITx.READ_COMMITTED));
   
           // SAMPLE(lprice+1)
           final SAMPLE op = new SAMPLE(false/* distinct */, expr);
           assertFalse(op.isDistinct());
           assertFalse(op.isWildcard());
   
           final Set<IV<?, ?>> values = new LinkedHashSet<IV<?, ?>>();
           for (IBindingSet bs : data) {
               values.add(expr.get(bs));
           }
   
           op.reset();
           for (IBindingSet bs : data) {
               op.get(bs);
           }
   
           assertTrue(values.contains(op.done()));
       } finally {
          kb.getIndexManager().destroy();
       }

    }

    public void test_sample_with_null() {
        
        final IVariable<IV> org = Var.var("org");
        final IVariable<IV> auth = Var.var("auth");
        final IVariable<IV> book = Var.var("book");
        final IVariable<IV> lprice = Var.var("lprice");

        final IConstant<String> org1 = new Constant<String>("org1");
        final IConstant<String> org2 = new Constant<String>("org2");
        final IConstant<String> auth1 = new Constant<String>("auth1");
        final IConstant<String> auth2 = new Constant<String>("auth2");
        final IConstant<String> auth3 = new Constant<String>("auth3");
        final IConstant<String> book1 = new Constant<String>("book1");
        final IConstant<String> book2 = new Constant<String>("book2");
        final IConstant<String> book3 = new Constant<String>("book3");
        final IConstant<String> book4 = new Constant<String>("book4");
        final IConstant<XSDNumericIV<BigdataLiteral>> price5 = new Constant<XSDNumericIV<BigdataLiteral>>(
                new XSDNumericIV<BigdataLiteral>(5));
        final IConstant<XSDNumericIV<BigdataLiteral>> price7 = new Constant<XSDNumericIV<BigdataLiteral>>(
                new XSDNumericIV<BigdataLiteral>(7));
        final IConstant<XSDNumericIV<BigdataLiteral>> price9 = new Constant<XSDNumericIV<BigdataLiteral>>(
                new XSDNumericIV<BigdataLiteral>(9));

        /**
         * The test data:
         * 
         * <pre>
         * ?org  ?auth  ?book  ?lprice
         * org1  auth1  book1  9
         * org1  auth1  book3  NULL
         * org1  auth2  book3  7
         * org2  auth3  book4  7
         * </pre>
         */
        final IBindingSet data [] = new IBindingSet []
        {
            new ListBindingSet ( new IVariable<?> [] { org, auth, book, lprice }, new IConstant [] { org1, auth1, book1, price9 } )
          , new ListBindingSet ( new IVariable<?> [] { org, auth, book,        }, new IConstant [] { org1, auth1, book2,        } )
          , new ListBindingSet ( new IVariable<?> [] { org, auth, book, lprice }, new IConstant [] { org1, auth2, book3, price7 } )
          , new ListBindingSet ( new IVariable<?> [] { org, auth, book, lprice }, new IConstant [] { org2, auth3, book4, price7 } )
        };
        
        final IValueExpression<IV> expr = lprice;

        // SAMPLE(lprice)
        final SAMPLE op = new SAMPLE(false/* distinct */, expr);
        assertFalse(op.isDistinct());
        assertFalse(op.isWildcard());

        final Set<IV<?, ?>> values = new LinkedHashSet<IV<?, ?>>();
        for (IBindingSet bs : data) {
            final IV iv = expr.get(bs);
            if (iv != null)
                values.add(iv);
        }

        op.reset();
        for (IBindingSet bs : data) {
            op.get(bs);
        }

        assertTrue(values.contains(op.done()));

    }

    /**
     * Note: This will produce a NotMaterializedException unless we force the
     * materialization of all the XSDNumericIV values for lprice, but that is just
     * because MathBOp is persnickety. Since SAMPLE really accepts any non-null
     * value, I've left out this unit test.
     * 
     * FIXME We should test this because of the sticky error contract.
     */
//    public void test_sample_with_errors() {
//        
//        final BigdataValueFactory f = BigdataValueFactoryImpl.getInstance(getName());
//        
//        final IVariable<IV> org = Var.var("org");
//        final IVariable<IV> auth = Var.var("auth");
//        final IVariable<IV> book = Var.var("book");
//        final IVariable<IV> lprice = Var.var("lprice");
//
//        final IConstant<String> org1 = new Constant<String>("org1");
//        final IConstant<String> org2 = new Constant<String>("org2");
//        final IConstant<String> auth1 = new Constant<String>("auth1");
//        final TermId tid1 = new TermId<BigdataValue>(VTE.LITERAL, 1);
//        tid1.setValue(f.createLiteral("auth2"));
//        final IConstant<IV> auth2 = new Constant<IV>(tid1);
//        final IConstant<String> auth3 = new Constant<String>("auth3");
//        final IConstant<String> book1 = new Constant<String>("book1");
//        final IConstant<String> book2 = new Constant<String>("book2");
//        final IConstant<String> book3 = new Constant<String>("book3");
//        final IConstant<String> book4 = new Constant<String>("book4");
//        final IConstant<XSDNumericIV<BigdataLiteral>> price5 = new Constant<XSDNumericIV<BigdataLiteral>>(
//                new XSDNumericIV<BigdataLiteral>(5));
//        final IConstant<XSDNumericIV<BigdataLiteral>> price7 = new Constant<XSDNumericIV<BigdataLiteral>>(
//                new XSDNumericIV<BigdataLiteral>(7));
//        final IConstant<XSDNumericIV<BigdataLiteral>> price9 = new Constant<XSDNumericIV<BigdataLiteral>>(
//                new XSDNumericIV<BigdataLiteral>(9));
//
//        /**
//         * The test data:
//         * 
//         * <pre>
//         * ?org  ?auth  ?book  ?lprice
//         * org1  auth1  book1  9
//         * org1  auth1  book3  5
//         * org1  auth2  book3  7
//         * org2  auth3  book4  7
//         * </pre>
//         */
//        final IBindingSet data [] = new IBindingSet []
//        {
//            new ListBindingSet ( new IVariable<?> [] { org, auth, book, lprice }, new IConstant [] { org1, auth1, book1, auth2 } )
//          , new ListBindingSet ( new IVariable<?> [] { org, auth, book, lprice }, new IConstant [] { org1, auth1, book2, price5 } )
//          , new ListBindingSet ( new IVariable<?> [] { org, auth, book, lprice }, new IConstant [] { org1, auth2, book3, price7 } )
//          , new ListBindingSet ( new IVariable<?> [] { org, auth, book, lprice }, new IConstant [] { org2, auth3, book4, price7 } )
//        };
//
//        /*
//         * Note: We need to use an expression in this test which will cause a
//         * type error since SAMPLE does not have a problem with non-numeric
//         * data. Also, the order of the data has been changed in order to ensure
//         * that an error is provoked.
//         */
//        
//        final IValueExpression<IV> expr = new MathBOp(lprice, new Constant(new XSDNumericIV(1)),
//                MathBOp.MathOp.PLUS);
//
//        // SAMPLE(lprice+1)
//        final SAMPLE op = new SAMPLE(false/* distinct */, expr);
//        assertFalse(op.isDistinct());
//        assertFalse(op.isWildcard());
//
//        final Set<IV<?, ?>> values = new LinkedHashSet<IV<?, ?>>();
//        for (IBindingSet bs : data) {
//            final IV iv = expr.get(bs);
//            if (iv != null)
//                values.add(iv);
//        }
//
//        try {
//            op.reset();
//            for (IBindingSet bs : data) {
//                op.get(bs);
//            }
//            fail("Expecting: " + SparqlTypeErrorException.class);
//        } catch (SparqlTypeErrorException ex) {
//            if (log.isInfoEnabled()) {
//                log.info("Ignoring expected exception: " + ex);
//            }
//        }
//        
//    }

}
