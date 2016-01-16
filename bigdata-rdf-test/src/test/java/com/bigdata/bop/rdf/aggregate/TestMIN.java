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

import java.math.BigInteger;

import junit.framework.TestCase2;

import com.bigdata.bop.BOpContext;
import com.bigdata.bop.Constant;
import com.bigdata.bop.ContextBindingSet;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.TestMockUtility;
import com.bigdata.bop.Var;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.journal.ITx;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.internal.constraints.MathBOp;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.internal.impl.literal.XSDIntegerIV;
import com.bigdata.rdf.internal.impl.literal.XSDNumericIV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Unit tests for {@link MIN}.
 * 
 * @author thompsonbry
 */
public class TestMIN extends TestCase2 {

	public TestMIN() {
	}

	public TestMIN(String name) {
		super(name);
	}

    public void test_min() {
        
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
        
        final MIN op = new MIN(false/* distinct */, lprice);
        assertFalse(op.isDistinct());
        assertFalse(op.isWildcard());

        op.reset();
        for (IBindingSet bs : data) {
            op.get(bs);
        }
        assertEquals(new XSDNumericIV(5), op.done());

    }

    public void test_min_with_complex_inner_value_expression() {
        
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
   
           // MIN(lprice+1)
           final MIN op = new MIN(false/* distinct */, new MathBOp(lprice,
                   new Constant<IV>(new XSDNumericIV(1)), MathBOp.MathOp.PLUS, new GlobalAnnotations(lexiconNamespace, ITx.READ_COMMITTED)));
           assertFalse(op.isDistinct());
           assertFalse(op.isWildcard());
   
           op.reset();
           for (IBindingSet bs : data) {
               op.get(bs);
           }
           assertEquals(new XSDIntegerIV(BigInteger.valueOf(5 + 1)), op.done());
       } finally {
          kb.getIndexManager().destroy();
       }

    }

    public void test_min_with_null() {
        
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
        
        final MIN op = new MIN(false/* distinct */, lprice);
        assertFalse(op.isDistinct());
        assertFalse(op.isWildcard());

        op.reset();
        for (IBindingSet bs : data) {
            op.get(bs);
        }
        assertEquals(new XSDNumericIV(7), op.done());

    }

    /**
     * MIN is defined in terms of SPARQL <code>ORDER BY</code> rather than
     * <code>LT</code>.
     * 
     * @see <a href="http://www.w3.org/TR/rdf-sparql-query/#modOrderBy">SPARQL
     *      Query Language for RDF</a>
     * @see <a
     *      href="http://www.w3.org/TR/2013/REC-sparql11-query-20130321/#op_lt">&lt;</a>
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/736">
     *      MIN() malfunction </a>
     */
    public void test_min_uses_ORDER_BY_not_LT() {
        
        final BigdataValueFactory f = BigdataValueFactoryImpl.getInstance(getName());
        
        final IVariable<IV> org = Var.var("org");
        final IVariable<IV> auth = Var.var("auth");
        final IVariable<IV> book = Var.var("book");
        final IVariable<IV> lprice = Var.var("lprice");

        final IConstant<String> org1 = new Constant<String>("org1");
        final IConstant<String> org2 = new Constant<String>("org2");
        final IConstant<String> auth1 = new Constant<String>("auth1");
        final TermId tid1 = new TermId<BigdataValue>(VTE.LITERAL, 1);
        tid1.setValue(f.createLiteral("auth2"));
        final IConstant<IV> auth2 = new Constant<IV>(tid1);
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
         * org1  auth2  book3  auth2
         * org2  auth3  book4  7
         * </pre>
         */
        final IBindingSet data [] = new IBindingSet []
        {
            new ListBindingSet ( new IVariable<?> [] { org, auth, book, lprice }, new IConstant [] { org1, auth1, book1, price9 } )
          , new ListBindingSet ( new IVariable<?> [] { org, auth, book, lprice }, new IConstant [] { org1, auth1, book2, price5 } )
          , new ListBindingSet ( new IVariable<?> [] { org, auth, book, lprice }, new IConstant [] { org1, auth2, book3, auth2 } )
          , new ListBindingSet ( new IVariable<?> [] { org, auth, book, lprice }, new IConstant [] { org2, auth3, book4, price7 } )
        };

        /*
         * Note: We have to materialize the IVs since we will be comparing
         * against a materialized IV once we observe [auth2].
         */
        price9.get().setValue(f.createLiteral("9", XSD.INT));
        price5.get().setValue(f.createLiteral("5", XSD.INT));
        price7.get().setValue(f.createLiteral("7", XSD.INT));

        final MIN op = new MIN(false/* distinct */, lprice);
        assertFalse(op.isDistinct());
        assertFalse(op.isWildcard());

        op.reset();
        for (IBindingSet bs : data) {
            op.get(bs);
        }
        
        assertEquals(auth2.get(), op.done());

//        try {
//            op.reset();
//            for (IBindingSet bs : data) {
//                op.get(bs);
//            }
//            fail("Expecting: " + SparqlTypeErrorException.class);
//        } catch (RuntimeException ex) {
//            if (InnerCause.isInnerCause(ex, SparqlTypeErrorException.class)) {
//                if (log.isInfoEnabled()) {
//                    log.info("Ignoring expected exception: " + ex);
//                }
//            } else {
//                fail("Expecting: " + SparqlTypeErrorException.class, ex);
//            }
//        }
//
//        /*
//         * Now verify that the error is sticky.
//         */
//        try {
//            op.done();
//            fail("Expecting: " + SparqlTypeErrorException.class);
//        } catch (RuntimeException ex) {
//            if (InnerCause.isInnerCause(ex, SparqlTypeErrorException.class)) {
//                if (log.isInfoEnabled()) {
//                    log.info("Ignoring expected exception: " + ex);
//                }
//            } else {
//                fail("Expecting: " + SparqlTypeErrorException.class, ex);
//            }
//        }
//
//        /*
//         * Now verify that reset() clears the error.
//         */
//        op.reset();
//        op.done();

    }

}
