/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Aug 19, 2010
 */

package com.bigdata.bop.solutions;

import java.math.BigInteger;
import java.util.concurrent.FutureTask;

import junit.framework.TestCase2;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.Bind;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.Var;
import com.bigdata.bop.bindingSet.ArrayBindingSet;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.bop.engine.BlockingBufferWithStats;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.MockRunningQuery;
import com.bigdata.bop.engine.TestQueryEngine;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.XSDIntIV;
import com.bigdata.rdf.internal.XSDIntegerIV;
import com.bigdata.rdf.internal.constraints.MathBOp;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.ThickAsynchronousIterator;

/**
 * Unit tests for the {@link ProjectionOp}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
@Deprecated // Probably does not make sense with the materialization pipeline.
public class TestProjectionOp extends TestCase2 {

    /**
     * 
     */
    public TestProjectionOp() {
    }

    /**
     * @param name
     */
    public TestProjectionOp(String name) {
        super(name);
    }

//    private long termId = 1;
//    
//    private IV<BigdataLiteral, ?> makeIV(final BigdataLiteral lit) {
//
//        final IV<BigdataLiteral, ?> iv = new TermId<BigdataLiteral>(
//                VTE.LITERAL, termId++);
//
//        iv.setValue(lit);
//
//        return iv;
//
//    }

    /**
     * Test with computed value expressions.
     * <p>
     * Note: Since there are some unbound values for the base variables,
     * solutions in which those variables are not bound will cause type errors
     * which should cause those solutions to be dropped.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testComputedValueExpressions() 
    {

        final IVariable<IV> x = Var.var("x");
        final IVariable<IV> y = Var.var("y");
        final IVariable<IV> z = Var.var("z");
        final IConstant<IV> _1 = new Constant<IV>(new XSDIntIV(1));
        final IConstant<IV> _2 = new Constant<IV>(new XSDIntIV(2));
        final IConstant<IV> _3 = new Constant<IV>(new XSDIntIV(3));
        final IConstant<IV> _4 = new Constant<IV>(new XSDIntIV(4));
        final IConstant<IV> _5 = new Constant<IV>(new XSDIntIV(5));
        
        // The computed values are all xsd:integer.
        final IConstant<IV> __2 = new Constant<IV>(new XSDIntegerIV(BigInteger.valueOf(2)));
        final IConstant<IV> __4 = new Constant<IV>(new XSDIntegerIV(BigInteger.valueOf(4)));
        final IConstant<IV> __5 = new Constant<IV>(new XSDIntegerIV(BigInteger.valueOf(5)));
        final IConstant<IV> __6 = new Constant<IV>(new XSDIntegerIV(BigInteger.valueOf(6)));

        final IValueExpression<?> exprs[] = new IValueExpression[] { //
                new Bind(z, new MathBOp(x, y, MathBOp.MathOp.PLUS)),//
                y //
        };

        final int bopId = 1;
        
        final PipelineOp query = new ProjectionOp(new BOp[] {},
                NV.asMap(new NV[] {//
                        new NV(ProjectionOp.Annotations.BOP_ID, bopId),
                        new NV(ProjectionOp.Annotations.PROJECT, exprs),//
                }));

        //
        // the test data
        //
        final IBindingSet data [] = new IBindingSet []
        {
              new ListBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { _1, _1 } ) // x+y=2
            , new ListBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { _1, _5 } ) // x+y=6
            , new ListBindingSet ( new IVariable<?> [] { x },    new IConstant [] { _3 }    )  // x+y=N/A
            , new ListBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { _4, _1 } ) // x+y=5
            , new ListBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { _4, _2 } ) // x+y=6
            , new ListBindingSet ( new IVariable<?> [] {},       new IConstant [] {}       )   // x+y=N/A
            , new ListBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { _1, _3 } ) // x+y=4
            , new ListBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { _2, _4 } ) // x+y=6
            , new ListBindingSet ( new IVariable<?> [] { y },    new IConstant [] { _1 }    )  // x+y=NA
            , new ListBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { _2, _2 } ) // x+y=4
        } ;

        //
        // the expected solutions
        //
        final IBindingSet expected [] = new IBindingSet []
        {
              new ArrayBindingSet ( new IVariable<?> [] { z, y },   new IConstant [] { __2, _1 } )
            , new ArrayBindingSet ( new IVariable<?> [] { z, y },   new IConstant [] { __6, _5 } )
            , new ArrayBindingSet ( new IVariable<?> [] { z, y },   new IConstant [] { __5, _1 } )
            , new ArrayBindingSet ( new IVariable<?> [] { z, y },   new IConstant [] { __6, _2 } )
            , new ArrayBindingSet ( new IVariable<?> [] { z, y },   new IConstant [] { __4, _3 } )
            , new ArrayBindingSet ( new IVariable<?> [] { z, y },   new IConstant [] { __6, _4 } )
            , new ArrayBindingSet ( new IVariable<?> [] { z, y },   new IConstant [] { __4, _2 } )
        } ;

        final BOpStats stats = query.newStats () ;

        final IAsynchronousIterator<IBindingSet[]> source = new ThickAsynchronousIterator<IBindingSet[]>(
                new IBindingSet[][] { data });

        final IBlockingBuffer<IBindingSet[]> sink = new BlockingBufferWithStats<IBindingSet[]>(
                query, stats);

        final IRunningQuery runningQuery = new MockRunningQuery(null/* fed */
        , null/* indexManager */
        );
        
        final BOpContext<IBindingSet> context = new BOpContext<IBindingSet>(
                runningQuery, -1/* partitionId */
                , stats, source, sink, null/* sink2 */
        );

        context.setLastInvocation();

        final FutureTask<Void> ft = query.eval(context);
        // Run the query.
        {
            final Thread t = new Thread() {
                public void run() {
                    ft.run();
                }
            };
            t.setDaemon(true);
            t.start();
        }

        // Check the solutions.
        TestQueryEngine.assertSameSolutions(expected, sink.iterator(), ft);

        assertEquals ( 1, stats.chunksIn.get () ) ;
        assertEquals ( 10, stats.unitsIn.get () ) ;
        assertEquals ( 7, stats.unitsOut.get () ) ;
        assertEquals ( 1, stats.chunksOut.get () ) ;
    }

}
