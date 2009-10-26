/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Sep 26, 2008
 */

package com.bigdata.rdf.rules;

import java.util.Properties;

import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPOPredicate;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.AbstractTripleStore.Options;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.rule.ArrayBindingSet;
import com.bigdata.relation.rule.Constant;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IConstant;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.ISlice;
import com.bigdata.relation.rule.IVariable;
import com.bigdata.relation.rule.QueryOptions;
import com.bigdata.relation.rule.Rule;
import com.bigdata.relation.rule.Slice;
import com.bigdata.relation.rule.Var;
import com.bigdata.relation.rule.eval.ActionEnum;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.IJoinNexusFactory;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.relation.rule.eval.NestedSubqueryWithJoinThreadsTask;
import com.bigdata.striterator.IChunkedOrderedIterator;

/**
 * Test for {@link ISlice} handling in native {@link IRule} execution. Slice for
 * joins is handled by {@link NestedSubqueryWithJoinThreadsTask}. Slice for an
 * {@link IAccessPath} scan is handled using the appropriate iterator and is not
 * tested by this class.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestSlice extends AbstractRuleTestCase {

    /**
     * 
     */
    public TestSlice() {
    }

    /**
     * @param name
     */
    public TestSlice(String name) {
        super(name);
    }

    /**
     * Tests various slices on an {@link IRule} using a single JOIN with 3
     * solutions.
     * 
     * @throws Exception
     */
    public void test_slice() throws Exception {
        
        final AbstractTripleStore store;
        {
        
            final Properties properties = new Properties(getProperties());
            
            // turn off infererence.
            properties.setProperty(Options.AXIOMS_CLASS, NoAxioms.class
                    .getName());
            
            store = getStore(properties);
            
        }

        try {

            /*
             * Setup the data. We will be running a simple JOIN:
             * 
             * (x foo y) (y bar z)
             * 
             * The slice will let us page through the result set.
             */

            final BigdataValueFactory f = store.getValueFactory();
            
            final BigdataURI foo = f.createURI("http://www.bigdata.com/foo");
            final BigdataURI bar = f.createURI("http://www.bigdata.com/bar");

            final BigdataURI x0 = f.createURI("http://www.bigdata.com/x0");
            final BigdataURI x1 = f.createURI("http://www.bigdata.com/x1");
            final BigdataURI x2 = f.createURI("http://www.bigdata.com/x2");

            final BigdataURI y0 = f.createURI("http://www.bigdata.com/y0");
            final BigdataURI y1 = f.createURI("http://www.bigdata.com/y1");
            final BigdataURI y2 = f.createURI("http://www.bigdata.com/y2");
            
            final BigdataURI z0 = f.createURI("http://www.bigdata.com/z0");
            final BigdataURI z1 = f.createURI("http://www.bigdata.com/z1");
            final BigdataURI z2 = f.createURI("http://www.bigdata.com/z2");
            
            /*
             * Define the terms that we will be using.
             * 
             * Note: The lexical order of the term values will determine the
             * order of the assigned term identifiers so x0<x1<y0<y1<z0<z1.
             * 
             * Note: The SLICE tests below *DEPEND* on this order constraint!!!
             */
            store.addTerms(new BigdataValue[] {//
                    
                    foo, bar,

                    x0, x1, x2,

                    y0, y1, y2,
                    
                    z0, z1, z2
                    
                    
            });
            
            // add statements.
            {

                final StatementBuffer buf = new StatementBuffer(store, 100);

                buf.add(f.createStatement(x0, foo, y0, null, StatementEnum.Explicit));
                buf.add(f.createStatement(x1, foo, y1, null, StatementEnum.Explicit));
                buf.add(f.createStatement(x2, foo, y2, null, StatementEnum.Explicit));

                buf.add(f.createStatement(y0, bar, z0, null, StatementEnum.Explicit));
                buf.add(f.createStatement(y1, bar, z1, null, StatementEnum.Explicit));
                buf.add(f.createStatement(y2, bar, z2, null, StatementEnum.Explicit));

                buf.flush();

            }

            if (log.isInfoEnabled())
                log.info("\n" + store.dumpStore());

            // used to evaluate the rule.
            final IJoinNexusFactory joinNexusFactory = store.newJoinNexusFactory(
                    RuleContextEnum.HighLevelQuery, ActionEnum.Query,
                    IJoinNexus.BINDINGS, null/* filter */);
            
            // the variables that will be bound by the rule.
            final IVariable[] vars = new IVariable[] { Var.var("x"),
                    Var.var("y"), Var.var("z") };
            
            final IBindingSet bs0 = new ArrayBindingSet(vars, new IConstant[] {//
                    new Constant<Long>(x0.getTermId()),//
                    new Constant<Long>(y0.getTermId()),//
                    new Constant<Long>(z0.getTermId())//
                    }
            ); 
            final IBindingSet bs1 = new ArrayBindingSet(vars, new IConstant[] {//
                    new Constant<Long>(x1.getTermId()),//
                    new Constant<Long>(y1.getTermId()),//
                    new Constant<Long>(z1.getTermId())//
                    }
            ); 
            final IBindingSet bs2 = new ArrayBindingSet(vars, new IConstant[] {//
                    new Constant<Long>(x2.getTermId()),//
                    new Constant<Long>(y2.getTermId()),//
                    new Constant<Long>(z2.getTermId())//
                    }
            ); 
            
            // no slice.
            assertSameSolutions(joinNexusFactory.newInstance(
                    store.getIndexManager()).runQuery(
                    newRule(store, null/* slice */, foo, bar)),
                    new IBindingSet[] { bs0, bs1, bs2 });
            
            // slice(0,1).
            assertSameSolutions(joinNexusFactory.newInstance(
                    store.getIndexManager()).runQuery(
                    newRule(store, new Slice(0L,1L), foo, bar)),
                    new IBindingSet[] { bs0 });
            
            // slice(1,1).
            assertSameSolutions(joinNexusFactory.newInstance(
                    store.getIndexManager()).runQuery(
                    newRule(store, new Slice(1L,1L), foo, bar)),
                    new IBindingSet[] { bs1 });
            
            // slice(1,2).
            assertSameSolutions(joinNexusFactory.newInstance(
                    store.getIndexManager()).runQuery(
                    newRule(store, new Slice(1L,2L), foo, bar)),
                    new IBindingSet[] { bs1, bs2 });
            
            // slice(2,1).
            assertSameSolutions(joinNexusFactory.newInstance(
                    store.getIndexManager()).runQuery(
                    newRule(store, new Slice(2L,1L), foo, bar)),
                    new IBindingSet[] { bs2 });
            
            // slice(0,2).
            assertSameSolutions(joinNexusFactory.newInstance(
                    store.getIndexManager()).runQuery(
                    newRule(store, new Slice(0L,2L), foo, bar)),
                    new IBindingSet[] { bs0, bs1 });
            
            // slice(0,4).
            assertSameSolutions(joinNexusFactory.newInstance(
                    store.getIndexManager()).runQuery(
                    newRule(store, new Slice(0L,4L), foo, bar)),
                    new IBindingSet[] { bs0, bs1, bs2 });

            // slice(2,2).
            assertSameSolutions(joinNexusFactory.newInstance(
                    store.getIndexManager()).runQuery(
                    newRule(store, new Slice(2L,2L), foo, bar)),
                    new IBindingSet[] { bs2 });

        } finally {

            store.__tearDownUnitTest();

        }
        
    }

    /**
     * Creates a new rule instance for {@link #test_slice()}.
     * 
     * @param store
     * @param slice
     * @param foo
     * @param bar
     * @return
     */
    protected IRule newRule(AbstractTripleStore store, ISlice slice,
            BigdataValue foo, BigdataValue bar) {
        
        assert foo.getTermId() != NULL;
        assert bar.getTermId() != NULL;
        
        return new Rule<ISPO>(getName(),
                null/* head */, new SPOPredicate[] {
                //
                new SPOPredicate(store.getSPORelation()
                    .getNamespace(), Var.var("x"), new Constant<Long>(foo
                    .getTermId()), Var.var("y")),
                //
                new SPOPredicate(store.getSPORelation()
                            .getNamespace(), Var.var("y"), new Constant<Long>(bar
                            .getTermId()), Var.var("z")),
                },//
                new QueryOptions(false/* distinct */, true/* stable */,
                        null/* orderBy */, slice),//
                null// constraints
        );

    }

    /**
     * Verifies the the iterator visits {@link ISolution}s have the expected
     * {@link IBindingSet}s in the expected order.
     * 
     * @param itr
     *            The iterator.
     * @param expected
     *            The expected {@link IBindingSet}s.
     */
    protected void assertSameSolutions(final IChunkedOrderedIterator<ISolution> itr,
            final IBindingSet[] expected) {

        if (itr == null)
            throw new IllegalArgumentException();

        if (expected == null)
            throw new IllegalArgumentException();

        try {

            int n = 0;
            
            while(itr.hasNext()) {
                
                final IBindingSet actual = itr.next().getBindingSet();

                assertNotNull("bindings not requested?", actual);
                
                if(!actual.equals(expected[n])) {
                    
                    fail("Wrong bindings: index=" + n + ", actual=" + actual
                            + ", expected=" + expected[n]);
                    
                }
                
                n++;
                
            }
            
            if(log.isInfoEnabled())
                log.info("Matched "+n+" binding sets");
            
            // verify correct #of solutions identified.
            assertEquals("#of solutions", n, expected.length);
            
        } finally {
            
            itr.close();
            
        }
        
    }

}
