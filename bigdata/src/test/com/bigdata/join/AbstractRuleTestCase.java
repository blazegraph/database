/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Apr 18, 2007
 */

package com.bigdata.join;

import junit.framework.TestCase2;

import com.bigdata.join.rdf.SPOPredicate;
import com.bigdata.journal.ProxyTestCase;
import com.bigdata.service.EmbeddedFederation;
import com.bigdata.service.IBigdataClient;
import com.bigdata.service.LocalDataServiceFederation;

/**
 * 
 * @todo make this a {@link ProxyTestCase} and run against a
 *       {@link LocalDataServiceFederation}, an {@link EmbeddedFederation},
 *       and a jini federation (the abstraction is the {@link IBigdataClient}).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractRuleTestCase extends TestCase2 {
    
    /**
     * 
     */
    public AbstractRuleTestCase() {
    }

    /**
     * @param name
     */
    public AbstractRuleTestCase(String name) {
        super(name);
    }

    protected final static Constant<Long> rdfsSubClassOf = new Constant<Long>(
            1L);
    
    protected final static Constant<Long> rdfsResource = new Constant<Long>(
            2L);
    
    protected final static Constant<Long> rdfType = new Constant<Long>(
            3L);
    
    protected final static Constant<Long> rdfsClass = new Constant<Long>(
            4L);

    protected final static Constant<Long> rdfProperty = new Constant<Long>(
            5L);

    /**
     * this is rdfs9:
     * 
     * <pre>
     * (?u,rdfs:subClassOf,?x), (?v,rdf:type,?u) -> (?v,rdf:type,?x)
     * </pre>
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    @SuppressWarnings("serial")
    static protected class TestRuleRdfs9 extends Rule {
        
        public TestRuleRdfs9(IRelationName relation) {
            
            super(  "rdfs9",//
                    new SPOPredicate(relation,var("v"), rdfType, var("x")), //
                    new SPOPredicate[] {//
                            new SPOPredicate(relation, var("u"), rdfsSubClassOf, var("x")),//
                            new SPOPredicate(relation, var("v"), rdfType, var("u")) //
                    },//
                    new IConstraint[] {
                            new NE(var("u"),var("x"))
                        }
            );
            
        }

    }
    
    /**
     * rdfs4a:
     * 
     * <pre>
     * (?u ?a ?x) -&gt; (?u rdf:type rdfs:Resource)
     * </pre>
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    @SuppressWarnings("serial")
    static protected class TestRuleRdfs04a extends Rule {

        public TestRuleRdfs04a(IRelationName relation) {

            super("rdfs4a",//
                    new SPOPredicate(relation,//
                            Var.var("u"), rdfType, rdfsResource), //
                    new SPOPredicate[] { //
                    new SPOPredicate(relation,//
                            Var.var("u"), Var.var("a"), Var.var("x")) //
                    },
                    /* constraints */
                    null);

        }

    }

}
