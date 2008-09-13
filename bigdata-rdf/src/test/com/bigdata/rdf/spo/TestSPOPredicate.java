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
 * Created on Jun 20, 2008
 */

package com.bigdata.rdf.spo;

import java.util.HashMap;

import junit.framework.TestCase2;

import com.bigdata.relation.rule.Constant;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IVariableOrConstant;
import com.bigdata.relation.rule.Predicate;
import com.bigdata.relation.rule.Var;

/**
 * Test suite for {@link SPOPredicate}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestSPOPredicate extends TestCase2 {

    /**
     * 
     */
    public TestSPOPredicate() {
    }

    /**
     * @param name
     */
    public TestSPOPredicate(String name) {
        super(name);
    }
    
    final String relation  = "test";

    final static Constant<Long> rdfsSubClassOf = new Constant<Long>(
            1L);
    
    final static Constant<Long> rdfsResource = new Constant<Long>(
            2L);
    
    final static Constant<Long> rdfType = new Constant<Long>(
            3L);
    
    final static Constant<Long> rdfsClass = new Constant<Long>(
            4L);
    
    public void test_ctor() {

        {

            final Var<Long> u = Var.var("u");

            final SPOPredicate p1 = new SPOPredicate(relation,u, rdfsSubClassOf,
                    rdfsResource);
            
            log.info(p1.toString());

            assertEquals("arity",3,p1.arity());
            
            assertEquals("variableCount", 1, p1.getVariableCount());

            assertEquals(u,p1.get(0));

            assertEquals(rdfsSubClassOf,p1.get(1));
            
            assertEquals(rdfsResource,p1.get(2));
            
        }

        {

            final Var<Long> u = Var.var("u");

            final Var<Long> v = Var.var("v");

            final SPOPredicate p1 = new SPOPredicate(relation,u, rdfsSubClassOf, v);

            log.info(p1.toString());

            assertEquals("arity", 3, p1.arity());

            assertEquals("variableCount", 2, p1.getVariableCount());

            assertEquals(u, p1.get(0));

            assertEquals(rdfsSubClassOf, p1.get(1));

            assertEquals(v, p1.get(2));
            
        }

    }
    
    /**
     * Verify equality testing with same impl.
     */
    public void test_equalsSameImpl() {

        final Var<Long> u = Var.var("u");

        final SPOPredicate p1 = new SPOPredicate(relation,u, rdfsSubClassOf, rdfsResource);

        final SPOPredicate p2 = new SPOPredicate(relation,u, rdfType, rdfsClass);

        log.info(p1.toString());

        log.info(p2.toString());

        assertTrue(p1.equals(new SPOPredicate(relation, u, rdfsSubClassOf, rdfsResource)));

        assertTrue(p2.equals(new SPOPredicate(relation, u, rdfType, rdfsClass)));
        
        assertFalse(p1.equals(p2));

        assertFalse(p2.equals(p1));
        
    }
    
    public void test_equalsDifferentImpl() {
        
        final Var<Long> u = Var.var("u");

        final SPOPredicate p1 = new SPOPredicate(relation, u, rdfType, rdfsClass);

        final Predicate p2 = new Predicate(relation, new IVariableOrConstant[] {
                u, rdfType, rdfsClass });

        log.info(p1.toString());

        log.info(p2.toString());

        assertTrue(p1.equals(p2));

        assertTrue(p2.equals(p1));

    }

    /**
     * Note: {@link HashMap} support will breaks unless the {@link IPredicate}
     * class defines <code>equals(Object o)</code>. If it just defines
     * <code>equals(IPredicate)</code> then {@link Object#equals(Object)} will
     * be invoked instead!
     */
    public void test_hashMapSameImpl() {

        final Var<Long> u = Var.var("u");
        
        final SPOPredicate p1 = new SPOPredicate(relation, u, rdfsSubClassOf, rdfsResource);

        final Predicate p1b = new Predicate(relation, new IVariableOrConstant[]{u, rdfsSubClassOf, rdfsResource});
        
        final SPOPredicate p2 = new SPOPredicate(relation, u, rdfType, rdfsClass);

        final Predicate p2b = new Predicate(relation, new IVariableOrConstant[]{u, rdfType, rdfsClass});

        // p1 and p1b compare as equal.
        assertTrue(p1.equals(p1));
        assertTrue(p1.equals(p1b));
        assertTrue(p1b.equals(p1));
        assertTrue(p1b.equals(p1b));
        
        // {p1,p1b} not equal {p2,p2b}
        assertFalse(p1.equals(p2));
        assertFalse(p1.equals(p2b));
        assertFalse(p1b.equals(p2));
        assertFalse(p1b.equals(p2b));

        // {p1,p1b} have the same hash code.
        assertEquals(p1.hashCode(), p1b.hashCode());

        // {p2,p2b} have the same hash code.
        assertEquals(p2.hashCode(), p2b.hashCode());
        
        final HashMap<IPredicate,String> map = new HashMap<IPredicate,String>();
        
        assertFalse(map.containsKey(p1));
        assertFalse(map.containsKey(p2));
        assertFalse(map.containsKey(p1b));
        assertFalse(map.containsKey(p2b));
        
        assertEquals(0,map.size());
        assertNull(map.put(p1,"p1"));
        assertEquals(1,map.size());
        assertEquals("p1",map.put(p1,"p1"));
        assertEquals(1,map.size());

        assertTrue(p1.equals(p1b));
        assertTrue(p1b.equals(p1));
        assertTrue(p1.hashCode()==p1b.hashCode());
        assertEquals("p1",map.put(p1b,"p1"));
        assertEquals(1,map.size());
        
        assertTrue(map.containsKey(p1));
        assertTrue(map.containsKey(p1b));
        assertFalse(map.containsKey(p2));
        assertFalse(map.containsKey(p2b));

        assertEquals("p1",map.get(p1));
        assertEquals("p1",map.get(p1b));
        
        map.put(p2,"p2");
        
        assertTrue(map.containsKey(p1));
        assertTrue(map.containsKey(p1b));
        assertTrue(map.containsKey(p2));
        assertTrue(map.containsKey(p2b));
        
        assertEquals("p2",map.get(p2));
        assertEquals("p2",map.get(p2b));
        
    }
    
}
