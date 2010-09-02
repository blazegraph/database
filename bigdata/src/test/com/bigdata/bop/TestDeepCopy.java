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
 * Created on Aug 21, 2010
 */

package com.bigdata.bop;

import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.TestCase2;

import com.bigdata.bop.ap.Predicate;
import com.bigdata.bop.constraint.EQ;
import com.bigdata.bop.constraint.EQConstant;
import com.bigdata.bop.constraint.INBinarySearch;
import com.bigdata.bop.constraint.NE;
import com.bigdata.bop.constraint.NEConstant;
import com.bigdata.bop.constraint.OR;
import com.bigdata.rdf.internal.constraints.InlineEQ;
import com.bigdata.rdf.internal.constraints.InlineGE;
import com.bigdata.rdf.internal.constraints.InlineGT;
import com.bigdata.rdf.internal.constraints.InlineLE;
import com.bigdata.rdf.internal.constraints.InlineLT;
import com.bigdata.rdf.internal.constraints.InlineNE;
import com.bigdata.rdf.rules.RejectAnythingSameAsItself;
import com.bigdata.rdf.spo.SPOPredicate;
import com.bigdata.rdf.spo.SPOStarJoin;

/**
 * Unit tests for the existence of the required deep copy semantics for
 * {@link BOp}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Test that the copy has distinct references for each argument and each
 *       annotation which is a {@link BOp}.
 */
public class TestDeepCopy extends TestCase2 {

    static private final String cause_shallow = "No shallow copy constructor";

    static private final String cause_deep = "No deep copy constructor";

    /**
     * A list of all classes and interfaces which implement BOp.
     * <p>
     * Note: The only way to enumerate the implementations of a class is to scan
     * through the jars in the classpath. Therefore this list needs to be
     * maintained by hand.
     */
    static final Class<?>[] all = { //
        // com.bigdata.bop
            BOp.class,//
            BOpBase.class,//
            BOpList.class,//
            Predicate.class,//
            Constant.class,//
            Var.class,//
            QuoteOp.class,//
            // com.bigdata.bop.constraint
            EQ.class,//
            NE.class,//
            EQConstant.class,//
            NEConstant.class,//
            OR.class,//
            INBinarySearch.class,//
            // com.bigdata.rdf.spo
            SPOPredicate.class,//
            SPOStarJoin.class,//
            com.bigdata.rdf.magic.MagicPredicate.class,//
            // com.bigdata.rdf.internal.constraint
            InlineEQ.class,//
            InlineGE.class,//
            InlineLT.class,//
            InlineLE.class,//
            InlineGT.class,//
            InlineNE.class,//
            // com.bigdata.rdf.inf
            RejectAnythingSameAsItself.class,

    };

    /**
     * Exclusion list for classes which do not support deep copy semantics.
     */
    static final Set<Class<?>> noDeepCopy = new LinkedHashSet<Class<?>>(Arrays
            .asList(new Class<?>[] { /**
             * {@link Var} does not have deep copy
             * semantics since it imposes a canonizaling mapping from names to
             * object references.
             */
            Var.class, //
            }));

    /**
     * Exclusion list for classes which do not support shallow copy semantics.
     */
    static final Set<Class<?>> noShallowCopy = new LinkedHashSet<Class<?>>(
            Arrays.asList(new Class<?>[] {//
                    Var.class,//
                    Constant.class //
                    }));

    /**
     * 
     */
    public TestDeepCopy() {
    }

    /**
     * @param name
     */
    public TestDeepCopy(String name) {
        super(name);
    }

    /**
     * Visits the {@link BOp} hierarchy and verify that all {@link BOp}s declare
     * the required public constructors (shallow copy and deep copy). A list of
     * all bad {@link BOp}s is collected. If that list is not empty, then the
     * list is reported as a test failure.
     */
    public void test_ctors() {

        // all bad bops.
        final Map<Class<?>,String/*cause*/> bad = new LinkedHashMap<Class<?>,String>();

        // all discovered bops.
        final List<Class<?>> found = new LinkedList<Class<?>>();

        for(Class<?> cls : all) {
            
            if(cls.isInterface()) {
                // skip interfaces.
//                System.out.println("Skipping interface: "+cls.getName());
                continue;
            }

            final int mod = cls.getModifiers();
            if(Modifier.isAbstract(mod)) {
                // skip abstract classes since we can't get their ctors.
//                System.err.println("Skipping abstract classes: "+cls.getName());
                continue;
            }

            found.add(cls);
//            System.err.println(cls.getName());
            
        }

        for (Class<?> cls : found) {

            // test for shallow copy constructor.
            if (!noShallowCopy.contains(cls)) {
                try {
                    cls.getConstructor(new Class[] { BOp[].class, Map.class });
                } catch (NoSuchMethodException e) {
                    bad.put(cls, cause_shallow);
                    log.error(cause_shallow + " : " + cls);// , e);
                }
            }

            // test for deep copy constructor.
            if (!noDeepCopy.contains(cls)) {
                try {
                    cls.getConstructor(new Class[] { cls });
                } catch (NoSuchMethodException e) {
                    bad.put(cls, cause_deep);
                    log.error(cause_deep + " : " + cls);// , e);
                }
            }

        }

        if (!bad.isEmpty()) {

            System.err.println("Errors: "+bad.keySet());
            
            fail(bad.toString());
        
        }

    }

}
