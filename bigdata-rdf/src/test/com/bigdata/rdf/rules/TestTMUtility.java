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
 * Created on Jun 24, 2008
 */

package com.bigdata.rdf.rules;

import java.util.Iterator;

import com.bigdata.relation.rule.AbstractRuleTestCase;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.IStep;
import com.bigdata.relation.rule.Program;
import com.bigdata.relation.rule.Rule;

/**
 * Test suite for rule re-writes supporting truth maintenance for the RDF DB.
 * This does NOT test the execution of those rules or their role in truth
 * maintenance.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestTMUtility extends AbstractRuleTestCase {

    /**
     * 
     */
    public TestTMUtility() {

    }

    /**
     * @param name
     */
    public TestTMUtility(String name) {

        super(name);
        
    }

    final private String database = "database";

    final private String focusStore = "focusStore";

    /**
     * Test mapping of a rule with a single predicate in the tail across two
     * relations for truth maintenance. This will produce a single new rule.
     * That rule will read from the focusStore rather than the database.
     */
    public void test_rewrite1() {

        final Rule r = new TestRuleRdfs04a(database);

        if (log.isInfoEnabled())
            log.info(r.toString());

        final Program program = TMUtility.INSTANCE.mapRuleForTruthMaintenance(
                r, focusStore);

        assertEquals(1, program.stepCount());

        {

            final IRule r0 = (IRule) program.steps().next();

            if (log.isInfoEnabled())
                log.info(r0.toString());

            // reads from the focus store.
            assertEquals(focusStore, r0.getTail(0).getOnlyRelationName());

            // writes on the focus store.
            assertEquals(focusStore, r0.getHead().getOnlyRelationName());

        }

    }

    /**
     * Test mapping of a rule with a two predicates in the tail across two
     * relations for truth maintenance. This will produce 2 new rules.
     * <p>
     * In the first rule, the first tail will read from the focusStore while the
     * second tail reads from the fused view of the focusStore and the database.
     * <p>
     * In the second rule, the first tail will read from the fused view while
     * the second tail will read from the focusStore.
     */
    public void test_rewrite2() {

        final Rule r = new TestRuleRdfs9(database);

        if (log.isInfoEnabled())
            log.info(r.toString());

        final Program program = TMUtility.INSTANCE.mapRuleForTruthMaintenance(
                r, focusStore);

        assertEquals(2, program.stepCount());

        final Iterator<? extends IStep> itr = program.steps();

        {

            final IRule r0 = (IRule) itr.next();

            if (log.isInfoEnabled())
                log.info(r0.toString());

            // 1st tail.
            assertEquals(focusStore, r0.getTail(0).getOnlyRelationName());

            // 2nd tail
            assertEquals(database, r0.getTail(1).getRelationName(0));
            assertEquals(focusStore, r0.getTail(1).getRelationName(1));
            assertEquals(2, r0.getTail(1).getRelationCount());

            // writes on the focus store.
            assertEquals(focusStore, r0.getHead().getOnlyRelationName());

        }

        {

            final IRule r1 = (IRule) itr.next();

            if (log.isInfoEnabled())
                log.info(r1.toString());

            // 1st tail.
            assertEquals(database, r1.getTail(0).getRelationName(0));
            assertEquals(focusStore, r1.getTail(0).getRelationName(1));
            assertEquals(2, r1.getTail(0).getRelationCount());

            // 2nd tail
            assertEquals(focusStore, r1.getTail(1).getOnlyRelationName());

            // writes on the focus store.
            assertEquals(focusStore, r1.getHead().getOnlyRelationName());

        }

        assertFalse(itr.hasNext());

    }

}
