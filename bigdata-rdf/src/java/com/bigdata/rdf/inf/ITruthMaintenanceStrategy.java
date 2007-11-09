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
 * Created on Oct 24, 2007
 */

package com.bigdata.rdf.inf;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.rdf.spo.SPO;

/**
 * Interface for a truth maintenance strategy.
 * 
 * @todo implement AllProofs (refactor)
 * @todo implement OneProof (requires magic sets).
 * @todo implement NoProofs (reclose the store).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ITruthMaintenanceStrategy {

    /**
     * Create a proof.
     * 
     * @param rule
     *            The rule that licenses the proof.
     * @param head
     *            The entailment.
     * @param tail
     *            The bindings for the tail of the rule.
     */
    public void createProof(Rule rule, SPO head, SPO[] tail);
    
    /**
     * Create a proof.
     * 
     * @param rule
     *            The rule that licenses the proof.
     * @param head
     *            The entailment.
     * @param tail
     *            The bindings for the tail of the rule.
     */
    public void createProof(Rule rule, SPO head, long[] tail);

    /**
     * Removes explicit statement(s) matching the triple pattern. If the TM
     * strategy stores entailments in the database, then this and any
     * entailments that are no longer grounded.
     * 
     * @param s
     * @param p
     * @param o
     * @return The #of statements removed.
     */
    public int removeStatements(Resource s, URI p, Value o);

}
