/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Oct 24, 2007
 */

package com.bigdata.rdf.inf.tm;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.rdf.inf.Rule;
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
