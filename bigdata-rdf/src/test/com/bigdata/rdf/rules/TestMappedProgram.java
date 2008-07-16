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
 * Created on Jul 15, 2008
 */

package com.bigdata.rdf.rules;

import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.rule.IRule;

/**
 * {@link MappedProgram} is used to produce the full closure and fast closure
 * programs and is responsible, together with {@link TMUtility}, for mapping
 * those rules across the permutations of the [database/focusStore] views.
 * 
 * FIXME There are no assertions in this test suite.  It is all about
 * inspecting the {@link #log}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestMappedProgram extends AbstractRuleTestCase {

    /**
     * 
     */
    public TestMappedProgram() {
    }

    /**
     * @param name
     */
    public TestMappedProgram(String name) {
        super(name);
    }

    final private String database = "database";

    final private String focusStore = "focusStore";

    /**
     * 
     */
    public void testMappedRuleWithOneTail() {

        final AbstractTripleStore store = getStore();

        try {

            RDFSVocabulary vocab = new RDFSVocabulary(store);

            final boolean parallel = false; // actual value should not matter.

            final boolean closure = false; // actual value should not matter,

            // will map the rule across the database and the focusStore.
            final MappedProgram program = new MappedProgram(getName(),
                    focusStore, parallel, closure);

            final IRule rule = new RuleOwlEquivalentProperty(database, vocab);

            log.info("\n"+rule);
            
            program.addStep(rule);

            log.info("\n"+program);
            
        } finally {

            store.closeAndDelete();

        }

    }

    public void testMappedRuleWithTwoTails() {

        final AbstractTripleStore store = getStore();

        try {

            RDFSVocabulary vocab = new RDFSVocabulary(store);

            final boolean parallel = false; // actual value should not matter.

            final boolean closure = false; // actual value should not matter,

            // will map the rule across the database and the focusStore.
            final MappedProgram program = new MappedProgram(getName(),
                    focusStore, parallel, closure);

            final IRule rule = new RuleRdfs09(database,vocab);

            log.info("\n"+rule);
            
            program.addStep(rule);

            log.info("\n"+program);
            
        } finally {

            store.closeAndDelete();

        }

    }
    
    /**
     * 
     */
    public void testClosureOfMappedRuleWithOneTail() {

        final AbstractTripleStore store = getStore();

        try {

            RDFSVocabulary vocab = new RDFSVocabulary(store);

            final boolean parallel = false; // actual value should not matter.

            final boolean closure = false; // actual value should not matter,

            // will map the rule across the database and the focusStore.
            final MappedProgram program = new MappedProgram(getName(),
                    focusStore, parallel, closure);

            final IRule rule = new RuleOwlEquivalentProperty(database, vocab);

            log.info("\n"+rule);
            
            program.addClosureOf(rule);

            log.info("\n"+program);
            
        } finally {

            store.closeAndDelete();

        }

    }

    public void testClosureOfMappedRuleWithTwoTails() {

        final AbstractTripleStore store = getStore();

        try {

            RDFSVocabulary vocab = new RDFSVocabulary(store);

            final boolean parallel = false; // actual value should not matter.

            final boolean closure = false; // actual value should not matter,

            // will map the rule across the database and the focusStore.
            final MappedProgram program = new MappedProgram(getName(),
                    focusStore, parallel, closure);

            final IRule rule = new RuleRdfs09(database,vocab);

            log.info("\n"+rule);
            
            program.addClosureOf(rule);

            log.info("\n"+program);
            
        } finally {

            store.closeAndDelete();

        }

    }

}
