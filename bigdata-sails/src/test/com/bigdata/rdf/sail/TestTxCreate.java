/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Mar 7, 2011
 */

package com.bigdata.rdf.sail;

import java.util.Properties;

import org.openrdf.sail.SailException;

import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.internal.LexiconConfiguration;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.vocab.NoVocabulary;

/**
 * Unit test for the creation of a Sail with isolatable indices. This unit test
 * was developed in response to <a
 * href="https://sourceforge.net/apps/trac/bigdata/ticket/252">issue #252</a>,
 * which reported a problem when creating a Sail which supports fully isolated
 * indices and also uses inline date times. The problem goes back to how the
 * {@link LexiconConfiguration} gains access to the ID2TERM index when it is
 * initialized.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestTxCreate extends ProxyBigdataSailTestCase {

    /**
     * 
     */
    public TestTxCreate() {
    }

    /**
     * @param name
     */
    public TestTxCreate(String name) {
        super(name);
    }

    /**
     * Version of the test with data time inlining disabled.
     * 
     * @throws SailException 
     */
    public void test_tx_create() throws SailException {

        final Properties properties = getProperties();

        // truth maintenance is not compatible with full transactions.
        properties.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE, "false");

        properties.setProperty(AbstractTripleStore.Options.AXIOMS_CLASS,
                NoAxioms.class.getName());

        properties.setProperty(AbstractTripleStore.Options.VOCABULARY_CLASS,
                NoVocabulary.class.getName());

        properties.setProperty(BigdataSail.Options.ISOLATABLE_INDICES, "true");

        properties.setProperty(AbstractTripleStore.Options.JUSTIFY, "false");

        properties.setProperty(AbstractTripleStore.Options.INLINE_DATE_TIMES,
                "false");

        final BigdataSail sail = new BigdataSail(properties);
        
        try {

            sail.initialize();

            log.info("Sail is initialized.");

        } finally {

            sail.__tearDownUnitTest();

        }

    }
    
    /**
     * Version of the test with data time inlining enabled.
     * @throws SailException 
     */
    public void test_tx_create_withInlineDateTimes() throws SailException {

        final Properties properties = getProperties();

        // truth maintenance is not compatible with full transactions.
        properties.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE, "false");

        properties.setProperty(AbstractTripleStore.Options.AXIOMS_CLASS,
                NoAxioms.class.getName());
       
        properties.setProperty(AbstractTripleStore.Options.VOCABULARY_CLASS,
                NoVocabulary.class.getName());

        properties.setProperty(AbstractTripleStore.Options.JUSTIFY, "false");

        properties.setProperty(BigdataSail.Options.ISOLATABLE_INDICES, "true");

        properties.setProperty(AbstractTripleStore.Options.INLINE_DATE_TIMES,
                "true");

        final BigdataSail sail = new BigdataSail(properties);

        try {

            sail.initialize();

            log.info("Sail is initialized.");

        } finally {

            sail.__tearDownUnitTest();

        }

    }

}
