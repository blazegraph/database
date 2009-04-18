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
 * Created on Apr 18, 2009
 */

package com.bigdata.rdf.rio;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.util.concurrent.atomic.AtomicInteger;

import org.openrdf.rio.RDFFormat;

import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.AbstractTripleStoreTestCase;

/**
 * Abstract base class for unit tests involving the RIO integration.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AbstractRIOTestCase extends AbstractTripleStoreTestCase {

    /**
     * 
     */
    public AbstractRIOTestCase() {
    }

    /**
     * @param name
     */
    public AbstractRIOTestCase(String name) {
        super(name);
    }

    /**
     * Verify that the explicit statements given by the resource are present in
     * the KB.
     * <p>
     * What is actually verified is that all statements that are re-parsed are
     * found in the KB, that the lexicon is self-consistent, and that the
     * statement indices are self-consistent. The test does NOT reject a KB
     * which has statements not found during the re-parse since there can be
     * axioms and other stuff in the KB.
     * 
     * @param store
     * @param resource
     * @throws FileNotFoundException
     * @throws Exception
     * 
     * @todo test based on this method will probably fail if the source data
     *       contains bnodes since it does not validate bnodes based on
     *       consistent RDF properties but only based on their Java fields.
     */
    protected void verify(final AbstractTripleStore store, final String resource)
            throws FileNotFoundException, Exception {

        if (log.isInfoEnabled()) {
            log.info("computing predicate usage...");
            log.info("\n" + store.predicateUsage());
        }
        
        /*
         * re-parse and verify all statements exist in the db using each
         * statement index.
         */
        final AtomicInteger nerrs = new AtomicInteger(0);
        final int maxerrors = 20;
        {

            log.info("Verifying all statements found using reparse: file="
                    + resource);

            // buffer capacity (#of statements per batch).
            final int capacity = 100000;

            final IRioLoader loader = new StatementVerifier(store, capacity,
                    nerrs, maxerrors);

            loader.loadRdf(new BufferedReader(new InputStreamReader(
                    new FileInputStream(resource))), ""/* baseURI */,
                    RDFFormat.RDFXML, false/* verify */);

            log.info("End of reparse: nerrors=" + nerrs + ", file=" + resource);

        }

        assertEquals("nerrors", 0, nerrs.get());

        assertStatementIndicesConsistent(store, maxerrors);

    }

}
