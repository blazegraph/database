/*

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on March 19, 2008
 */

package com.bigdata.rdf.store;

import java.util.Properties;

import com.bigdata.btree.IIndex;

/**
 * Test suite for configuration of the BLOBS index support.
 * 
 * @see <a href="https://github.com/SYSTAP/bigdata-gpu/issues/25"> Disable BLOBS
 *      indexing completely for GPU </a>
 */
public class TestBlobsConfiguration extends AbstractTripleStoreTestCase {

    /**
     * 
     */
    public TestBlobsConfiguration() {
    }

    /**
     * @param name
     */
    public TestBlobsConfiguration(final String name) {
        super(name);
    }

	/**
	 * Default configuration - verify that BLOBS index exists.
	 */
	public void test_blobsSupport_defaultConfiguration() {

		final AbstractTripleStore store = getStore();

		try {

			final IIndex blobsIndex = store.getLexiconRelation().getBlobsIndex();

			assertNotNull(blobsIndex);

			assertEquals(store.getLexiconRelation().getLexiconConfiguration().getBlobsThreshold(),
					Integer.valueOf(AbstractTripleStore.Options.DEFAULT_BLOBS_THRESHOLD).intValue());

		} finally {

			store.__tearDownUnitTest();

		}

	}

	/**
	 * Override configuration - BLOBS index uses a non-default threashold.
	 */
	public void test_blobsSupport_nonDefaultBlobsIndexThresholdConfiguration() {

		final Properties p = new Properties(getProperties());

		final int overrideThreshold = Integer.valueOf(AbstractTripleStore.Options.DEFAULT_BLOBS_THRESHOLD) * 2;

		p.setProperty(AbstractTripleStore.Options.BLOBS_THRESHOLD, Integer.toString(overrideThreshold));

		final AbstractTripleStore store = getStore(p);

		try {

			final IIndex blobsIndex = store.getLexiconRelation().getBlobsIndex();

			assertNotNull(blobsIndex);

			assertEquals(store.getLexiconRelation().getLexiconConfiguration().getBlobsThreshold(),
					overrideThreshold);

		} finally {

			store.__tearDownUnitTest();

		}

	}

	/**
	 * Override configuration - BLOBS index is disabled.
	 */
	public void test_blobsSupport_noBlobsIndexConfiguration() {

		final Properties p = new Properties(getProperties());

		p.setProperty(AbstractTripleStore.Options.BLOBS_THRESHOLD, Integer.toString(Integer.MAX_VALUE));

		final AbstractTripleStore store = getStore(p);

		try {

			try {
				// Note: Defined to throw an exception if the index does not exist.
				store.getLexiconRelation().getBlobsIndex();
				fail("Expecting: " + IllegalStateException.class);
			} catch (IllegalStateException ex) {
				if (log.isInfoEnabled())
					log.info("Ignoring expected exception: " + ex);
			}			

			assertEquals(store.getLexiconRelation().getLexiconConfiguration().getBlobsThreshold(),
					Integer.MAX_VALUE);

		} finally {

			store.__tearDownUnitTest();

		}

	}

}
