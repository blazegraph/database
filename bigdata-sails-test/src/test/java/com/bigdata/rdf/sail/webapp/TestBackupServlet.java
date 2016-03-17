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
package com.bigdata.rdf.sail.webapp;

import java.io.File;
import java.util.Collections;
import java.util.UUID;

import junit.framework.Test;

import org.openrdf.rio.RDFFormat;

import com.bigdata.journal.BufferMode;
import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository.AddOp;
import com.bigdata.rdf.sail.webapp.client.RemoteRepositoryManager;

/**
 * Proxied test suite for {@link DataLoaderServlet}
 * 
 * @author beebs
 */
public class TestBackupServlet<S extends IIndexManager> extends
		AbstractTestNanoSparqlClient<S> {

	final static String BASE = "com/bigdata/rdf/sail/webapp/";

	public TestBackupServlet() {

	}

	public TestBackupServlet(final String name) {

		super(name);

	}

	static public Test suite() {

		return ProxySuiteHelper.suiteWhenStandalone(
				TestBackupServlet.class, "test_backup01",
				Collections.singleton(BufferMode.DiskRW));
	}
	
	
	/**
	 * Load the test data set.
	 * 
	 * @throws Exception
	 */
	private void doLoadFile() throws Exception {
        /*
		 * Only for testing. Clients should use AddOp(File, RDFFormat) or SPARQL
		 * UPDATE "LOAD".
		 */
        loadFile(
        		this.getClass().getResource("sample-data.ttl")
				.getFile(),
                RDFFormat.TURTLE);
	}
	
    /**
     * Load a file.
     * 
     * @param file
     *            The file.
     * @param format
     *            The file format.
     * @throws Exception
     */
    protected void loadFile(final String file, final RDFFormat format)
            throws Exception {

        final AddOp add = new AddOp(new File(file), format);

        m_repo.add(add);

    }

	public void test_backup01() throws Exception {
		
		doLoadFile();

		final String fileStr = System.getProperty("java.io.tmpdir")
				+ File.separator + UUID.randomUUID() + ".jnl";

		final File randomFile = new File(fileStr);
		
		assertTrue(!randomFile.exists());
		
		final RemoteRepositoryManager mgr = m_repo.getRemoteRepositoryManager();

		//Test with default configuration
		mgr.onlineBackup(fileStr, false, true);

		//The size of the uncompressed file
		long uncomp_size = randomFile.length();
		
		assertTrue(randomFile.exists());
		
		randomFile.delete();

		assertTrue(!randomFile.exists());

		//Test with gzip
		mgr.onlineBackup(fileStr, true, true);

		//Check if compressed
		assertTrue(uncomp_size > randomFile.length());

		assertTrue(randomFile.exists());
		
		
		randomFile.delete();

		assertTrue(!randomFile.exists());
		
		//Test with blocking
		mgr.onlineBackup(fileStr, false, false);
		
		final int maxSleep = 100;
		int i = 0;
		
		//Wait for the executor to complete.
		//Make sure we don't hit an infinite loop.
		while(!randomFile.exists() && i < maxSleep) {
			Thread.sleep(2);
			i++;
		}

		assertTrue(randomFile.exists());
		
		randomFile.delete();
		

		assertTrue(!randomFile.exists());

	}

}
