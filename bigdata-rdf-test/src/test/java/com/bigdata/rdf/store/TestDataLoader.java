/**

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
package com.bigdata.rdf.store;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Properties;

import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;

import com.bigdata.rdf.store.DataLoader.ClosureEnum;
import com.bigdata.rdf.store.DataLoader.CommitEnum;
import com.bigdata.util.InnerCause;

/**
 * Test suite for the {@link DataLoader}.
 *
 * @author bryan
 * 
 *         TODO Tests to verify that data was actually loaded.... (read back
 *         from last commit point and verify statements found). Do this for each
 *         of the public {@link DataLoader} methods.
 * 
 *         TODO Test to verify that end-of-batch handling is correct (no
 *         additional commits).
 */
public class TestDataLoader extends AbstractTripleStoreTestCase {

	public TestDataLoader() {
	}

	public TestDataLoader(final String name) {
		super(name);
	}

	/**
	 * Simple test of ability to load a resource (from the classpath).
	 */
	public void test_DataLoader_loadResource01() throws IOException {

		final AbstractTripleStore store = getStore();

		try {

			final DataLoader dataLoader = new DataLoader(store);

			final String resource = "com/bigdata/rdf/store/sample-data.ttl";

			final String baseURL = new File(resource).toURI().toString();

			dataLoader.loadData(new String[] { resource }, new String[] { baseURL },
					new RDFFormat[] { RDFFormat.TURTLE });

		} finally {

			store.__tearDownUnitTest();

		}

	}

	/**
	 * Test where an error in a source file SHOULD NOT be ignored because we
	 * have NOT specified {@link DataLoader.Options#IGNORE_INVALID_FILES}.
	 * 
	 * @see BLZG-1531 (Add option to make the DataLoader robust to files that
	 *      cause rio to throw a fatal exception)
	 */
	public void test_DataLoader_ignoreFailures01() throws IOException {

		boolean ok = false;
		
		final String resource = "com/bigdata/rdf/store/sample-data-bad.ttl";

		final AbstractTripleStore store = getStore();

		try {

			final DataLoader dataLoader = new DataLoader(store);

			final String baseURL = new File(resource).toURI().toString();

			dataLoader.loadData(new String[] { resource }, new String[] { baseURL },
					new RDFFormat[] { RDFFormat.TURTLE });

			ok = true;

		} catch (Throwable t) {

			if (!InnerCause.isInnerCause(t, RDFParseException.class)) {

				fail("Expected inner cause " + RDFParseException.class + " not found in " + t, t);

			}

		} finally {

			store.__tearDownUnitTest();

		}

		if (ok)
			fail("Error should have been reported for " + resource);
	
	}

	/**
	 * Test where an error in a source file SHOULD be ignored because we have
	 * specified {@link DataLoader.Options#IGNORE_INVALID_FILES}.
	 * 
	 * @see BLZG-1531 (Add option to make the DataLoader robust to files that
	 *      cause rio to throw a fatal exception)
	 */
	public void test_DataLoader_ignoreFailures02() throws IOException {

		final AbstractTripleStore store = getStore();

		try {

			final Properties properties = new Properties(store.getProperties());
			
			properties.setProperty(DataLoader.Options.IGNORE_INVALID_FILES, "true");
			
			final DataLoader dataLoader = new DataLoader(properties, store);

			final String resource = "com/bigdata/rdf/store/sample-data-bad.ttl";

			final String baseURL = new File(resource).toURI().toString();

			dataLoader.loadData(new String[] { resource }, new String[] { baseURL },
					new RDFFormat[] { RDFFormat.TURTLE });

		} finally {

			store.__tearDownUnitTest();

		}

	}

	/**
	 * Test where the load should fail even though we specified
	 * {@link DataLoader.Options#IGNORE_INVALID_FILES} because it error was a
	 * "resource not found" problem rather than a parser error.
	 * 
	 * @see BLZG-1531 (Add option to make the DataLoader robust to files that
	 *      cause rio to throw a fatal exception)
	 */
	public void test_DataLoader_ignoreFailures03() throws IOException {

		final AbstractTripleStore store = getStore();

		try {

			final Properties properties = new Properties(store.getProperties());
			
			properties.setProperty(DataLoader.Options.IGNORE_INVALID_FILES, "true");
			
			final DataLoader dataLoader = new DataLoader(properties, store);

			final String resource = "com/bigdata/rdf/store/sample-data-DOES-NOT-EXIST.ttl";

			final String baseURL = new File(resource).toURI().toString();

			try {

				dataLoader.loadData(new String[] { resource }, new String[] { baseURL },
						new RDFFormat[] { RDFFormat.TURTLE });
				
				fail();
				
			} catch (IOException ex) {
				
				if (log.isInfoEnabled())
					log.info("Ignoring expected exception: " + ex);
				
			}
			
		} finally {

			store.__tearDownUnitTest();

		}

	}

	/**
	 * Test durable queues using {@link CommitEnum#Incremental} and {@link ClosureEnum#None}
	 */
	public void test_durableQueues01_incrementalCommit_noClosure() throws IOException {

		final AbstractTripleStore store = getStore();

		try {

			final Properties properties = new Properties(store.getProperties());

			// enable durable queues.
			properties.setProperty(DataLoader.Options.DURABLE_QUEUES, "true");

			// Incremental commit.
			properties.setProperty(DataLoader.Options.COMMIT, CommitEnum.Incremental.name());

			properties.setProperty(DataLoader.Options.CLOSURE, ClosureEnum.None.name());

			final DataLoader dataLoader = new DataLoader(properties, store);

			doDurableQueueTest(dataLoader);
			
		} finally {

			store.__tearDownUnitTest();
		}

	}
	
	/**
	 * Test durable queues using {@link CommitEnum#Incremental} and {@link ClosureEnum#Batch}
	 */
	public void test_durableQueues01_incrementalCommit_batchClosure() throws IOException {

		final AbstractTripleStore store = getStore();

		try {

			final Properties properties = new Properties(store.getProperties());

			// enable durable queues.
			properties.setProperty(DataLoader.Options.DURABLE_QUEUES, "true");

			// Incremental commit.
			properties.setProperty(DataLoader.Options.COMMIT, CommitEnum.Incremental.name());

			properties.setProperty(DataLoader.Options.CLOSURE, ClosureEnum.Batch.name());

			final DataLoader dataLoader = new DataLoader(properties, store);

			doDurableQueueTest(dataLoader);
			
		} finally {

			store.__tearDownUnitTest();
		}

	}
	
	/**
	 * Test durable queues using {@link CommitEnum#Batch} and {@link ClosureEnum#Batch}
	 */
	public void test_durableQueues02_batchCommit_batchClosure() throws IOException {

		final AbstractTripleStore store = getStore();

		try {

			final Properties properties = new Properties(store.getProperties());

			// enable durable queues.
			properties.setProperty(DataLoader.Options.DURABLE_QUEUES, "true");

			// Batch commit.
			properties.setProperty(DataLoader.Options.COMMIT, CommitEnum.Batch.name());

			properties.setProperty(DataLoader.Options.CLOSURE, ClosureEnum.Batch.name());

			final DataLoader dataLoader = new DataLoader(properties, store);

			doDurableQueueTest(dataLoader);
			
		} finally {

			store.__tearDownUnitTest();
		}

	}
	
	/**
	 * Test durable queues using {@link CommitEnum#Batch} and {@link ClosureEnum#None}
	 */
	public void test_durableQueues02_batchCommit_noClosure() throws IOException {

		final AbstractTripleStore store = getStore();

		try {

			final Properties properties = new Properties(store.getProperties());

			// enable durable queues.
			properties.setProperty(DataLoader.Options.DURABLE_QUEUES, "true");

			// Batch commit.
			properties.setProperty(DataLoader.Options.COMMIT, CommitEnum.Batch.name());

			properties.setProperty(DataLoader.Options.CLOSURE, ClosureEnum.None.name());

			final DataLoader dataLoader = new DataLoader(properties, store);

			doDurableQueueTest(dataLoader);
			
		} finally {

			store.__tearDownUnitTest();
		}

	}
	
	/**
	 * Test durable queues using {@link CommitEnum#Batch} and {@link ClosureEnum#Incremental}
	 */
	public void test_durableQueues02_batchCommit_incrementalClosure() throws IOException {

		final AbstractTripleStore store = getStore();

		try {

			final Properties properties = new Properties(store.getProperties());

			// enable durable queues.
			properties.setProperty(DataLoader.Options.DURABLE_QUEUES, "true");

			// Batch commit.
			properties.setProperty(DataLoader.Options.COMMIT, CommitEnum.Batch.name());

			properties.setProperty(DataLoader.Options.CLOSURE, ClosureEnum.Incremental.name());

			final DataLoader dataLoader = new DataLoader(properties, store);

			doDurableQueueTest(dataLoader);
			
		} finally {

			store.__tearDownUnitTest();
		}

	}
	
	private void doDurableQueueTest(final DataLoader dataLoader) throws IOException {
		
		// temporary directory where we setup the test.
		final File tmpDir = Files.createTempDirectory(getClass().getName() + ".tmp").toFile();
		try { // recreate it as a directory.
		
			// Setup directory with files.
			final File goodFile = new File(tmpDir, "good.ttl");
			final File failFile = new File(tmpDir, "fail.ttl");
			{

				final String goodData = ""+//
				"@prefix bd: <http://www.bigdata.com/> .\n"+//
				"@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n"+//
				"@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n"+//
				"@prefix foaf: <http://xmlns.com/foaf/0.1/> .\n"+//
				"bd:Mike rdf:type foaf:Person .\n"+//
				"bd:Bryan rdf:type foaf:Person .\n"+//
				"bd:Martyn rdf:type foaf:Person .\n"+//
				"bd:Mike rdfs:label \"Mike\" .\n"+//
				"bd:Bryan rdfs:label \"Bryan\" .\n"+//
				"bd:DC rdfs:label \"DC\" .\n"+//
				"bd:Mike foaf:knows bd:Bryan .\n"+//
				"bd:Bryan foaf:knows bd:Mike .\n"+//
				"bd:Bryan foaf:knows bd:Martyn .\n"+//
				"bd:Martyn foaf:knows bd:Bryan .\n"+//
				"";

				writeOnFile(goodFile, goodData);
				
				// Note: has a Literal in the Subject position.
				final String failData = ""+//
				"@prefix bd: <http://www.bigdata.com/> .\n"+//
				"@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n"+//
				"@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n"+//
				"@prefix foaf: <http://xmlns.com/foaf/0.1/> .\n"+//
				"\"Mike\" rdf:type foaf:Person .\n"+//
				"bd:Bryan rdf:type foaf:Person .\n"+//
				"bd:Martyn rdf:type foaf:Person .\n"+//
				"bd:Mike rdfs:label \"Mike\" .\n"+//
				"bd:Bryan rdfs:label \"Bryan\" .\n"+//
				"bd:DC rdfs:label \"DC\" .\n"+//
				"bd:Mike foaf:knows bd:Bryan .\n"+//
				"bd:Bryan foaf:knows bd:Mike .\n"+//
				"bd:Bryan foaf:knows bd:Martyn .\n"+//
				"bd:Martyn foaf:knows bd:Bryan .\n"+//
				"";

				writeOnFile(failFile, failData);

			}

			// Run loader.
			{

				dataLoader.loadFiles(tmpDir, null/* baseURI */, RDFFormat.TURTLE, null/* defaultGraph */,
						new FilenameFilter() {
							@Override
							public boolean accept(File dir, String name) {
								return name.endsWith(".ttl");
							}
						});

			}
			
			// Verify post-conditions in the tmp directory.
			{
				
				if (goodFile.exists())
					fail("File was not renamed: " + goodFile);
				
				if (failFile.exists())
					fail("File was not renamed: " + failFile);

				{
					
					final File tmp = new File(goodFile.getPath() + ".good");

					if (!tmp.exists())
						fail("File not found: " + tmp);

				}
				
				{
			
					final File tmp = new File(failFile.getPath() + ".fail");

					if (!tmp.exists())
						fail("File not found: " + tmp);
					
				}

			}

		} finally {

			// destroy the temporary directory.
			recursiveDelete(tmpDir);
//			System.err.println("tmpDir=" + tmpDir);
			
		}

	}

	private void writeOnFile(final File file, final String data) throws IOException {
		
		file.createNewFile();
		
		try (final FileWriter fileWriter = new FileWriter(file)) {

			try (final BufferedWriter bufferedWriter = new BufferedWriter(fileWriter)) {

				bufferedWriter.write(data);

				bufferedWriter.flush();

			}

		}

	}

}
