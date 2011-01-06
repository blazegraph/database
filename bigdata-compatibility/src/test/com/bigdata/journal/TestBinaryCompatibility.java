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
 * Created on Nov 19, 2010
 */
package com.bigdata.journal;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import junit.framework.TestCase2;

import com.bigdata.Banner;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;

/**
 * Test suite for binary compatibility, portability, and forward compatibility
 * or automated migration of persistent stores and persistence or serialization
 * capable objects across different bigdata releases. The tests in this suite
 * rely on artifacts which are archived within SVN.
 * 
 * @todo create w/ small extent and truncate (RW store does not support
 *       truncate).
 * 
 * @todo test binary migration and forward compatibility.
 * 
 * @todo stubs to create and organize artifacts,etc.
 *
 * @todo data driven test suite?
 * 
 * @todo create artifact for each release, name the artifacts systematically,
 *       e.g., test.release.(RW|WORM).jnl or test.release.seg. Collect a list of
 *       the created artifacts and run each test against each of the versions of
 *       the artifact.
 * 
 * @todo Force artifact file name case for file system compatibility?
 * 
 * @todo test journal (WORM and RW), btree, index segment, row store, persistent
 *       data structures (checkpoints, index metadata, tuple serializers, etc.),
 *       RDF layer, RMI message formats, etc.
 * 
 * @todo Specific tests for
 *       <p>
 *       Name2Addr and DefaultKeyBuilderFactory portability problem. See
 *       https://sourceforge.net/apps/trac/bigdata/ticket/193
 *       <p>
 *       WORM global row store resolution problem introduced in the
 *       JOURNAL_HA_BRANCH. See
 *       https://sourceforge.net/apps/trac/bigdata/ticket/171#comment:5
 *       <p>
 *       Sparse row store JDK encoding problem:
 *       https://sourceforge.net/apps/trac/bigdata/ticket/107
 */
public class TestBinaryCompatibility extends TestCase2 {
	
	/**
	 * 
	 */
	public TestBinaryCompatibility() {
	}

	/**
	 * @param name
	 */
	public TestBinaryCompatibility(String name) {
		super(name);
	}

	/**
	 * @todo munge the release version into a name that is compatibility with
	 *       the file system ("." to "_"). Store artifacts at each release? At
	 *       each release in which an incompatibility is introduced? At each
	 *       release in which a persistence capable data structure or change is
	 *       introduced?
	 */
	static protected final File artifactDir = new File(
			"bigdata-compatibility/src/resources/artifacts");
	
	protected static class Version  {
		private final String version;
		private final String revision;
		public Version(String version,String revision) {
			this.version = version;
			this.revision = revision;
		}

		/**
		 * The bigdata version number associated with the release. This is in
		 * the form <code>xx.yy.zz</code>
		 */
		public String getVersion() {
			return version;
		}

		/**
		 * The SVN repository revision associated with the release. This is in
		 * the form <code>####</code>.
		 */
		public String getRevision() {
			return revision;
		}
	}

	/**
	 * Known release versions.
	 */
	protected static Version V_0_83_2 = new Version("0.83.2", "3349");

	/**
	 * Tested Versions.
	 */
	protected Version[] versions = new Version[] {
			V_0_83_2
	};
	
	protected void setUp() throws Exception {

		Banner.banner();
		
		super.setUp();
		
		if (!artifactDir.exists()) {

			if (!artifactDir.mkdirs()) {
			
				throw new IOException("Could not create: " + artifactDir);
				
			}

		}
		
		for (Version version : versions) {

			final File versionDir = new File(artifactDir, version.getVersion());
			
			if (!versionDir.exists()) {

				if (!versionDir.mkdirs()) {

					throw new IOException("Could not create: " + versionDir);

				}

			}

		}
		
	}

	protected void tearDown() throws Exception {

		super.tearDown();
		
	}

	/**
	 * @throws Throwable 
	 * 
	 * @todo Each 'test' should run an instance of a class which knows how to
	 *       create the appropriate artifacts and how to test them.
	 */
	public void test_WORM_compatibility_with_JOURNAL_HA_BRANCH()
			throws Throwable {

		final Version version = V_0_83_2;

		final File versionDir = new File(artifactDir, version.getVersion());

		final File artifactFile = new File(versionDir, getName()
				+ BufferMode.DiskWORM + Journal.Options.JNL);

		if (!artifactFile.exists()) {

			createArtifact(artifactFile);

		}

		verifyArtifact(artifactFile);

	}

	protected void createArtifact(final File artifactFile) throws Throwable {

		if (log.isInfoEnabled())
			log.info("Creating: " + artifactFile);

		final Properties properties = new Properties();

		properties.setProperty(Journal.Options.FILE, artifactFile.toString());

		properties.setProperty(Journal.Options.INITIAL_EXTENT, ""
				+ Journal.Options.minimumInitialExtent);

		final Journal journal = new Journal(properties);

		try {

			final IndexMetadata md = new IndexMetadata(UUID.randomUUID());
			
			final IIndex ndx = journal.registerIndex("kb.spo.SPO", md);
			
			ndx.insert(1,1);
			
			journal.commit();
			
			// reduce to minimum footprint.
			journal.truncate();

		} catch (Throwable t) {

			journal.destroy();
			
			throw new RuntimeException(t);
			
		} finally {
			
			if (journal.isOpen())
				journal.close();
			
		}

	}

	protected void verifyArtifact(final File artifactFile) throws Throwable {

		if (log.isInfoEnabled())
			log.info("Verifying: " + artifactFile);

		final Properties properties = new Properties();

		properties.setProperty(Journal.Options.FILE, artifactFile.toString());

		final Journal journal = new Journal(properties);

		try {

			final IIndex ndx = journal.getIndex("kb.spo.SPO");
			
			assertNotNull(ndx);
			
			assertEquals(1,ndx.lookup(1));
			
		} finally {

			journal.close();
			
		}

	}

}
