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
/*
 * Created on Feb 1, 2006
 */
package com.bigdata.io;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;

import junit.framework.TestCase;

/**
 * Test suite for {@link NameAndExtensionFilter}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestNameAndExtensionFilter extends TestCase
{

    /**
     * 
     */
    public TestNameAndExtensionFilter() {
        super();
    }

    /**
     * @param name
     */
    public TestNameAndExtensionFilter(final String name) {
        super(name);
    }

    /**
     * Verifies that the same files are present in each {@link File}[]. The
     * order in which the files are listed does not matter.
     * 
     * @param expected
     * @param actual
     */
    private void assertSameFiles(final File[] expected, final File[] actual) {

        if (expected == null) {

            throw new AssertionError("expected is null.");

        }

        if (actual == null) {

            fail("actual is null.");

        }

        assertEquals("#of files", expected.length, actual.length);

        // Insert the expected files into a set.
        final Set<String> expectedSet = new HashSet<String>();

        for (int i = 0; i < expected.length; i++) {

            final File expectedFile = expected[i];

            if (expectedFile == null) {

                throw new AssertionError("expected file is null at index=" + i);

            }

            if (!expectedSet.add(expectedFile.toString())) {

                throw new AssertionError(
                        "expected File[] contains duplicate: expected[" + i
                                + "]=" + expectedFile);

            }

        }

        /*
         * Verify that each actual file occurs in the expectedSet using a
         * selection without replacement policy.
         */

        for (int i = 0; i < actual.length; i++) {

            final File actualFile = actual[i];

            if (actualFile == null) {

                fail("actual file is null at index=" + i);

            }

            if (!expectedSet.remove(actual[i].toString())) {

                fail("actual file=" + actualFile + " at index=" + i
                        + " was not found in expected files.");

            }

        }

    }

    /**
     * Test verifies that no files are found using a guarenteed unique basename.
     */
    public void test_filter_001() throws IOException {

        final File basefile = File.createTempFile(getName(), "-test");

        try {

            final String basename = basefile.toString();

            final NameAndExtensionFilter logFilter = new NameAndExtensionFilter(
                    basename, ".log");

            assertSameFiles(new File[] {}, logFilter.getFiles());

        } finally {

            basefile.delete();

        }

    }

    /**
     * Test verifies that N files are found using a guarenteed unique basename.
     */
    public void test_filter_002() throws IOException {

        final int N = 100;

        final Vector<File> v = new Vector<File>(N);

        final File logBaseFile = File.createTempFile(getName(), "-test");
        // logBaseFile.deleteOnExit();

        try {

            final String basename = logBaseFile.toString();
            // System.err.println( "basename="+basename );

            final NameAndExtensionFilter logFilter = new NameAndExtensionFilter(
                    basename, ".log");

            for (int i = 0; i < N; i++) {

                final File logFile = new File(basename + "." + i + ".log");
                // logFile.deleteOnExit();
                logFile.createNewFile();
                // System.err.println( "logFile="+logFile );

                v.add(logFile);

            }

            final File[] expectedFiles = (File[]) v.toArray(new File[] {});

            assertSameFiles(expectedFiles, logFilter.getFiles());

        } finally {

            logBaseFile.delete();

            for (File f : v) {

                f.delete();

            }

        }
        
    }

}
