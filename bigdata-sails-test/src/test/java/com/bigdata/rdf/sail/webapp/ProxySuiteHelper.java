/**
Copyright (C) SYSTAP, LLC DBA Blazegraph 2013.  All rights reserved.

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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Set;
import java.util.regex.Pattern;

import junit.extensions.TestSetup;
import junit.extensions.proxy.ProxyTestSuite;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.bigdata.journal.BufferMode;

/**
 * This class provides static methods to help creating
 * test classes and suites of tests that use the proxy test
 * approach. For creating test classes use {@link #suiteWhenStandalone(Class, String, TestMode...)},
 * when creating test suites use {@link #suiteWithOptionalProxy(String, TestMode...)}
 * <p>
 * The intent is to enable the developer in eclipse to run JUnit tests
 * from a test file or a test suite file, while still allowing that same file
 * to be included unchanged in the main test suite. The methods defined here
 * hence provide a default behavior in the case that the {@link TestNanoSparqlServerWithProxyIndexManager}
 * has already loaded before this class.
 * @author jeremycarroll
 *
 */
public class ProxySuiteHelper {

	private static class CloningTestSuite extends ProxyTestSuite  {

		public CloningTestSuite(Test delegate, String name) {
			super(delegate, name);
		}

		@Override
		public void addTest(Test test) {
			super.addTest(cloneTest(getDelegate(),test));
		}
	}

	private static class MultiModeTestSuite extends TestSuite  {
		private final ProxyTestSuite subs[];
		
		public MultiModeTestSuite(final String name, final Set<BufferMode> bufferModes, final TestMode ...testModes ) {
			super(name);
			if (bufferModes.isEmpty())
				throw new IllegalArgumentException();
         final int ntests = testModes.length * bufferModes.size();
			subs = new ProxyTestSuite[ntests];
			int i = 0;
			for( final BufferMode bufferMode : bufferModes) {
				for (final TestMode testMode: testModes) {
					final ProxyTestSuite suite2 = TestNanoSparqlServerWithProxyIndexManager.createProxyTestSuite(TestNanoSparqlServerWithProxyIndexManager.getTemporaryJournal(bufferMode),testMode);
					super.addTest(new TestSetup(suite2) {
						@Override
			    		protected void setUp() throws Exception {
			    		}
						@SuppressWarnings("rawtypes")
						@Override
			    		protected void tearDown() throws Exception {
			    			((TestNanoSparqlServerWithProxyIndexManager)suite2.getDelegate()).tearDownAfterSuite();
	                        /*
	                         * Note: Do not clear. Will not leak unless the
	                         * QueryEngine objects are pinned. They will not be
	                         * pinned if you shutdown the Journal correctly for each
	                         * test; the call to tearDownAfterSuite above calls the destroy() method
	                         * on temporary journals, which appears to do the necessary thing.
	                         */
	    //		    			QueryEngineFactory.clearStandAloneQECacheDuringTesting();
			    		}
			    	});
//               suite2.setName(name + ", bufferMode=" + bufferMode.name()
//                     + ", testMode=" + testMode.name());
					subs[i++] = suite2;
				}
			}
		}

		@SuppressWarnings("rawtypes")
		@Override
		public void addTestSuite(final Class clazz) {
			for (final ProxyTestSuite s:subs) {
				s.addTestSuite(clazz);
			}
		}

		@Override
		public void addTest(final Test test) {
			for (final ProxyTestSuite s:subs) {
				s.addTest(cloneTest(s.getDelegate(),test));
			}
		}
	}

	private static Test cloneTest(final Test delegate, final Test test) {
		if (test instanceof TestSuite) {
			return cloneSuite(delegate, (TestSuite)test);
		}
		if (test instanceof TestCase) {
			return cloneTestCase((TestCase)test);
		}
		throw new IllegalArgumentException("Cannot handle test of type: "+test.getClass().getName());
	}


	private static Test cloneTestCase(final TestCase test) {
		return createTest(test.getClass(),test.getName());
	}

	private static Test cloneSuite(final Test delegate, final TestSuite suite) {
		final TestSuite rslt =  new CloningTestSuite(delegate,suite.getName());
		final Enumeration<Test> enumerate = suite.tests();
		while( enumerate.hasMoreElements() ) {
			rslt.addTest(enumerate.nextElement());
		}
		return rslt;
	}

	/**
	 * This variable tells us if the class {@link TestNanoSparqlServerWithProxyIndexManager}
	 * (or potentially a similar class that sets this variable)
	 * has loaded. This information is used by {@link #suiteWhenStandalone(Class, String, TestMode...)}
	 * to decide whether to operate in stand-alone fashion, or to default to participating
	 * in a larger test suite, managed by the proxy.
	 */
	static boolean proxyIndexManagerTestingHasStarted = false;

	/**
	 * Call this method to create local testing using one or more proxies.
	 * e.g. right clicking in eclipse and running JUnit tests works.
	 * Also using this within a TestSuite also works.
	 * 
	 * 
	 * @param clazz  The clazz to be tested, i.e. the calling class
	 * @param regex  Matched against the test names to decide which tests to run. Should usually start in "test.*"
	 * @param bufferMode The {@link BufferMode}(s) to be tested. 
	 * @param testModes  One or more TestModes.
	 * @return
	 */
	public static TestSuite suiteWhenStandalone(final Class<? extends TestCase> clazz, final String regex, final Set<BufferMode> bufferModes, final TestMode ... testModes) {
		if (!proxyIndexManagerTestingHasStarted) {
			final Pattern pat = Pattern.compile(regex);
			proxyIndexManagerTestingHasStarted = true;
			final TestSuite suite = new MultiModeTestSuite(clazz.getName(),bufferModes, testModes);
			addMatchingTestsFromClass(suite, clazz, pat);
			return suite;
		} else {
			return new TestSuite(clazz);
		}
	}

	public static Test suiteWhenStandalone(final Class<? extends TestCase> clazz, final String regex, final TestMode ... testModes) {
		return suiteWhenStandalone(clazz, regex, Collections.singleton(BufferMode.Transient), testModes);
	}

	/**
	 * Call this method to create a new test suite which can include
	 * other test suites and tests using proxies.
	 * Having created the test suite then the classes and tests and suites
	 * are added in the usual way.
	 * @param modes  One or more TestModes.
	 * @return
	 */
	public static TestSuite suiteWithOptionalProxy(final String name, final Set<BufferMode> bufferModes, final TestMode ... testMode) {
		if (!proxyIndexManagerTestingHasStarted) {
			proxyIndexManagerTestingHasStarted = true;
			return new MultiModeTestSuite(name,bufferModes,testMode);
		} else {
			return new TestSuite(name);
		}
	}
	public static TestSuite suiteWithOptionalProxy(final String name, final TestMode ... testMode) {
		return suiteWithOptionalProxy(name,Collections.singleton(BufferMode.Transient),testMode);
	}

	private static void addMatchingTestsFromClass(final TestSuite suite3, final Class<? extends TestCase> clazz, final Pattern pat) {
		for (final Method m:clazz.getMethods()) {
			if ( m.getParameterTypes().length==0 && pat.matcher(m.getName()).matches() ) {
				suite3.addTest(createTest(clazz,m.getName()));
			}
		}
	}

	private static Test createTest(final Class<? extends TestCase> clazz, final String name) {
		try {
			final Constructor<?> cons = TestSuite.getTestConstructor(clazz);
			if (cons.getParameterTypes().length == 1) {
				return (Test) cons.newInstance(name);
			} else {
				final TestCase test = (TestCase) cons.newInstance();
				test.setName(name);
				return test;
			}
		}  catch (final NoSuchMethodException e) {
			throw new RuntimeException("Failed to find constructor");
		} catch (final InstantiationException e) {
			throw new RuntimeException(e);
		} catch (final IllegalAccessException e) {
			throw new RuntimeException(e);
		} catch (final IllegalArgumentException e) {
			throw new RuntimeException(e);
		} catch (final InvocationTargetException e) {
			throw new RuntimeException(e);
		}
	}
}
