/**
Copyright (C) SYSTAP, LLC 2013.  All rights reserved.

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

package com.bigdata.rdf.sail.webapp;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Enumeration;
import java.util.regex.Pattern;

import junit.extensions.TestSetup;
import junit.extensions.proxy.ProxyTestSuite;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

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
		
		public MultiModeTestSuite(String name, TestMode ...modes ) {
			super(name);
			subs = new ProxyTestSuite[modes.length];
			int i = 0;
			for (final TestMode mode: modes) {
				final ProxyTestSuite suite2 = TestNanoSparqlServerWithProxyIndexManager.createProxyTestSuite(TestNanoSparqlServerWithProxyIndexManager.getTemporaryJournal(),mode);
				super.addTest(new TestSetup(suite2) {
		    		protected void setUp() throws Exception {
		    		}
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
				suite2.setName(mode.name());
				subs[i++] = suite2;
			}
		}

		@SuppressWarnings("rawtypes")
		@Override
		public void addTestSuite(Class clazz) {
			for (final ProxyTestSuite s:subs) {
				s.addTestSuite(clazz);
			}
		}

		@Override
		public void addTest(Test test) {
			for (final ProxyTestSuite s:subs) {
				s.addTest(cloneTest(s.getDelegate(),test));
			}
		}
	}

	private static Test cloneTest(Test delegate, Test test) {
		if (test instanceof TestSuite) {
			return cloneSuite(delegate, (TestSuite)test);
		}
		if (test instanceof TestCase) {
			return cloneTestCase((TestCase)test);
		}
		throw new IllegalArgumentException("Cannot handle test of type: "+test.getClass().getName());
	}


	private static Test cloneTestCase(TestCase test) {
		return createTest(test.getClass(),test.getName());
	}

	private static Test cloneSuite(Test delegate, TestSuite suite) {
		final TestSuite rslt =  new CloningTestSuite(delegate,suite.getName());
		@SuppressWarnings("unchecked")
		final
		Enumeration<Test> enumerate = suite.tests();
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
	 * @param modes  One or more TestModes.
	 * @return
	 */
	public static Test suiteWhenStandalone(Class<? extends TestCase> clazz, String regex, TestMode ... modes) {
		if (!proxyIndexManagerTestingHasStarted) {
			final Pattern pat = Pattern.compile(regex);
			proxyIndexManagerTestingHasStarted = true;
			final TestSuite suite = new MultiModeTestSuite(clazz.getName(),modes);
			addMatchingTestsFromClass(suite, clazz, pat);
			return suite;
		} else {
			return new TestSuite(clazz);
		}
	}

	/**
	 * Call this method to create a new test suite which can include
	 * other test suites and tests using proxies.
	 * Having created the test suite then the classes and tests and suites
	 * are added in the usual way.
	 * @param modes  One or more TestModes.
	 * @return
	 */
	public static TestSuite suiteWithOptionalProxy(String name, TestMode ... mode) {
		if (!proxyIndexManagerTestingHasStarted) {
			proxyIndexManagerTestingHasStarted = true;
			return new MultiModeTestSuite(name,mode);
		} else {
			return new TestSuite(name);
		}
	}

	private static void addMatchingTestsFromClass(TestSuite suite3, Class<? extends TestCase> clazz, Pattern pat) {
		for (final Method m:clazz.getMethods()) {
			if ( m.getParameterTypes().length==0 && pat.matcher(m.getName()).matches() ) {
				suite3.addTest(createTest(clazz,m.getName()));
			}
		}
	}

	private static Test createTest(Class<? extends TestCase> clazz, String name) {
		try {
			@SuppressWarnings("unchecked")
			final
			Constructor<? extends TestCase> cons = TestSuite.getTestConstructor(clazz);
			if (cons.getParameterTypes().length == 1) {
				return cons.newInstance(name);
			} else {
				final TestCase test = cons.newInstance();
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
