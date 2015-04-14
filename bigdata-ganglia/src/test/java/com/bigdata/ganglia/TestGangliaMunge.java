/*
   Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package com.bigdata.ganglia;

import com.bigdata.ganglia.GangliaMunge;

import junit.framework.TestCase;

/**
 * Unit tests for ability to munge a metric name into something compatible with
 * a file system (ganglia stores metrics in the file system and does not do a
 * good job of munging the metrics itself).
 */
public class TestGangliaMunge extends TestCase {

	public TestGangliaMunge() {
	}

	public TestGangliaMunge(String name) {
		super(name);
	}

	public void test_munge01() {

		assertEquals("test-context", GangliaMunge.munge("test-context"));

	}

	public void test_munge02() {

		assertEquals("test.context", GangliaMunge.munge("test.context"));
		
	}

	/**
	 * Note: The ganglia UI messes up when there is whitespace in a metric name.
	 */
	public void test_munge03() {
		
		assertEquals("test_context", GangliaMunge.munge("test context"));
		
	}

	public void test_munge04() {

		assertEquals("test_Percent", GangliaMunge.munge("test%"));

	}

	public void test_munge05() {

		assertEquals("Percent_test", GangliaMunge.munge("%test"));

	}

	public void test_munge06() {

		assertEquals("test-context-3.math.range.ab_x.c-d",
				GangliaMunge.munge("test-context-3.math.range.ab x.c-d"));

	}

}
