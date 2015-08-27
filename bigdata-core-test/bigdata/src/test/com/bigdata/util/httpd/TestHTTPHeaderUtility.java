/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/

package com.bigdata.util.httpd;

import java.util.regex.Pattern;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.bigdata.util.httpd.HTTPHeaderUtility;

/**
 * Test suite for {@link HTTPHeaderUtility}.
 *
 * @todo Add explicit tests for the {@link Pattern} objects used to
 * match an HTTP <code>token</code> and HTTP
 * <code>quoted-string</code>.  These tests should ensure that the
 * pattern objects provide no more or less coverage than they should
 * of the corresponding character classes.
 */

public class TestHTTPHeaderUtility
    extends TestCase
{

    public static Test suite()
    {

        TestSuite suite = new TestSuite();

        suite.addTestSuite
	    ( TestHTTPHeaderUtility.class
	      );

        return suite;

    }

    /**
     * @todo Implement tests.
     */
    public void testFoo()
    {
    }

}
