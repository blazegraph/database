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
/*
 * Created on Nov 20, 2006
 */

package com.bigdata.objndx;


/**
 * FIXME write tests for {@link DefaultLeafSplitPolicy}. Those tests should
 * verify the math using simple examples and also assess the impact on the
 * utilization in leaves under "near sequential" and "random but asymptotic to
 * dense sequential" key insertion scenariors.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestLeafSplitPolicy extends AbstractBTreeTestCase {

    /**
     * 
     */
    public TestLeafSplitPolicy() {
    }

    /**
     * @param name
     */
    public TestLeafSplitPolicy(String name) {
        super(name);
    }
    
    public void test_simpleSplitRule01() {
        
        ILeafSplitPolicy splitter = SimpleLeafSplitPolicy.INSTANCE;
        
        /*
         * @todo Create a btree with a branching factor.
         * 
         * @todo populate the root leaf with known keys.
         * 
         * @todo verify the computed split index.
         */
        
    }

    public void test_defaultSplitRule01() {
        
        ILeafSplitPolicy splitter = DefaultLeafSplitPolicy.INSTANCE;
        
        /*
         * @todo Create a btree with a branching factor.
         * 
         * @todo populate the root leaf with known keys.  choose examples that
         * are dense and that are sparse.
         * 
         * @todo verify the computed split index.
         */
        
    }

    /**
     * @todo compare utilization with the simple and default leaf split rules
     *       for sequential key insertion, near sequential key insertion, and a
     *       pattern that reflects the expected assignment of persistent
     *       identifiers for bigdata read-optimized database pages.
     * 
     */
    public void test_defaultSplitRule02() {
    
    }
    
}
