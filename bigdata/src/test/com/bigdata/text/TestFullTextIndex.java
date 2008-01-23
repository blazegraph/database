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
 * Created on Jan 23, 2008
 */

package com.bigdata.text;

import junit.framework.TestCase;

/**
 * Test suite for full text indexing and search.
 * 
 * @todo test restart safety of the full text index.
 * 
 * @todo some sort of benchmarking. b+trees are not the normal means to realize
 *       search. how much of a performance penalty is there for this approach?
 *       If it is too high then there will need to be a looser integration with
 *       either lucene or mg4j.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestFullTextIndex extends TestCase {

    /**
     * 
     */
    public TestFullTextIndex() {
    }

    /**
     * @param arg0
     */
    public TestFullTextIndex(String arg0) {
        super(arg0);
    }

}
