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

package com.bigdata.search;

import java.io.StringReader;

import com.bigdata.service.AbstractLocalDataServiceFederationTestCase;

/**
 * Test suite for full text indexing and search.
 * 
 * @todo test language code support
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestFullTextIndex extends AbstractLocalDataServiceFederationTestCase {

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
    
    public void test_simple() throws InterruptedException {
        
        FullTextIndex ndx = new FullTextIndex(client,"test");
        
        final long docId = 12L;
        
        final int fieldId = 3;
        
        final String text = "The quick brown dog";
        
        final String languageCode = "EN";
        
        TokenBuffer buffer = new TokenBuffer(2,ndx);
        
        ndx.index(buffer, docId, fieldId, languageCode, new StringReader(text));

        ndx.index(buffer, docId+1, fieldId, languageCode, new StringReader("The slow brown cow"));

        buffer.flush();
        
        Hiterator itr = ndx.search(text,languageCode);
        
        assertEquals(2,itr.size());
        
        assertTrue(itr.hasNext());
        
        IHit hit1 = itr.next();

        System.err.println("hit1:"+hit1);
        
        assertTrue(itr.hasNext());
        
        IHit hit2 = itr.next();

        System.err.println("hit2:"+hit2);
        
    }
    
}
