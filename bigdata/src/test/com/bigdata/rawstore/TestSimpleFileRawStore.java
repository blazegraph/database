/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Jan 31, 2007
 */

package com.bigdata.rawstore;

import java.io.File;
import java.io.IOException;


/**
 * Test suite for {@link SimpleFileRawStore}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestSimpleFileRawStore extends AbstractRawStoreTestCase {

    /**
     * 
     */
    public TestSimpleFileRawStore() {
    }

    /**
     * @param name
     */
    public TestSimpleFileRawStore(String name) {
        super(name);
    }

    private boolean firstTime = true;
    private SimpleFileRawStore store;
    
    protected IRawStore getStore() {

        File file = new File(getName()+".raw2");

        if(firstTime && file.exists() && !file.delete()) {

            throw new RuntimeException("Could not delete existing file: "
                    + file.getAbsoluteFile());
            
        }
                
        firstTime = false;
        
        try {
        
            store = new SimpleFileRawStore(file,"rw");
            
            return store;
            
        }
        catch(IOException ex) {
            
            throw new RuntimeException(ex);
            
        }
        
    }
    
    public void tearDown() throws Exception {
        
        super.tearDown();
        
        if(store != null && store.isOpen()) {
            
            // force release of any file handle.
            store.raf.close();
            
        }
        
    }

}
