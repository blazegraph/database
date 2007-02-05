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
 * Created on Jan 31, 2007
 */

package com.bigdata.rawstore;

import java.io.File;
import java.io.IOException;


/**
 * Test suite for {@link SimpleFileRawStore2}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestSimpleFileRawStore2 extends AbstractRawStore2TestCase {

    /**
     * 
     */
    public TestSimpleFileRawStore2() {
    }

    /**
     * @param name
     */
    public TestSimpleFileRawStore2(String name) {
        super(name);
    }

    private boolean firstTime = true;
    private SimpleFileRawStore2 store;
    
    protected IRawStore getStore() {

        File file = new File(getName()+".raw2");

        if(firstTime && file.exists() && !file.delete()) {

            throw new RuntimeException("Could not delete existing file: "
                    + file.getAbsoluteFile());
            
        }
                
        firstTime = false;
        
        try {
        
            store = new SimpleFileRawStore2(file,"rw");
            
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
