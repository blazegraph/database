/**
 * JDBM LICENSE v1.00
 *
 * Redistribution and use of this software and associated documentation
 * ("Software"), with or without modification, are permitted provided
 * that the following conditions are met:
 *
 * 1. Redistributions of source code must retain copyright
 *    statements and notices.  Redistributions must also contain a
 *    copy of this document.
 *
 * 2. Redistributions in binary form must reproduce the
 *    above copyright notice, this list of conditions and the
 *    following disclaimer in the documentation and/or other
 *    materials provided with the distribution.
 *
 * 3. The name "JDBM" must not be used to endorse or promote
 *    products derived from this Software without prior written
 *    permission of Cees de Groot.  For written permission,
 *    please contact cg@cdegroot.com.
 *
 * 4. Products derived from this Software may not be called "JDBM"
 *    nor may "JDBM" appear in their names without prior written
 *    permission of Cees de Groot.
 *
 * 5. Due credit should be given to the JDBM Project
 *    (http://jdbm.sourceforge.net/).
 *
 * THIS SOFTWARE IS PROVIDED BY THE JDBM PROJECT AND CONTRIBUTORS
 * ``AS IS'' AND ANY EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT
 * NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL
 * CEES DE GROOT OR ANY CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * Copyright 2000 (C) Cees de Groot. All Rights Reserved.
 * Contributions are Copyright (C) 2000 by their associated contributors.
 *
 */

package com.bigdata.btree;

import java.io.IOException;
import java.util.Properties;

import com.bigdata.istore.IStore;
import com.bigdata.istore.JournalStore;
import com.bigdata.journal.ProxyTestCase;

/**
 * This class contains all Unit tests for {@link Bpage}.
 * 
 * @author <a href="mailto:boisvert@exoffice.com">Alex Boisvert</a>
 * @version $Id$
 */
public class TestBPage extends ProxyTestCase {

    public TestBPage() {
        
    }

    public TestBPage( String name ) {

        super( name );

    }

    IStore store;
    
    public void setUp() throws Exception {

        Properties properties = new Properties();
        
        properties.setProperty("bufferMode","transient");
//        properties.setProperty("segmentId","0");
        
        store = new JournalStore( properties );
        
    }

    public void tearDown() throws Exception {

        if( store.isOpen() ) {
            
            store.close();
            
        }
        
    }

    /**
     * Basic tests
     */
    public void testBasics() throws IOException {

//        final Properties properties = getProperties();
//        
//        final String filename = getTestJournalFile();
//        
//        try {
//            
//            Journal journal = new Journal(properties);

            String test, test1, test2, test3;

            test = "test";

            test1 = "test1";

            test2 = "test2";

            test3 = "test3";

            BTree tree = new BTree( store.getObjectManager(), new StringComparator(), null, null, null, 32 );

            BPage page = new BPage( tree, test, test );

            TupleBrowser browser;

            Tuple tuple = new Tuple();

            // test insertion

            page.insert( 1, test2, test2, false );

            page.insert( 1, test3, test3, false );

            page.insert( 1, test1, test1, false );

            // test binary search

            browser = page.find( 1, test2 );

            if ( browser.getNext( tuple ) == false ) {

                throw new IllegalStateException( "Browser didn't have 'test2'" );

            }

            if ( ! tuple.getKey().equals( test2 ) ) {

                throw new IllegalStateException( "Tuple key is not 'test2'" );

            }

            if ( ! tuple.getValue().equals( test2 ) ) {

                throw new IllegalStateException( "Tuple value is not 'test2'" );

            }

//            journal.close();
//        
//        } finally {
//
//            deleteTestJournalFile(filename);
//            
//        }

    }

}

