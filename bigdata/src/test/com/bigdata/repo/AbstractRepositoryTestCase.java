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
 * Created on Jan 17, 2008
 */

package com.bigdata.repo;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.util.Properties;

import com.bigdata.journal.BufferMode;
import com.bigdata.rawstore.WormAddressManager;
import com.bigdata.repo.BigdataRepository.Options;
import com.bigdata.service.AbstractEmbeddedFederationTestCase;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AbstractRepositoryTestCase extends
        AbstractEmbeddedFederationTestCase {

    /**
     * 
     */
    public AbstractRepositoryTestCase() {
    }

    /**
     * @param arg0
     */
    public AbstractRepositoryTestCase(String arg0) {
        super(arg0);
    }

    public Properties getProperties() {
        
        Properties properties = new Properties( super.getProperties() );
        
        /*
         * Note: this uses the scale-up offset bits so that the maximum block
         * size is only 4M when running the unit tests. Some of the unit tests
         * read and write full size blocks, so using a 64M block will cause
         * those tests to run out of memory without constributing any benefit to
         * "correctness" testing.
         */
        properties.setProperty(Options.OFFSET_BITS,""+WormAddressManager.SCALE_UP_OFFSET_BITS);
        
        return properties;
    
    }
    
    protected int BLOCK_SIZE;     

    protected BigdataRepository repo;
    
    public void setUp() throws Exception {

        super.setUp();

        // setup the repository
        repo = new BigdataRepository(client);
        
        BLOCK_SIZE = repo.getBlockSize();
        
        // register the indices.
        repo.registerIndices();

    }

    public void tearDown() throws Exception {

        super.tearDown();

    }

    /**
     * Read a stream into a byte[].
     * <p>
     * Note: The stream is closed as a side-effect.
     * 
     * @param is
     *            The stream.
     *            
     * @return The data read from the stream.
     * 
     * @throws IOException
     */
    protected static byte[] read( InputStream is ) throws IOException
    {

        final boolean close = true;
        
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        
        try {
            
            int i;
            
            while ( ( i = is.read() ) != -1 ) {
                
                baos.write( i );
                
            }
            
        } finally {
            
            try {
                
                if ( close ) {
                
                    is.close();
                    
                }
                
                baos.close();
                
            } catch ( Exception ex ) {
                
                log.warn("Could not close input/output stream: "+ex, ex );
                
            }
            
        }
        
        return baos.toByteArray();
        
    }
        
    /**
     * Suck the character data from the reader into a string.
     * 
     * @param reader
     * 
     * @return
     * 
     * @throws IOException
     */
    protected static String read( Reader reader ) throws IOException
    {
        
        StringWriter writer = new StringWriter();
        
        try {
            
            int i;
            
            while ( ( i = reader.read() ) != -1 ) {
                
                writer.write( i );
                
            }
            
        } finally {
            
            try {
                
                reader.close();
                
                writer.close();
                
            } catch ( Exception ex ) {
                
                log.warn( "Could not close reader/writer: "+ex, ex );
                
            }
            
        }
        
        return writer.toString();
        
    }

}
