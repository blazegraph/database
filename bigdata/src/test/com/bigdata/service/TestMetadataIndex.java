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
 * Created on Mar 4, 2008
 */

package com.bigdata.service;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.bigdata.btree.IndexMetadata;
import com.bigdata.mdi.MetadataIndex;

/**
 * Some unit tests for the {@link MetadataIndex} as accessed via the
 * {@link IMetadataService}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestMetadataIndex extends
        AbstractEmbeddedFederationTestCase {

    /**
     * 
     */
    public TestMetadataIndex() {
        super();
    }

    /**
     * @param arg0
     */
    public TestMetadataIndex(String arg0) {
        super(arg0);
    }

    /**
     * Verify that the first index partition is assigned partitionId zero (0)
     * and that subsequent calls to
     * {@link IMetadataService#nextPartitionId(String)} return strictly
     * increasing partition identifiers.
     * 
     * @throws IOException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_nextPartitionId() throws IOException, InterruptedException, ExecutionException {
        
        final String name = "test";
        
        {
            
            IndexMetadata indexMetadata = new IndexMetadata(name,UUID.randomUUID());
        
            indexMetadata.setDeleteMarkers(true);
            
            fed.registerIndex(indexMetadata);
        
        }
        
        assertEquals(1,fed.getMetadataService().nextPartitionId(name));
        
        assertEquals(2,fed.getMetadataService().nextPartitionId(name));
        
    }
    
}
