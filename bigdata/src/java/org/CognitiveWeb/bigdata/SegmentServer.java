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
 * Created on Jun 18, 2006
 */
package org.CognitiveWeb.bigdata;

import java.io.IOException;
import java.util.Properties;

import org.CognitiveWeb.dbcache.DBCache;

/**
 * Class exposes a dbCache instance containing a segment.
 * 
 * @todo Factor the transaction logic out of dbCache so that we can operate with
 *       distributed transactions.
 * 
 * @todo Add dbCache option where the modified page replaces the unmodified page
 *       in the page cache. This option is consistent with the original
 *       forumulation of dbCache and with the use of per-segment locking.
 * 
 * @todo Implement methods to read/write on the dbCache.
 * 
 * @todo Support two-phase commit protocol (prepare message). This is just
 *       writing all dirty pages in the cache on the safe but waiting until we
 *       get the commit message to update the safe begin record.
 * 
 * @version $Id$
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson
 *         </a>
 */
public class SegmentServer implements ISegmentServer
{
    
    private final long _segmentId;
    private final DBCache _dbCache;

    public static class ProvisioningOptions extends DBCache.ProvisioningOptions {
        
        /**
         * The name of the required property whose value is an object identifier
         * whose partition and segment components identify the segment. The page
         * and slot components are ignored and may be zero (0).
         */
        public static final String SEGMENT_ID = "bigdata.segmentServer.segmentId";
        
    }
    
    public static class RuntimeOptions extends DBCache.RuntimeOptions {
        
    }
    
    /**
     * @param properties
     * @throws IOException
     */
    public SegmentServer( Properties properties ) throws IOException {
        
        String value;
        
        if( properties == null ) {
            throw new IllegalArgumentException();
        }
        
        value = properties.getProperty(ProvisioningOptions.SEGMENT_ID);
        if( value == null ) {
            throw new RuntimeException("Required property not specified: "
                    + ProvisioningOptions.SEGMENT_ID);
        }
        _segmentId = new OId( Long.parseLong( value ) ).maskPageAndSlot();
        
        
        _dbCache = DBCache.open( properties );
        
    }

    //
    // IPageServer
    //
    
    /**
     * This operation is not supported. Locks should be obtained before
     * accessing a segment.
     */
    public void lock(long tx,OId oid, boolean readOnly,long timeout) {
        throw new UnsupportedOperationException();
    }

    public void write(long tx, OId pageId, byte[] data ) throws IOException {
        throw new UnsupportedOperationException();
    }
    
    public byte[] read(long tx, OId pageId ) throws IOException {
        throw new UnsupportedOperationException();
    }

    public void prepare(long tx) {
        throw new UnsupportedOperationException();
    }
    
    public void commit(long tx, boolean releaseLocks, boolean syncToDisk) {
        throw new UnsupportedOperationException();
    }
    
    public void abort(long tx) {
        throw new UnsupportedOperationException();
    }

    //
    // ISegmentServer
    //
    /**
     * The segment identifier. The page and slot components will be zero.
     * 
     * @return The segment identifier.
     */
    public long getSegementId() {
        return _segmentId;
    }
    
    /**
     * The size of a page, which is fixed for all pages in all copies of a given
     * segment.
     * 
     * @return The size of a page.
     */
    public int getPageSize() {
        return _dbCache.getPageSize();
    }
    
    /**
     * The #of allocated pages in the segment. The #of allocated but unused
     * pages in the segment may be computed as
     * <code>getPageCount() - getIntUsePageCount()</code>.
     * 
     * @return The #of allocated pages.
     */
    public long getPageCount() {
        return _dbCache.getPageCount();
    }
    
    /**
     * The #of in use pages in the segment.
     * 
     * @return The #of in use pages.
     * 
     * @todo This is only defined by the dbCache application.
     */
    public long getInUsePageCount() {
        throw new UnsupportedOperationException();
    }

    /**
     * Close the segment.
     */
    public void close() {
        try {
            _dbCache.close();
        }
        catch( IOException ex ) {
            throw new RuntimeException( ex );
        }
    }
    
    /**
     * The {@link DBCache} instance.
     */
    public Object getStorageLayer() {
        return _dbCache;
    }

}
