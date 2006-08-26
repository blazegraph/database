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
 * Created on Jun 19, 2006
 */
package org.CognitiveWeb.bigdata;

/**
 * <p>
 * A segment server is an interface to a random access file with support for an
 * atomic commit protocol. An implementation of this interface is generally a
 * lightweight wrapper around a specific store component supporting a 2 or
 * 3-phase commit protocol.
 * </p>
 * 
 * @version $Id$
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson
 *         </a>
 */

public interface ISegmentServer extends IPageServer {

    /**
     * The segment identifier. The page and slot components will be zero.
     * 
     * @return The segment identifier.
     */
    public long getSegementId();
    
    /**
     * The size of a page, which is fixed for all pages in all copies of a given
     * segment.
     * 
     * @return The size of a page.
     */
    public int getPageSize();
    
    /**
     * The #of allocated pages in the segment. The #of allocated but unused
     * pages in the segment may be computed as
     * <code>getPageCount() - getIntUsePageCount()</code>.
     * 
     * @return The #of allocated pages.
     */
    public long getPageCount();
    
    /**
     * The #of in use pages in the segment.
     * 
     * @return The #of in use pages.
     */
    public long getInUsePageCount();

    /**
     * Close the segment.
     */
    public void close();
    
    /**
     * The storage layer implementation object. You can cast this to the
     * implementation object in order to access additional information about and
     * operations on the storage layer.
     * 
     * @return
     */
    public Object getStorageLayer();
    
}
