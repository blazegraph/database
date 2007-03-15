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
 * Created on Mar 13, 2007
 */

package com.bigdata.scaleup;

import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IJournal;

/**
 * An interface for managing global definitions of scalable indices including
 * index partitions. In a scale-out architecture, this interface is realized as
 * a robust service with failover. The client protocol for this service should
 * use pre-fetch and leases to reduce latency and enable clients to talk
 * directly with data services to the greatest extent possible. During the term
 * of a lease, a client may safely presume that any index partition definitions
 * covered by that lease are valid and therefore may issue operations directly
 * to the appropriate data service instances. However, once a lease has expired
 * a client must verify that index partition definitions before issuing
 * operations against those partitions. The duration of a lease is managed so as
 * to balance the time between the completion of an index build task, at which
 * point the index partition definitions are updated and can be immediately used
 * by new clients, and the release of the inputs to the index build task (at
 * which point the old index partition definition is no longer useable).
 * <p>
 * Note that this interface extends the semantics of {@link IIndexManager} to
 * provide a global registration or removal of an index while the latter
 * operations in a purely local manner on an {@link IJournal}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IMetadataIndexManager extends IIndexManager {

    /*
     * Note: IIndexManager is implemented by AbstractJournal but the semantics
     * of this implementation are purely local -- indices are registered on or
     * dropped from the specific journal instance only not the distributed
     * database. An IIndexManager service is responsible for a distinct global
     * registration of an index and for managing the mapping of the index onto
     * journals and index segments via the metadata index.
     */
    
}
