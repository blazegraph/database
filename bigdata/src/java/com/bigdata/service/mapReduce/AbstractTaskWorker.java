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
 * Created on Sep 22, 2007
 */

package com.bigdata.service.mapReduce;

import java.util.UUID;
import java.util.concurrent.Callable;

import com.bigdata.service.IBigdataClient;

/**
 * Abstract base class for task workers running in the {@link MapService} or the
 * {@link ReduceService}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractTaskWorker implements Callable<Object> {

    /**
     * Used to access the intermediate store for the map/reduce operation.
     */
    protected final IBigdataClient client;
    
    /**
     * The task identifier.
     */
    protected final UUID uuid;
    
    public AbstractTaskWorker(IBigdataClient client, UUID uuid) {

        if (client == null)
            throw new IllegalArgumentException();

        if (uuid == null)
            throw new IllegalArgumentException();

        this.client = client;
        
        this.uuid = uuid;
        
    }
    
}
