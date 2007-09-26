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
 * Created on Mar 17, 2007
 */

package com.bigdata.service.mapReduce;

import com.bigdata.service.IBigdataClient;

/**
 * <p>
 * The master for running parallel map/reduce jobs distributed across a cluster.
 * </p>
 * 
 * @todo define main() that shares semantics with {@link EmbeddedMaster#main(String[])}
 */
public class Master extends AbstractMaster {

    public Master(MapReduceJob job, IBigdataClient client) {

        super(job, client);
        
        throw new UnsupportedOperationException();
        
    }

    /**
     * Extends {@link #setUp()} to discover map and reduce services.
     */
    protected void setUp() {

        // FIXME We need to perform service discovery for the map and reduce
        // services

        if(true) throw new UnsupportedOperationException();
        
        super.setUp();
        
    }

}
