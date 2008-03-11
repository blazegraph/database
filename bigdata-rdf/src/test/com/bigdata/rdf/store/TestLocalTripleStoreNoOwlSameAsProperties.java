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
 * Created on Oct 18, 2007
 */

package com.bigdata.rdf.store;

import java.util.Properties;

import com.bigdata.btree.BTree;
import com.bigdata.rdf.inf.InferenceEngine;

/**
 * Proxy test suite for {@link LocalTripleStore} when the backing indices are
 * {@link BTree}s. This configuration does NOT support transactions since the
 * various indices are NOT isolatable. This one turns off forward
 * chaining for owl:sameAs rules {2,3}.
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class TestLocalTripleStoreNoOwlSameAsProperties extends TestLocalTripleStore {

    /**
     * 
     */
    public TestLocalTripleStoreNoOwlSameAsProperties() {
    }

    public TestLocalTripleStoreNoOwlSameAsProperties(String name) {
        super(name);
    }
    
    /**
     * Properties for tests in this file and this proxy suite (if any).
     */
    public Properties getProperties() {

        Properties properties = super.getProperties();
        
        properties.setProperty(InferenceEngine.Options.FORWARD_CHAIN_OWL_SAMEAS_PROPERTIES,"false");

        return properties;

    }
    
}
