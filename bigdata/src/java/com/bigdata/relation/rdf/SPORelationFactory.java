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
 * Created on Jun 30, 2008
 */

package com.bigdata.relation.rdf;

import java.util.concurrent.ExecutorService;

import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.Journal;
import com.bigdata.journal.TemporaryStore;
import com.bigdata.relation.AbstractRelationFactory;
import com.bigdata.relation.IRelation;
import com.bigdata.service.IBigdataFederation;

/**
 * @todo move the cache into the factory? (from the locators).
 * 
 * @todo add init() to set additional properties or Properties ctor for the
 *       factory?
 *       
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SPORelationFactory extends AbstractRelationFactory<SPO> {

    /**
     * 
     */
    private static final long serialVersionUID = 7265961777132687319L;

    /**
     * De-serialization ctor.
     */
    public SPORelationFactory() {
        
    }
    
    /*
     * Strengthened return types.
     */
    
    public SPORelation newRelation(ExecutorService service, Journal journal,
            String namespace, long timestamp) {

        return (SPORelation) super.newRelation(service, journal, namespace,
                timestamp);

    }

    public SPORelation newRelation(ExecutorService service,
            TemporaryStore tempStore, String namespace) {

        return (SPORelation) super.newRelation(service, tempStore, namespace);

    }

    public SPORelation newRelation(IBigdataFederation fed, String namespace,
            long timestamp) {

        return (SPORelation) super.newRelation(fed, namespace, timestamp);

    }

    public SPORelation newRelation(ExecutorService service, AbstractTask task,
            String namespace) {

        return (SPORelation) super.newRelation(service, task, namespace);

    }

    @Override
    protected Class<? extends IRelation<SPO>> getRelationClass() {

        return SPORelation.class;
        
    }

}
