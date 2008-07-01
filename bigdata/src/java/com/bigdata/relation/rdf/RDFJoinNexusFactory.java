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
import com.bigdata.relation.IRelationLocator;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.eval.IJoinNexusFactory;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.service.IBigdataFederation;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @deprecated Still a trial balloon.
 */
public class RDFJoinNexusFactory implements IJoinNexusFactory {

    private IRelationLocator relationLocator;
    private boolean elementOnly;
    
    /**
     * 
     * @param elementOnly
     *            <code>true</code> if only the entailed element should be
     *            materialized in the computed {@link ISolution}s when the
     *            program is executed and <code>false</code> if the
     *            {@link IRule} and {@link IBindingSet} should be materialized
     *            as well.
     */
    public RDFJoinNexusFactory(IRelationLocator relationLocator, boolean elementOnly) {

        this.relationLocator = relationLocator;
        
        this.elementOnly = elementOnly;

    }

    public RDFJoinNexus newJoinNexus(IBigdataFederation fed, long timestamp) {

        return new RDFJoinNexus(fed.getThreadPool(),relationLocator, timestamp, elementOnly);

    }

    public RDFJoinNexus newJoinNexus(ExecutorService service, AbstractTask task) {

        return new RDFJoinNexus(service, relationLocator, task.getTimestamp(), elementOnly);

    }

}
