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

package com.bigdata.relation.rule.eval;

import java.util.concurrent.ExecutorService;

import com.bigdata.journal.AbstractTask;
import com.bigdata.relation.rule.IProgram;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IClientIndex;
import com.bigdata.service.LocalDataServiceFederation;

/**
 * A factory for {@link IJoinNexus} instances.
 * <p>
 * Note: This factory plays a critical role in (re-)constructing a suitable
 * {@link IJoinNexus} instance when an {@link IProgram} is executed on a remote
 * {@link DataService} or when its execution is distributed across an
 * {@link IBigdataFederation} using RMI.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @deprecated Still a trial balloon.
 */
public interface IJoinNexusFactory {

    /**
     * Variant when an {@link IBigdataFederation} will be used to locate the
     * relation indices. This variant will use {@link IClientIndex} views of
     * indices which may be anywhere in the {@link IBigdataFederation}.
     * 
     * @param fed
     *            The {@link IBigdataFederation}.
     * @param timestamp
     *            The timestamp of the relation view(s).
     */
    IJoinNexus newJoinNexus(IBigdataFederation fed, long timestamp);
    
    /**
     * Variant used when running inside a {@link DataService} as an
     * {@link AbstractTask}. This variant is only useful when all indices for
     * the relation(s) are (a) monolithic; and (b) located on the same
     * {@link DataService}, which is the case for the
     * {@link LocalDataServiceFederation}.
     * 
     * @param service
     * @param task
     */
    IJoinNexus newJoinNexus(ExecutorService service, AbstractTask task);
    
}
