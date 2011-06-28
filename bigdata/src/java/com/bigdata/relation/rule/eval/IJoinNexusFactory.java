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

import java.io.Serializable;
import java.util.Properties;

import com.bigdata.bop.joinGraph.IEvaluationPlanFactory;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IIndexStore;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.rule.IProgram;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataFederation;

/**
 * A factory for {@link IJoinNexus} instances.
 * <p>
 * Note: This factory plays a critical role in (re-)constructing a suitable
 * {@link IJoinNexus} instance when an {@link IProgram} is executed on a remote
 * {@link DataService} or when its execution is distributed across an
 * {@link IBigdataFederation} using RMI. Implementations are presumed to carry
 * some state relating to the desired execution context, including the
 * <i>solutionFlags</i>, any {@link IElementFilter} to be applied to the
 * created buffers, etc.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IJoinNexusFactory extends Serializable {

    ActionEnum getAction();
    
    /**
     * The timestamp for the write view of the relation(s).
     */
    long getWriteTimestamp();
    
    /**
     * The timestamp for the read view of the relation(s).
     */
    long getReadTimestamp();
    
    /**
     * Used by fix point closure operations to advance the read timestamp to the
     * {@link IIndexStore#getLastCommitTime()} at the start of each round of
     * closure.
     */
    void setReadTimestamp(long readTimestamp);
    
    Properties getProperties();
    
    int getSolutionFlags();
    
    IElementFilter<?> getSolutionFilter();

    IEvaluationPlanFactory getEvaluationPlanFactory();
    
    IRuleTaskFactory getDefaultRuleTaskFactory();
    
    /**
     * Singleton factory for an {@link IJoinNexus} instance for the given
     * <i>indexManager</i>.
     * <p>
     * Note: The use of a singleton pattern helps to reduce the proliferation of
     * distinct {@link IJoinNexus} instances for a given combination of
     * {@link IJoinNexusFactory} and {@link IIndexManager} objects. A distinct
     * {@link IJoinNexus} instance is of course generated when the
     * {@link IJoinNexusFactory} is de-serialized, e.g., during RMI, and when a
     * different {@link IIndexManager} is used.
     * 
     * @param indexManager
     *            Used to locate relations and parallelize operations during
     *            rule execution.
     */
    IJoinNexus newInstance(IIndexManager indexManager);

}
