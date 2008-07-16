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

import java.util.Iterator;

import org.apache.log4j.Logger;

import com.bigdata.relation.rule.IProgram;
import com.bigdata.relation.rule.IStep;

/**
 * Support for determining how and where a program should be executed.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ProgramUtility {
    
    protected static Logger log = Logger.getLogger(ProgramUtility.class);

//    private final IResourceLocator resourceLocator;
//    private final long readTimestamp;

//    public ProgramUtility(IJoinNexus joinNexus) {
//        
//        this(joinNexus.getRelationLocator());//, joinNexus.getReadTimestamp());
//        
//    }

//    /**
//     * 
//     * @param resourceLocator
//     */
    public ProgramUtility() {//IResourceLocator resourceLocator) {
        
//        if (resourceLocator == null)
//            throw new IllegalArgumentException();
//        
//        this.resourceLocator = resourceLocator;
        
    }
    
//    /**
//     * Return the {@link IIndexManager} that is capable of resolving all
//     * relations (and their indices) used in the {@link IStep} -or-
//     * <code>null</code> if some relations or indices are not resolvable from
//     * the current context (in general, either because they do not exist or
//     * because they rely on machine or JVM local resources not accessible from
//     * the current execution context).
//     * <p>
//     * Note: If the returned {@link IIndexManager} is an {@link AbstractJournal},
//     * an {@link LocalDataServiceFederation} then various optimizations are
//     * possible, including running program as an {@link AbstractTask} within the
//     * {@link LocalDataServiceImpl}, including the case where there is access
//     * to local resources.
//     * 
//     * @param step
//     *            The program.
//     * @param timestamp
//     *            The timestamp is relatively arbitrary. We do not actually use
//     *            the relation objects that are being created other than to
//     *            inquire after the names of their indices.
//     * 
//     * @return The {@link IBigdataFederation} -or- <code>null</code> if the
//     *         indices are all hosted by <strong>local</strong> resources
//     *         outside of {@link IBigdataFederation} control, such as a
//     *         {@link Journal} or {@link TemporaryStore}.
//     * 
//     * @throws IllegalArgumentException
//     *             if <i>program</i> is <code>null</code>
//     * 
//     * @throws IllegalArgumentException
//     *             if <i>program</i> is empty (not an {@link IRule} and having
//     *             no {@link IProgram#steps()}).
//     * 
//     * @throws IllegalStateException
//     *             if some of the indices are hosted on local resources and
//     *             {@link IBigdataFederation#isDistributed()} is
//     *             <code>true</code> (this implies that those resources MAY be
//     *             unreachable from at least some of the {@link DataService}s
//     *             in the federation).
//     * 
//     * @throws IllegalStateException
//     *             if there are relations belonging to different
//     *             {@link IBigdataFederation}s.
//     */
//    public IBigdataFederation getFederation(IIndexManager indexManager,
//            IStep step, long timestamp) {
//
//        if (step == null)
//            throw new IllegalArgumentException();
//
//        if(!step.isRule() && ((IProgram)step).stepCount()==0)
//            throw new IllegalArgumentException("empty program");
//
//        /*
//         * The set of distinct relations.
//         */
//        final Map<String, IRelation> relations = getRelations(indexManager, step,
//                timestamp);
//
//        if(relations.isEmpty()) {
//            
//            // Note: this is either an empty program or a bug.
//            throw new IllegalArgumentException();
//            
//        }
//        
//        // set of distinct index managers (IBigdataFederation, TemporaryStore, Journal, etc).
//        final Set<IIndexManager> indexManagers = new HashSet<IIndexManager>();
//
//        IBigdataFederation fed = null;
//        
//        final Set<IIndexManager> localResources = new HashSet<IIndexManager>();
//        
//        final Set<IBigdataFederation> feds = new HashSet<IBigdataFederation>();
//        
//        for(IRelation relation : relations.values()) {
//            
//            final IIndexManager tmp = relation.getIndexManager();
//            
//            indexManagers.add( tmp );
//            
//            if(tmp instanceof IBigdataFederation) {
//                
//                feds.add( (IBigdataFederation)tmp );
//               
//                if(fed == null) {
//                
//                    fed = (IBigdataFederation)tmp;
//                    
//                }
//                
//            } else {
//            
//                localResources.add(tmp);
//                
//            }
//            
//        }
//
//        assert !indexManagers.isEmpty();
//        
//        if(fed == null) {
//            
//            /*
//             * No federation, so all resources must be local.
//             */
//            
//            assert ! localResources.isEmpty();
//
//            // Note: [null] indicates non-federation based operation.
//            return null;
//            
//        }
//
//        if (feds.size() > 2) {
//            
//            throw new IllegalStateException(
//                    "Program uses more than one federation: #feds="
//                            + feds.size());
//            
//        }
//
//        if(!localResources.isEmpty() && fed.isDistributed()) {
//            
//            throw new IllegalStateException("Program uses local resources but federation is distributed.");
//            
//        }
//        
//        return fed;
//        
//    }

    /**
     * <code>true</code> iff the program contains an embedded closure
     * operation. <code>false</code> if the program is a rule or is itself a
     * closure operation.
     * 
     * @param step
     *            The program.
     * 
     * @return
     */
    public boolean isClosureProgram(IStep step) {

        if (step == null)
            throw new IllegalArgumentException();

        if(step.isRule()) return false;
                
        final IProgram program = (IProgram)step;
        
        if (program.isClosure())
            return false;

        final Iterator<IStep> itr = program.steps();

        while (itr.hasNext()) {

            if (isClosureProgram2(itr.next()))
                return true;

        }

        return false;

    }

    /**
     * <code>true</code> iff this program is or contains a closure operation.
     */
    private boolean isClosureProgram2(IStep step) {

        if (step == null)
            throw new IllegalArgumentException();

        if (step.isRule()) return false;
                
        final IProgram program = (IProgram)step;
        
        if (program.isClosure())
            return true;

        final Iterator<IStep> itr = program.steps();

        while (itr.hasNext()) {

            if (isClosureProgram2(itr.next()))
                return true;

        }

        return false;

    }

}
