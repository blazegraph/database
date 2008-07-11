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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.bigdata.btree.BTree;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.Journal;
import com.bigdata.journal.TemporaryStore;
import com.bigdata.relation.IMutableRelation;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.IRelationIdentifier;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.rule.IProgram;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.IStep;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.LocalDataServiceFederation;
import com.bigdata.service.LocalDataServiceFederation.LocalDataServiceImpl;

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
    
    /**
     * The set of distinct relations identified by the various rules.
     */
    public Set<IRelationIdentifier> getRelationNames(IStep step) {

        Set<IRelationIdentifier> c = new HashSet<IRelationIdentifier>();
        
        getRelationNames(step, c);

        if(log.isDebugEnabled()) {
            
            log.debug("Found "+c.size()+" relations, program="+step.getName());
            
        }

        return c;
        
    }
    
    private void getRelationNames(IStep p, Set<IRelationIdentifier> c) {

        if (p.isRule()) {

            final IRule r = (IRule) p;

            c.add(r.getHead().getRelationName());

        } else {
            
            final Iterator<IStep> itr = ((IProgram)p).steps();

            while (itr.hasNext()) {

                getRelationNames(itr.next(), c);

            }

        }

    }
    
    /**
     * Locate all relation identifers and resolve them to their relations.
     * <p>
     * Note: Distinct relation views are often used for reading from the
     * relation and writing on the relation. This allows greater concurrency
     * since the {@link BTree} that absorbs writes does not support concurrent
     * writers. See {@link IJoinNexus#getWriteTimestamp()} and
     * {@link IJoinNexus#getReadTimestamp()}.
     * 
     * @param timestamp
     *            The timestamp for the relation views.
     * 
     * @throws RuntimeException
     *             if any relation can not be resolved.
     * @throws RuntimeException
     *             if any relation is not local.
     */
    public Map<IRelationIdentifier, IRelation> getRelations(IIndexManager indexManager, IStep step, long timestamp) {

        if (step == null)
            throw new IllegalArgumentException();

        final Map<IRelationIdentifier, IRelation> c = new HashMap<IRelationIdentifier, IRelation>();

        getRelations(indexManager, step, c, timestamp );

        if (log.isDebugEnabled()) {

            log.debug("Located " + c.size() + " relations, program="
                    + step.getName());

        }

        return c;

    }

    @SuppressWarnings("unchecked")
    private void getRelations(IIndexManager indexManager, IStep p, Map<IRelationIdentifier, IRelation> c, long timestamp) {

        if (p.isRule()) {

            final IRule r = (IRule) p;

            final IRelationIdentifier relationIdentifier = r.getHead().getRelationName();

            if (!c.containsKey(relationIdentifier)) {

                final IRelation relation = (IRelation) indexManager.getResourceLocator()
                        .locate(relationIdentifier, timestamp);
                
                c.put(relationIdentifier, relation );
                
            }

        } else {
            
            final Iterator<IStep> itr = ((IProgram)p).steps();

            while (itr.hasNext()) {

                getRelations(indexManager, itr.next(), c, timestamp);

            }

        }

    }
    
    /**
     * Returns the names of the indices maintained by the relations.
     * 
     * @param c
     *            A collection of {@link IRelation}s.
     * 
     * @return The names of the indices maintained by those relations.
     */
    @SuppressWarnings("unchecked")
    public Set<String> getIndexNames(Collection<IRelation> c) {

        if (c == null)
            throw new IllegalArgumentException();

        if (c.isEmpty())
            return Collections.EMPTY_SET;

        final Set<String> set = new HashSet<String>();
        
        final Iterator<IRelation> itr = c.iterator();
        
        while(itr.hasNext()) {
            
            final IRelation relation = itr.next();
            
            set.addAll(relation.getIndexNames());
            
        }

        return set;
        
    }

    /**
     * Return the {@link IBigdataFederation} that is capable of resolving all
     * non-local indices used by the {@link IRelation}s in the program -or-
     * <code>null</code> if all indices are resolved by local resources
     * outside of {@link IBigdataFederation} control.
     * <p>
     * Note: If the {@link IBigdataFederation} is completely local to the
     * machine on which the program will run and any indices outside of the
     * federation's control are also local to that same machine, then the
     * federation reference will be returned and the local resources will be
     * resolved.
     * <p>
     * Note: If any index is hosted by a scale-out {@link IBigdataFederation}
     * and there are any machine local resources that are not resolvable by a
     * remote host for some part of the program then an exception will be
     * thrown.
     * <p>
     * Note: If the returned {@link IBigdataFederation} is a
     * {@link LocalDataServiceFederation} then optimizations are possible by
     * running program as an {@link AbstractTask} within the
     * {@link LocalDataServiceImpl}, including the case where there is access
     * to local resources.
     * 
     * @param step
     *            The program.
     * @param timestamp
     *            The timestamp is relatively arbitrary. We do not actually use
     *            the relation objects that are being created other than to
     *            inquire after the names of their indices.
     * 
     * @return The {@link IBigdataFederation} -or- <code>null</code> if the
     *         indices are all hosted by <strong>local</strong> resources
     *         outside of {@link IBigdataFederation} control, such as a
     *         {@link Journal} or {@link TemporaryStore}.
     * 
     * @throws IllegalArgumentException
     *             if <i>program</i> is <code>null</code>
     * 
     * @throws IllegalArgumentException
     *             if <i>program</i> is empty (not an {@link IRule} and having
     *             no {@link IProgram#steps()}).
     * 
     * @throws IllegalStateException
     *             if some of the indices are hosted on local resources and
     *             {@link IBigdataFederation#isDistributed()} is
     *             <code>true</code> (this implies that those resources MAY be
     *             unreachable from at least some of the {@link DataService}s
     *             in the federation).
     * 
     * @throws IllegalStateException
     *             if there are relations belonging to different
     *             {@link IBigdataFederation}s.
     */
    public IBigdataFederation getFederation(IIndexManager indexManager, IStep step, long timestamp) {

        if (step == null)
            throw new IllegalArgumentException();

        if(!step.isRule() && ((IProgram)step).stepCount()==0)
            throw new IllegalArgumentException("empty program");

        /*
         * The set of distinct relations.
         */
        final Map<IRelationIdentifier, IRelation> relations = getRelations(indexManager, step,
                timestamp);

        if(relations.isEmpty()) {
            
            // Note: this is either an empty program or a bug.
            throw new IllegalArgumentException();
            
        }
        
        // set of distinct index managers (IBigdataFederation, TemporaryStore, Journal, etc).
        final Set<IIndexManager> indexManagers = new HashSet<IIndexManager>();

        IBigdataFederation fed = null;
        
        final Set<IIndexManager> localResources = new HashSet<IIndexManager>();
        
        final Set<IBigdataFederation> feds = new HashSet<IBigdataFederation>();
        
        for(IRelation relation : relations.values()) {
            
            final IIndexManager tmp = relation.getIndexManager();
            
            indexManagers.add( tmp );
            
            if(tmp instanceof IBigdataFederation) {
                
                feds.add( (IBigdataFederation)tmp );
               
                if(fed == null) {
                
                    fed = (IBigdataFederation)tmp;
                    
                }
                
            } else {
            
                localResources.add(tmp);
                
            }
            
        }

        assert !indexManagers.isEmpty();
        
        if(fed == null) {
            
            /*
             * No federation, so all resources must be local.
             */
            
            assert ! localResources.isEmpty();

            // Note: [null] indicates non-federation based operation.
            return null;
            
        }

        if (feds.size() > 2) {
            
            throw new IllegalStateException(
                    "Program uses more than one federation: #feds="
                            + feds.size());
            
        }

        if(!localResources.isEmpty() && fed.isDistributed()) {
            
            throw new IllegalStateException("Program uses local resources but federation is distributed.");
            
        }
        
        return fed;
        
    }

    /**
     * <code>true</code> iff the program either is the fix point closure of a
     * rule or contains a step (recursively) that is the fix point closure of
     * one or more rules.
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
            return true;

        final Iterator<IStep> itr = program.steps();

        while (itr.hasNext()) {

            if (isClosureProgram(itr.next()))
                return true;

        }

        return false;

    }

    /**
     * Create the appropriate buffers to absorb writes by the rules in the
     * program that target an {@link IMutableRelation}.
     * 
     * @return the map from relation identifier to the corresponding buffer.
     * 
     * @throws IllegalStateException
     *             if the program is being executed as an
     *             {@link ActionEnum#Query}.
     * @throws RuntimeException
     *             If a rule requires mutation for a relation (it will write on
     *             the relation) and the corresponding entry in the map does not
     *             implement {@link IMutableRelation}.
     */
    protected Map<IRelationIdentifier, IBuffer<ISolution>> getMutationBuffers(
            ActionEnum action, IJoinNexus joinNexus,
            Map<IRelationIdentifier, IRelation> relations) {

        if (action == ActionEnum.Query) {

            throw new IllegalStateException();
            
        }

        if(log.isDebugEnabled()) {
            
            log.debug("");
            
        }

        final Map<IRelationIdentifier, IBuffer<ISolution>> c = new HashMap<IRelationIdentifier, IBuffer<ISolution>>(
                relations.size());

        final Iterator<Map.Entry<IRelationIdentifier, IRelation>> itr = relations
                .entrySet().iterator();

        while (itr.hasNext()) {

            final Map.Entry<IRelationIdentifier, IRelation> entry = itr.next();

            final IRelationIdentifier relationIdentifier = entry.getKey();

            final IRelation relation = entry.getValue();

            final IBuffer<ISolution> buffer;

            switch (action) {
            
            case Insert:
                
                buffer = joinNexus.newInsertBuffer((IMutableRelation)relation);
                
                break;
                
            case Delete:
                
                buffer = joinNexus.newDeleteBuffer((IMutableRelation)relation);
                
                break;
                
            default:
                
                throw new AssertionError("action=" + action);
            
            }

            c.put(relationIdentifier, buffer);
            
        }

        if(log.isDebugEnabled()) {
            
            log.debug("Created "+c.size()+" mutation buffers: action="+action);
            
        }

        return c;
        
    }
    
}
