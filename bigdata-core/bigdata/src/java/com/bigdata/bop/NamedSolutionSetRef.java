/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on Aug 31, 2011
 */

package com.bigdata.bop;

import java.util.Arrays;
import java.util.UUID;

import com.bigdata.bop.controller.INamedSolutionSetRef;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.journal.ITx;

/**
 * Class models the information which uniquely describes a named solution set.
 * The "name" is comprised of the following components:
 * <dl>
 * <dt>queryId</dt>
 * <dd>The {@link UUID} of the query which generated the named solution set.
 * This provides the scope for the named solution set. It is used to (a) locate
 * the data; and (b) release the data when the query goes out of scope.</dd>
 * <dt>namedSet</dt>
 * <dd>The "name" of the solution set as given in the query. The name is not a
 * sufficient identifier since the same solution set name may be used in
 * different queries and with different join variables.</dd>
 * <dt>joinVars[]</dt>
 * <dd>The ordered array of the join variable. This serves to differentiate
 * among named solution sets having the same data but different join variables.</dd>
 * </dl>
 * Together, these components provide for a name that is unique within the scope
 * of a query.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class NamedSolutionSetRef implements INamedSolutionSetRef {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
	 * The {@link UUID} of the {@link IRunningQuery} which generated the named
	 * solution set. This is where you need to look to find the data.
	 * <p>
	 * Note: The value <code>null</code> has two different interpretations
	 * depending on which constructor form was used.  
	 * <p>
	 * <dl>
	 * <dt>new NamedSolutionSetRef(queryId, namedSet, joinVars);</dt>
	 * <dd>This form is used to refer to an
	 * {@link com.bigdata.bop.IQueryAttributes}. When queryId is
	 * <code>null</code>, the code will look at the
	 * {@link com.bigdata.bop.IQueryAttributes} for the current running query
	 * (this form is the new usage and makes it possible to scope an attribute
	 * to an instance of a query when the same query plan is executed more than
	 * once, as it is for the sub-queries issued by the property path operator).
	 * Otherwise it will resolve the identified query and use its
	 * {@link com.bigdata.bop.IQueryAttributes} (this 2nd form is the historical
	 * usage and makes it possible to reference an attribute on a parent query).
	 * </dd>
	 * <dt>NamedSolutionSetRef(namespace, timestamp, localName, joinVars);</dt>
	 * <dd>This form is used to refer to a named solution set.</dd>
	 * </dl>
	 * 
	 * @see <a href="https://jira.blazegraph.com/browse/BLZG-1493" > NPE in
	 *      nested star property paths </a>
	 */
    private final UUID queryId;

    /**
     * The namespace associated with the KB view -or- <code>null</code> if the
     * named solution set is attached to an {@link IRunningQuery}.
     */
    private final String namespace;

    /**
     * The timestamp associated with the KB view.
     * <p>
     * Note: This MUST be ignored if {@link #namespace} is <code>null</code>.
     */
    private final long timestamp;
  
    /**
     * The application level name for the named solution set.
     */
    private final String localName;

    /**
     * The ordered set of variables that specifies the ordered set of components
     * in the key for the desired index over the named solution set (required,
     * but may be an empty array).
     */
    @SuppressWarnings("rawtypes")
    private final IVariable[] joinVars;

    @Override
    final public UUID getQueryId() {

        return queryId;
        
    }

    @Override
    public String getNamespace() {

        return namespace;
        
    }

    @Override
    public long getTimestamp() {

        return timestamp;
        
    }

    @Override
    final public String getLocalName() {

        return localName;
        
    }

    @Override
    final public IVariable[] getJoinVars() {

        // TODO return clone of the array to avoid possible modification?
        return joinVars;

    }

    /**
     * 
     * @param queryId
     *            The {@link UUID} of the {@link IRunningQuery} where you need
     *            to look to find the data (optional - see BLZG-1493).
     * @param namedSet
     *            The application level name for the named solution set
     *            (required).
     * @param joinVars
     *            The join variables (required, but may be an empty array).
     */
    @SuppressWarnings("rawtypes")
    NamedSolutionSetRef(//
            final UUID queryId, //
            final String namedSet,//
            final IVariable[] joinVars//
            ) {

//    	See BLZG-1493
//        if (queryId == null)
//            throw new IllegalArgumentException();

        if (namedSet == null)
            throw new IllegalArgumentException();

        if (joinVars == null)
            throw new IllegalArgumentException();

        this.queryId = queryId;

        this.namespace = null;
        
        // Note: This should be IGNORED since the [namespace] is null.
        this.timestamp = ITx.READ_COMMITTED;

        this.localName = namedSet;

        this.joinVars = joinVars;

    }

    /**
     * 
     * @param namespace
     *            The namespace of the KB view.
     * @param timestamp
     *            The timestamp associated with the KB view.
     * @param localName
     *            The application level name for the named solution set
     *            (required).
     * @param joinVars
     *            The join variables (required, but may be an empty array).
     */
    @SuppressWarnings("rawtypes")
    NamedSolutionSetRef(//
            final String namespace, //
            final long timestamp,//
            final String localName,//
            final IVariable[] joinVars//
            ) {

        if (namespace == null)
            throw new IllegalArgumentException();

        if (localName == null)
            throw new IllegalArgumentException();

        if (joinVars == null)
            throw new IllegalArgumentException();

        this.queryId = null;

        this.namespace = namespace;
        
        this.timestamp = timestamp;
        
        this.localName = localName;

        this.joinVars = joinVars;

    }

    private transient volatile String fqn;

    @Override
    public String getFQN() {

        if (fqn == null) {

            synchronized (localName) {

                if (namespace == null) {

                    fqn = localName;

                } else {

                    fqn = NamedSolutionSetRefUtility.getFQN(
                            namespace, localName, joinVars);

                }

            }

        }

        return fqn;
           
    }
    
    @Override
    public int hashCode() {
        if (h == 0) {
            // TODO Review this for effectiveness.
        	// See BLZG-1493 for queryId == null
            h = (queryId == null ? namespace==null?0:namespace.hashCode() + (int) timestamp
                    : queryId.hashCode())
                    + localName.hashCode()
                    + Arrays.hashCode(joinVars);
        }
        return h;
    }

    private transient int h;

    @Override
    public boolean equals(final Object o) {

        if (this == o)
            return true;

        if (!(o instanceof NamedSolutionSetRef))
            return false;

        final NamedSolutionSetRef t = (NamedSolutionSetRef) o;

        if (queryId != null) {
         
            if (!queryId.equals(t.queryId))
                return false;
            
        } else {
            
            if (!namespace.equals(t.namespace))
                return false;
            
            if (timestamp != t.timestamp)
                return false;
            
        }

        if (!localName.equals(t.localName))
            return false;

        if (!Arrays.equals(joinVars, t.joinVars))
            return false;

        return true;

    }

    @Override
    public String toString() {

        final StringBuilder sb = new StringBuilder();

        sb.append(getClass().getSimpleName());
        sb.append("{localName=").append(localName);
        if (queryId == null) {
            sb.append(",namespace=").append(namespace);
            sb.append(",timestamp=").append(timestamp);
        } else {
            sb.append(",queryId=").append(queryId);
        }
        sb.append(",joinVars=").append(Arrays.toString(joinVars));
        sb.append("}");

        return sb.toString();

    }

}
