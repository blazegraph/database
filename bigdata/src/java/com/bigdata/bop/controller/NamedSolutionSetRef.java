/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Aug 31, 2011
 */

package com.bigdata.bop.controller;

import java.io.Serializable;
import java.util.Arrays;
import java.util.UUID;

import com.bigdata.bop.IVariable;
import com.bigdata.bop.Var;
import com.bigdata.bop.engine.IRunningQuery;

/**
 * Class models the information which uniquely describes a named solution
 * set.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
 * @version $Id$
 */
public class NamedSolutionSetRef implements Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public final UUID queryId;

    public final String namedSet;

    public final IVariable[] joinVars;

    /**
     * 
     * @param queryId
     *            The {@link UUID} of the {@link IRunningQuery} which generated
     *            the named solution set. This is where you need to look to find
     *            the data.
     * @param namedSet
     *            The application level name for the named solution set.
     * @param joinVars
     *            The join variables.
     */
    public NamedSolutionSetRef(final UUID queryId, final String namedSet,
            final IVariable[] joinVars) {

        if (queryId == null)
            throw new IllegalArgumentException();

        if (namedSet == null)
            throw new IllegalArgumentException();

        if (joinVars == null)
            throw new IllegalArgumentException();

        this.queryId = queryId;

        this.namedSet = namedSet;

        this.joinVars = joinVars;

    }

    public int hashCode() {
        if (h == 0) {
            h = queryId.hashCode() + namedSet.hashCode()
                    + Arrays.hashCode(joinVars);

        }
        return h;
    }

    private transient int h;

    public boolean equals(final Object o) {

        if (this == o)
            return true;

        if (!(o instanceof NamedSolutionSetRef))
            return false;

        final NamedSolutionSetRef t = (NamedSolutionSetRef) o;

        if (!queryId.equals(t.queryId))
            return false;

        if (!namedSet.equals(t.namedSet))
            return false;

        if (!Arrays.equals(joinVars, t.joinVars))
            return false;

        return true;

    }

    /**
     * Return a human readable representation which can be decoded by
     * {@link #valueOf(String)}.
     */
    public String toString() {

        final StringBuilder sb = new StringBuilder();

        sb.append(getClass().getSimpleName());
        sb.append("{queryId=").append(queryId);
        sb.append(",namedSet=").append(namedSet);
        sb.append(",joinVars=").append(Arrays.toString(joinVars));
        sb.append("}");

        return sb.toString();

    }

    /**
     * Parses the {@link #toString()} representation, returning an instance of
     * this class.
     */
    public static NamedSolutionSetRef valueOf(final String s) {

        final int posQueryId = assertIndex(s, s.indexOf("queryId="));
        final int posQueryIdEnd = assertIndex(s, s.indexOf(",", posQueryId));
        final String queryIdStr = s.substring(posQueryId + 8, posQueryIdEnd);
        final UUID queryId = UUID.fromString(queryIdStr);

        final int posNamedSet = assertIndex(s, s.indexOf("namedSet="));
        final int posNamedSetEnd = assertIndex(s, s.indexOf(",", posNamedSet));
        final String namedSet = s.substring(posNamedSet + 9, posNamedSetEnd);

        final int posJoinVars = assertIndex(s, s.indexOf("joinVars=["));
        final int posJoinVarsEnd = assertIndex(s, s.indexOf("]", posJoinVars));
        final String joinVarsStr = s
                .substring(posJoinVars + 10, posJoinVarsEnd);

        final String[] a = joinVarsStr.split(", ");

        final IVariable[] joinVars = new IVariable[a.length];

        for (int i = 0; i < a.length; i++) {

            joinVars[i] = Var.var(a[i]);

        }

        return new NamedSolutionSetRef(queryId, namedSet, joinVars);

    }

    static private int assertIndex(final String s, final int index) {

        if (index >= 0)
            return index;
        
        throw new IllegalArgumentException(s);
        
    }

}
