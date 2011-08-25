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
 * Created on Aug 18, 2011
 */

package com.bigdata.rdf.sparql.ast;


/**
 * An AST node which provides a reference in an {@link IGroupNode} and indicates
 * that a named solution set should be joined with the solutions in the group.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see NamedSubqueryRoot
 */
public class NamedSubqueryInclude extends GroupMemberNodeBase {

    private static final long serialVersionUID = 1L;

    interface Annotations extends SubqueryRoot.Annotations {
        
        String SUBQUERY_NAME = "subqueryName";
        
    }

    /**
     * @param name
     *            The name of the subquery result set.
     */
    public NamedSubqueryInclude(final String name) {
        setName(name);
    }

    /**
     * The name of the {@link NamedSubqueryRoot} to be joined.
     */
    public String getName() {
        
        return (String) getProperty(Annotations.SUBQUERY_NAME);
                
    }

    /**
     * Set the name of the {@link NamedSubqueryRoot} to be joined.
     * 
     * @param name
     */
    public void setName(String name) {
     
        if(name == null)
            throw new IllegalArgumentException();
        
        setProperty(Annotations.SUBQUERY_NAME, name);
        
    }

    @Override
    public String toString(int indent) {

        return indent(indent) + "INCLUDE %" + getName();

    }
}
