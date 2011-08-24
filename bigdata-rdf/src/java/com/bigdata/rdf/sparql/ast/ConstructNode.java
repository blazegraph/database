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
 * Created on Aug 24, 2011
 */

package com.bigdata.rdf.sparql.ast;

/**
 * A template for the construction of one or more graphs based on the solutions
 * projected by a query.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 *          FIXME We might want to open this up a bit more to allow group graph
 *          patterns or something like that to be embedded. Right now it only
 *          permits statement patterns. That handles the traditional CONSTRUCT
 *          but not the Anzo CONSTRUCT extension for quads.
 */
public class ConstructNode<E extends StatementPatternNode> extends GroupNodeBase<E> {

    public ConstructNode() {
        super(false/* optional */);
    }

    @Override
    public IGroupNode<E> addChild(final E child) {

        if (!(child instanceof StatementPatternNode))
            throw new UnsupportedOperationException();
        
        super.addChild(child);
        
        return this;
    }
    
    @Override
    public String toString(final int indent) {

        final StringBuilder sb = new StringBuilder();

        sb.append("\n");

        sb.append(indent(indent));

        sb.append("construct ");

        boolean first = true;

        for (StatementPatternNode v : this) {

            if (first) {
                first = false;
            } else {
                sb.append(". ");
            }

            sb.append(v);

        }

        return sb.toString();

    }

}
