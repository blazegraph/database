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
 * Created on Aug 24, 2011
 */

package com.bigdata.rdf.sail.sparql.ast;

import org.openrdf.model.Value;
import com.bigdata.rdf.sail.sparql.ast.SimpleNode;
import com.bigdata.rdf.sail.sparql.ast.SyntaxTreeBuilder;

/**
 * An abstract base class for AST objects modeling RDF Values.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class ASTRDFValue extends SimpleNode {

    private Value value;
    
    /**
     * @param id
     */
    public ASTRDFValue(int id) {
        super(id);
    }

    /**
     * @param parser
     * @param id
     */
    public ASTRDFValue(SyntaxTreeBuilder parser, int id) {
        super(parser, id);
    }

    public Value getRDFValue() {
        
        return value;
        
    }

    public void setRDFValue(final Value value) {
        
        this.value = value;
        
    }
    
}
