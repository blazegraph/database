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
 * Created on Mar 10, 2012
 */

package com.bigdata.rdf.sparql.ast;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IVariable;

/**
 * The top level container for a sequence of UPDATE operations.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class UpdateRoot extends GroupNodeBase<Update> implements IPrefixDecls {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends QueryNodeBase.Annotations,
            IPrefixDecls.Annotations {

    }
    
    /**
     * 
     */
    public UpdateRoot() {
    }

    /**
     * @param op
     */
    public UpdateRoot(UpdateRoot op) {
        super(op);
    }

    /**
     * @param args
     * @param anns
     */
    public UpdateRoot(BOp[] args, Map<String, Object> anns) {
        super(args, anns);
    }

    @SuppressWarnings("unchecked")
    public Map<String, String> getPrefixDecls() {

        final Map<String, String> prefixDecls = (Map<String, String>) getProperty(Annotations.PREFIX_DECLS);

        if (prefixDecls == null)
            return Collections.emptyMap();

        return Collections.unmodifiableMap(prefixDecls);

    }

    @Override
    public void setPrefixDecls(final Map<String, String> prefixDecls) {

        setProperty(Annotations.PREFIX_DECLS, prefixDecls);

    }

    @Override
    public String toString(final int indent) {
        
        final String s = indent(indent);
        
        final StringBuilder sb = new StringBuilder();

        final Map<String/* prefix */, String/* uri */> prefixDecls = getPrefixDecls();

//        if (getProperty(Annotations.TIMEOUT) != null) {
//            sb.append("\n");
//            sb.append(s);
//            sb.append("timeout=" + getTimeout());
//        }

        if(prefixDecls != null) {

            for (Map.Entry<String, String> e : prefixDecls.entrySet()) {

                sb.append("\n");

                sb.append(s);
                
                sb.append("PREFIX ");
                
                sb.append(e.getKey());
                
                sb.append(": <");
                
                sb.append(e.getValue());
                
                sb.append(">");

            }

        }

        for (Update n : this) {

            sb.append(n.toString(indent + 1));

        }

        return sb.toString();

    }
    

    @Override
    public Set<IVariable<?>> getRequiredBound(StaticAnalysis sa) {
       return new HashSet<IVariable<?>>();
    }

    @Override
    public Set<IVariable<?>> getDesiredBound(StaticAnalysis sa) {
       return new HashSet<IVariable<?>>();
    }

}
