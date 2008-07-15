/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Jul 9, 2008
 */

package com.bigdata.rdf.rules;

import com.bigdata.journal.IIndexManager;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.rule.eval.ActionEnum;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.IJoinNexusFactory;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RDFJoinNexusFactory implements IJoinNexusFactory {

    /**
     * 
     */
    private static final long serialVersionUID = 8270873764858640472L;
    
    private final ActionEnum action;
    private final long writeTime;
    private final long readTime;
    private final boolean justify;
    private final int bufferCapacity;
    private final int solutionFlags;
    private final IElementFilter filter;

    public String toString() {
        
        StringBuilder sb = new StringBuilder();
        
        sb.append(getClass().getSimpleName());
        
        sb.append("{ action="+action);

        sb.append(", writeTime="+writeTime);
        
        sb.append(", readTime="+readTime);
        
        sb.append(", justify="+justify);
        
        sb.append(", bufferCapacity="+bufferCapacity);
        
        sb.append(", solutionFlags="+solutionFlags);
        
        sb.append(", filter="+(filter==null?"N/A":filter.getClass().getName()));
        
        sb.append("}");
        
        return sb.toString();
        
    }
    
    /**
     * 
     * @param writeTime
     * @param readTime
     * @param justify
     * @param solutionFlags
     * @param filter
     */
    public RDFJoinNexusFactory(ActionEnum action, long writeTime,
            long readTime, boolean justify, int bufferCapacity,
            int solutionFlags, IElementFilter filter) {

        if (action == null)
            throw new IllegalArgumentException();
        
        this.action = action;
        
        this.writeTime = writeTime;

        this.readTime = readTime;

        this.justify = justify;

        this.bufferCapacity = bufferCapacity;
        
        this.solutionFlags = solutionFlags;

        this.filter = filter;

    }

    public IJoinNexus newInstance(IIndexManager indexManager) {

        return new RDFJoinNexus(this, indexManager, action, writeTime,
                readTime, justify, bufferCapacity, solutionFlags, filter);

    }

}
