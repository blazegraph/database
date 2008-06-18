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
 * Created on Jun 17, 2008
 */

package com.bigdata.rdf.inf;

import com.bigdata.rdf.inf.Rule.IConstraint;
import com.bigdata.rdf.inf.Rule.State;
import com.bigdata.rdf.inf.Rule.Var;

/**
 * A constraint that a variable may only take on the bindings enumerated by some
 * set.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class IN implements IConstraint {

    private final Var x;
    private final long[] set;

    /**
     * 
     * @param x
     *            Some variable.
     * @param set
     *            A set of legal term identifiers providing a constraint on the
     *            allowable values for that variable.
     */
    public IN(Var x, long[] set) {
        
        if (x == null || set == null)
            throw new IllegalArgumentException();

        if (set.length==0)
            throw new IllegalArgumentException();
        
        this.x = x;
        
        this.set = set;
        
    }
    
    public boolean accept(State s) {
        
        // get binding for "x".
        long x = s.get(this.x);
       
        if(x==NULL) return true; // not yet bound.

        for(int i=0; i<set.length; i++) {
            
            if(x == set[i]) return true;
            
        }
        
        return false;

   }

}
