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
 * Created on Jun 23, 2008
 */

package com.bigdata.join;

import java.io.Serializable;
import java.util.Iterator;

/**
 * A program consists of a set of rules and/or programs. Some programs are
 * executed sequentially while others are (at least logically) parallel. A
 * program may also specify the transitive closure of its rules.
 * 
 * @todo add an XML (de-)serialization for programs and rules so that the rule
 *       sets may be declared. Note that there are a few very specialized rules
 *       whose semantics might not be fully declarable for the fast closure
 *       method used by the RDF KB.
 * 
 * @todo Make it possible for people to easily extend the RDF KB rule sets or
 *       run rules over a RDF data set, but note that there are interactions in
 *       the rules choosen for evaluation during forward closure and those
 *       choosen for evaluation at query time.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IProgram extends Serializable {

    /**
     * The name of the program.
     */
    public String getName();
    
    /**
     * True iff the {@link #steps()} should be executed in parallel.
     */
    boolean isParallel();

    /**
     * True iff the {@link #steps()} should be run to fixed point.
     * 
     * @todo does closure always imply parallel? I think so. If true then impose
     *       a constraint on the ctor parameters for {@link Program}.
     */
    boolean isClosure();
    
    /**
     * The sequence of sub-program steps. When {@link #isParallel()} is
     * <code>true</code> those steps MAY be executed in parallel.
     */
    Iterator<IProgram> steps();

    /**
     * The #of steps in the program (non-recursive).
     */
    public int stepCount();

    /**
     * An array containing the steps in the program (non-recursive).
     */
    public IProgram[] toArray();

    /**
     * A human readable representation of the program.
     */
    public String toString();
    
}
