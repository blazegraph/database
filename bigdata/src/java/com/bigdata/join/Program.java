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
 * Created on Jun 20, 2008
 */

package com.bigdata.join;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

/**
 * Mutable program may be used to create a variety of rule executions.
 * 
 * FIXME Make sure that we can handle the "fast" closure method - that is a
 * specific sequence of both custom and standard rules than runs once rather
 * than to fixed point (some of the rules in the program are run to fixed point
 * so that needs to be a program control parameter in addition to sequential vs
 * parallel execution).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Program implements IProgram {

    private static final long serialVersionUID = 2774954504183880320L;

    protected static final Logger log = Logger.getLogger(Program.class);

    private final String name;
    
    private final boolean parallel;
    
    private final boolean closure;
    
    private final List<IProgram> steps = new LinkedList<IProgram>();
    
    /**
     * An empty program.
     * 
     * @param parallel
     *            <code>true</code> iff the steps in the program are
     *            parallelizable.
     * @param closure
     *            <code>true</code> iff the steps in the program must be run
     *            until a fixed point is achieved.
     */
    public Program(String name, boolean parallel, boolean closure) {
        
        if (name == null)
            throw new IllegalArgumentException();
        
        this.name = name;
        
        this.parallel = parallel;
        
        this.closure = closure;
        
    }
    
    public String getName() {
        
        return name;
        
    }

    final public boolean isRule() {
        
        return false;
        
    }
    
    public boolean isParallel() {

        return parallel;
        
    }

    public boolean isClosure() {
        
        return closure;
        
    }
    
    public int stepCount() {
        
        return steps.size();
        
    }
    
    public Iterator<IProgram> steps() {

        return steps.iterator();
    }

    public IProgram[] toArray() {
        
        return steps.toArray(new IProgram[stepCount()]);
        
    }
    
    /**
     * Add another step in the program.
     * 
     * @param step
     *            The step.
     */
    public void addStep(final IProgram step) {

        if (step == null)
            throw new IllegalArgumentException();

        if (step == this) // no cycles please.
            throw new IllegalArgumentException();
        
        steps.add(step);

    }

}
