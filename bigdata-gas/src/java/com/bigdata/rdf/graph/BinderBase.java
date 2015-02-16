/**
   Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package com.bigdata.rdf.graph;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;

/**
 * A base class for IBinders.
 */
public abstract class BinderBase<VS, ES, ST> implements IBinder<VS, ES, ST> {

    /**
     * The ordinal index of the variable that is bound by this
     * {@link BinderBase}. By convention, index ZERO is the vertex. Indices
     * greater than ZERO are typically aspects of the state of the vertex.
     */
    @Override
    public abstract int getIndex();

    /**
     * Subclasses can implement this method if they follow the old single
     * bind paradigm.
     */
    public abstract Value bind(ValueFactory vf, final IGASState<VS, ES, ST> state, Value v);
    
    /**
     * Call {@link #bind(ValueFactory, IGASState, Value)}.
     */
	@Override
    @SuppressWarnings("unchecked")
    public List<Value> bind(final ValueFactory vf, final IGASState<VS, ES, ST> state, 
    		final Value u, final IVariable<?>[] outVars, final IBindingSet bs) {
    	
    	final Value val = bind(vf, state, u);
    	
    	if (val == null) {
    		
    		return Collections.EMPTY_LIST;
    		
    	} else {
    	
    		return Arrays.asList(new Value[] { val });
    		
    	}
    	
    }

}

