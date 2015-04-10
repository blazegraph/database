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
package com.bigdata.rdf.graph.impl;

import org.openrdf.model.Statement;

import com.bigdata.rdf.graph.IGASContext;
import com.bigdata.rdf.graph.IGASState;

import cutthecrap.utils.striterators.Filter;

/**
 * Filter visits only edges (filters out attribute values).
 * <p>
 * Note: This filter is pushed down onto the AP and evaluated close to the data.
 */
public class EdgeOnlyFilter<VS, ES, ST> extends Filter {

    private static final long serialVersionUID = 1L;
    
    private final IGASState<VS, ES, ST> gasState;

    public EdgeOnlyFilter(final IGASContext<VS, ES, ST> ctx) {
    
        this.gasState = ctx.getGASState();
        
    }

    @Override
    public boolean isValid(final Object e) {

        return gasState.isEdge((Statement) e);
        
    }
    
}
