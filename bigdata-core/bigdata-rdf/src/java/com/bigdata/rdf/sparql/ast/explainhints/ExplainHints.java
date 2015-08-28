/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
 * Created on Aug 28, 2015
 */
package com.bigdata.rdf.sparql.ast.explainhints;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.bigdata.rdf.sparql.ast.ASTBase;

/**
 * A list of {@link IExplainHint}s, to be attached as an annotation to an
 * {@link ASTBase} node. See {@link IExplainHint} interface for description
 * of explain hints. This class is merely a wrapper allowing to add a set
 * of explain hints to a single node.
 * 
 * @author <a href="ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class ExplainHints {

   private final Set<IExplainHint> explainHints;
   
   /**
    * Internal, empty constructor
    */
   protected ExplainHints() {
      explainHints = new HashSet<IExplainHint>();
   }
   
   /**
    * Constructor, setting up an explain hints object with a single object
    */
   public ExplainHints(final IExplainHint explainHint) {
      this();
      explainHints.add(explainHint);
   }

   /**
    * Add an explain hint to the set of hints.
    */
   public void addExplainHint(final IExplainHint explainHint) {
      explainHints.add(explainHint);
   }

   /**
    * @return the explain hints as unmodifiable object
    */
   public Set<IExplainHint> getExplainHints() {
      return Collections.unmodifiableSet(explainHints);
   }
   
}
