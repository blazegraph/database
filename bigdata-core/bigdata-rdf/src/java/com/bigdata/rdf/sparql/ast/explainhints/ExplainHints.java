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
 * Created on Aug 28, 2015
 */
package com.bigdata.rdf.sparql.ast.explainhints;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.rdf.sparql.ast.ASTBase;
import com.bigdata.rdf.sparql.ast.ASTBase.Annotations;

import cutthecrap.utils.striterators.Expander;
import cutthecrap.utils.striterators.Filter;
import cutthecrap.utils.striterators.SingleValueIterator;
import cutthecrap.utils.striterators.Striterator;

/**
 * A list of {@link IExplainHint}s, to be attached as an annotation to an
 * {@link ASTBase} node. See {@link IExplainHint} interface for description
 * of explain hints. This class is merely a wrapper allowing to add a set
 * of explain hints to a single node.
 * 
 * @author <a href="ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class ExplainHints implements Iterable<IExplainHint> {

   private final Set<IExplainHint> explainHints;
   
   /**
    * Constructor, setting up an explain hints object containing
    * a single {@link IExplainHint}.
    */
   public ExplainHints(final IExplainHint explainHint) {
      explainHints = new HashSet<IExplainHint>();
      explainHints.add(explainHint);
   }

   /**
    * Add an explain hint to the set of hints.
    */
   public void addExplainHint(final IExplainHint explainHint) {
      explainHints.add(explainHint);
   }
   

   /**
    * Utility function to remove explain hint annotations from a BOp. For
    * use in test case to ease comparison. Be careful, since this modifies
    * the argument.
    * 
    * @param astBase
    */
   public static void removeExplainHintAnnotationsFromBOp(final BOp bop) {

      final Iterator<BOp> explainHintAnnotatedBOps = 
         ExplainHints.explainHintAnnotatedBOpIterator(bop);
      
      // collect explain hint nodesWithExplainHints nodes
      final List<BOp> nodesWithExplainHints = new ArrayList<BOp>();
      while (explainHintAnnotatedBOps.hasNext()) {
         nodesWithExplainHints.add(explainHintAnnotatedBOps.next());
      }
      
      for (final BOp nodeWithExplainHint : nodesWithExplainHints) {
         nodeWithExplainHint.setProperty(Annotations.EXPLAIN_HINTS, null);
      }
   }

   /**
    * Returns all {@link BOp}s that are annotated with {@link ExplainHints}.
    * 
    * @param op An operator.
    * 
    * @return The respective iterator.
    */
   @SuppressWarnings({ "unchecked" })
   public static Iterator<BOp> explainHintAnnotatedBOpIterator(final BOp astBase) {
      
      return new Striterator(
         
         BOpUtility.preOrderIterator(astBase)).addFilter(new Expander() {

            private static final long serialVersionUID = 1L;
   
            @SuppressWarnings("rawtypes")
            @Override
            protected Iterator expand(final Object arg0) {
   
               final BOp op = (BOp)arg0;
   
               // visit the node.
               final Striterator itr = 
                  new Striterator(new SingleValueIterator(op));
   
               // visit the node's operator annotations.
               final Striterator itr2 = new Striterator(
                  BOpUtility.annotationOpIterator(op));
   
               // expand each operator annotation with a pre-order traversal.
               itr2.addFilter(new Expander() {
                  private static final long serialVersionUID = 1L;
   
                  @Override
                  protected Iterator expand(final Object ann) {
                     return explainHintAnnotatedBOpIterator((BOp) ann);
                  }
                    
               });
                
               // append the pre-order traversal of each annotation.
               itr.append(itr2);
   
               return itr;
            }
            
         }).addFilter(new Filter() {
         
            private static final long serialVersionUID = -6488014508516304898L;

            @Override
            public boolean isValid(Object obj) {
               
               if (obj instanceof BOp) {
                  final BOp bop = (BOp)obj;
                  return bop.getProperty(Annotations.EXPLAIN_HINTS)!=null;
               }
               
               return false;
            }
         
      });
   }

   @Override
   public String toString() {
      return explainHints.toString();
   }
   
   @Override
   public Iterator<IExplainHint> iterator() {
      return explainHints.iterator();
   }
   
}
