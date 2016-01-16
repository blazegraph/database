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
 * Created on Aug 25, 2010
 */

package com.bigdata.bop.paths;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.relation.accesspath.IBlockingBuffer;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * An attempt to solve the zero length path problem with its own operator.
 * 
 * @deprecated Does not work. Leads to cardinality problems and can be removed.
 *             Zero Length Paths are integrated into the ALP node /
 *             ArbitraryLengthPathOp now.
 */
public class ZeroLengthPathOp extends PipelineOp {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends PipelineOp.Annotations {

    	/**
    	 * The left side of the zero-length path.
    	 */
        String LEFT_TERM = Annotations.class.getName() + ".leftTerm";

    	/**
    	 * The right side of the zero-length path.
    	 */
        String RIGHT_TERM = Annotations.class.getName() + ".rightTerm";
        
    }

    /**
     * Deep copy constructor.
     * 
     * @param op
     */
    public ZeroLengthPathOp(ZeroLengthPathOp op) {
        super(op);
    }

    /**
     * Shallow copy constructor.
     * 
     * @param args
     * @param annotations
     */
    public ZeroLengthPathOp(BOp[] args, Map<String, Object> annotations) {
        super(args, annotations);
    }

    public ZeroLengthPathOp(final BOp[] args, NV... annotations) {

        this(args, NV.asMap(annotations));
        
    }

    public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new ZeroLengthPathTask(this, context));

    }

    static private class ZeroLengthPathTask implements Callable<Void> {

        private final BOpContext<IBindingSet> context;
        
        private final IVariable<?> leftVar, rightVar;
        
        private final IConstant<?> leftConst, rightConst;
        
        ZeroLengthPathTask(final ZeroLengthPathOp op,
                final BOpContext<IBindingSet> context) {

            this.context = context;

            final IVariableOrConstant<?> leftTerm = (IVariableOrConstant<?>) op
                    .getProperty(Annotations.LEFT_TERM);
            
        	this.leftVar = leftTerm.isVar() ? (IVariable<?>) leftTerm : null;
        	
        	this.leftConst = leftTerm.isConstant() ? (IConstant<?>) leftTerm : null;

            final IVariableOrConstant<?> rightTerm = (IVariableOrConstant<?>) op
                    .getProperty(Annotations.RIGHT_TERM);
            
            this.rightVar = rightTerm.isVar() ? (IVariable<?>) rightTerm : null;
            
            this.rightConst = rightTerm.isConstant() ? (IConstant<?>) rightTerm : null;
            
        	if (leftConst != null && rightConst != null) {
        		
        		throw new IllegalArgumentException("must be a variable on at least one side");
        		
        	}
        	
        }

        public Void call() throws Exception {

            // source.
            final ICloseableIterator<IBindingSet[]> source = context
                    .getSource();

            // default sink
            final IBlockingBuffer<IBindingSet[]> sink = context.getSink();
            
            try {

            	while (source.hasNext()) {
            		
            		final IBindingSet[] chunk = source.next();
            		
            		final IBindingSet[] chunkOut = processChunk(chunk);
            		
        			sink.add(chunkOut);
            		
            	}
            	
                // flush the sink.
                sink.flush();

                // Done.
                return null;
                
            } finally {
                
                sink.close();
                
                source.close();
                
            }

        }
        
        @SuppressWarnings("unchecked")
		private IBindingSet[] processChunk(final IBindingSet[] chunk) {
        	
    		final IBindingSet[] chunkOut = new IBindingSet[chunk.length];
    		
    		int j = 0;
    		for (int i = 0; i < chunk.length; i++) {
    			
    			final IBindingSet bs = chunk[i].clone();
    			
    			final Gearing gearing = getGearing(bs);
    			
    			if (gearing == null) {
    				
    				// neither side of the zero-length path is bound
    				return new IBindingSet[0];
    				
    			}
    			
    			// first check to see if the variable side is already bound
    			if (bs.isBound(gearing.var)) {
    				
    				/*
    				 * If it has a value that is not equals to the constant 
    				 * side then we filter out the solution (by not adding it
    				 * to chunkOut).
    				 */
    				
    				if (!bs.get(gearing.var).equals(gearing.constant)) {
    					
    					continue;
    					
    				}
    				
    			} else {
    				
    				// create a zero length path
    				bs.set(gearing.var, gearing.constant);
    				
    			}
    			
    			chunkOut[j++] = bs;
    			
    		}
    		
    		if (j != chunk.length) {
    			
    			final IBindingSet[] tmp = new IBindingSet[j];
    			
    			System.arraycopy(chunkOut, 0, tmp, 0, j);
    			
    			return tmp;
    			
    		} else {
    			
    			return chunkOut;
    			
    		}
    			
        }
        
        private Gearing getGearing(final IBindingSet bs) {
        	
        	if (leftConst != null) {
        		
        		return new Gearing(rightVar, leftConst);
        		
        	} else if (rightConst != null) {
        		
        		return new Gearing(leftVar, rightConst);
        		
        	} else { // both left and right are vars

        		if (bs.isBound(this.leftVar)) {
        			
        			return new Gearing(this.rightVar, bs.get(this.leftVar));
        			
        		} else if (bs.isBound(this.rightVar)) {
        			
        			return new Gearing(this.leftVar, bs.get(this.rightVar));
        			
        		} else {
        			
        			return null;
        			
        		}
        		
        	}
        	
        }
        
        private class Gearing {
        	
        	final public IVariable<?> var;
        	
        	final public IConstant<?> constant;
        	
        	public Gearing(final IVariable<?> var, final IConstant<?> constant) {
        		
        		this.var = var;
        		this.constant = constant;
        		
        	}
        	
        }

    } // class ZeroLengthPathTask

}
