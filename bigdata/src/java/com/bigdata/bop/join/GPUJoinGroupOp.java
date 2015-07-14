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
 * Created on July 14, 2015
 */

package com.bigdata.bop.join;

import java.util.Map;
import java.util.concurrent.FutureTask;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;

/**
 * Pipeline operator for join group operation via Mapgraph/GPU.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class GPUJoinGroupOp<E> extends PipelineOp {

//	static private final transient Logger log = Logger
//			.getLogger(GPUJoinGroupOp.class);


	private static final long serialVersionUID = 1L;

	public interface Annotations {

		/**
		 * Annotation for join groups to enable evaluation of a join group via GPU
		 */
		String EVALUATE_ON_GPU = GPUJoinGroupOp.class.getName() + ".evaluateOnGPU";

		boolean DEFAULT_EVALUATE_ON_GPU = false;
		
	}

	/**
	 * Deep copy constructor.
	 * 
	 * @param op
	 */
	public GPUJoinGroupOp(final GPUJoinGroupOp<E> op) {
		super(op);
	}

	/**
	 * Shallow copy vararg constructor.
	 * 
	 * @param args
	 * @param annotations
	 */
	public GPUJoinGroupOp(final BOp[] args, final NV... annotations) {

		this(args, NV.asMap(annotations));

	}

	/**
	 * Shallow copy constructor.
	 * 
	 * @param args
	 * @param annotations
	 */
	public GPUJoinGroupOp(final BOp[] args, final Map<String, Object> annotations) {

		super(args, annotations);

	}

   @Override
   public FutureTask<Void> eval(BOpContext<IBindingSet> context) {
      // TODO implement
      return null;
   }


}
