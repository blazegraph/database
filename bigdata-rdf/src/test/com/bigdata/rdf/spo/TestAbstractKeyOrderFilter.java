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
 * Created on Jun 21, 2008
 */

package com.bigdata.rdf.spo;

import java.util.Iterator;

import junit.framework.TestCase2;

import com.bigdata.bop.Constant;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.Var;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.MockTermIdFactory;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.striterator.AbstractKeyOrder;
import com.bigdata.striterator.IKeyOrder;

/**
 * Test suite for
 * {@link AbstractKeyOrder#getFilteredKeyOrderIterator(IPredicate, Iterator)}.
 * 
 * @see <a href="http://trac.bigdata.com/ticket/1035" > DISTINCT PREDICATEs
 *      query is slow </a>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 *         FIXME Keep or drop?
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class TestAbstractKeyOrderFilter extends TestCase2 {

    /**
     * 
     */
    public TestAbstractKeyOrderFilter() {
    }

    /**
     * @param name
     */
    public TestAbstractKeyOrderFilter(String name) {
        super(name);
    }

    private MockTermIdFactory factory;
    
    @Override
    protected void setUp() throws Exception {
        
        super.setUp();
        
        factory = new MockTermIdFactory();
        
    }

    @Override
    protected void tearDown() throws Exception {

        super.tearDown();
        
        factory = null;
        
    }

    /** A mock IV. */
    private IV<?,?> tid(final long tidIsIgnored) {
        
        return factory.newTermId(VTE.URI);
        
    }
    
	/** A constant wrapping a mock IV. */
	private IConstant<IV> con(final long tidIsIgnored) {

		return new Constant(tid(tidIsIgnored));

	}
    
    /** An anonymous and distinct variable. */
	private IVariable<IV> var() {

    	return (IVariable<IV>) Var.var();
    	
	}

	private <E> Iterator<IKeyOrder<E>> getFilteredKeyOrderIterator(
			final IPredicate<E> pred,//
			final Iterator<IKeyOrder<E>> src//
			) {

		return AbstractKeyOrder.getFilteredKeyOrderIterator(pred, src);
		
	}
	
	/*
	 * Triples.
	 */
	
	/**
	 * All bound.
	 */
	public void test_keyOrder_triples_xxx() {

        final IVariableOrConstant<IV> S = con(1), P = con(2), O = con(3);

		final IPredicate pred = new SPOPredicate(getName(), S, P, O);

		assertSameIteratorAnyOrder(
				new IKeyOrder[] { SPOKeyOrder.SPO, SPOKeyOrder.POS,
						SPOKeyOrder.OSP },
				getFilteredKeyOrderIterator(pred,
						(Iterator) SPOKeyOrder.tripleStoreKeyOrderIterator()));
		
    }
    
    /**
     * Only S is a variable.
     */
	public void test_keyOrder_triples_Sxx() {
    	
        final IVariableOrConstant<IV> S = var(), P = con(2), O = con(3);

		final IPredicate pred = new SPOPredicate(getName(), S, P, O);

		assertSameIteratorAnyOrder(
				new IKeyOrder[] { SPOKeyOrder.POS },
				getFilteredKeyOrderIterator(pred,
						(Iterator) SPOKeyOrder.tripleStoreKeyOrderIterator()));
				
    }
    
    /**
     * Only P is a variable.
     */
	public void test_keyOrder_triples_xPx() {
    	
        final IVariableOrConstant<IV> S = con(1), P = var(), O = con(3);

		final IPredicate pred = new SPOPredicate(getName(), S, P, O);

		assertSameIteratorAnyOrder(
				new IKeyOrder[] { SPOKeyOrder.OSP },
				getFilteredKeyOrderIterator(pred,
						(Iterator) SPOKeyOrder.tripleStoreKeyOrderIterator()));
		
    }
    
    /**
     * Only O is a variable.
     */
	public void test_keyOrder_triples_xxO() {
    	
        final IVariableOrConstant<IV> S = con(1), P = con(2), O = var();

		final IPredicate pred = new SPOPredicate(getName(), S, P, O);

		assertSameIteratorAnyOrder(
				new IKeyOrder[] { SPOKeyOrder.SPO },
				getFilteredKeyOrderIterator(pred,
						(Iterator) SPOKeyOrder.tripleStoreKeyOrderIterator()));
		
    }

    /**
     * S and O are variables.
     */
	public void test_keyOrder_triples_SxO() {
   	
        final IVariableOrConstant<IV> S = var(), P = con(2), O = var();

		final IPredicate pred = new SPOPredicate(getName(), S, P, O);

		assertSameIteratorAnyOrder(
				new IKeyOrder[] { SPOKeyOrder.POS },
				getFilteredKeyOrderIterator(pred,
						(Iterator) SPOKeyOrder.tripleStoreKeyOrderIterator()));
		
    }
    
    /**
     * S and P are variables.
     */
	public void test_keyOrder_triples_SPx() {

        final IVariableOrConstant<IV> S = var(), P = var(), O = con(3);

		final IPredicate pred = new SPOPredicate(getName(), S, P, O);

		assertSameIteratorAnyOrder(
				new IKeyOrder[] { SPOKeyOrder.OSP },
				getFilteredKeyOrderIterator(pred,
						(Iterator) SPOKeyOrder.tripleStoreKeyOrderIterator()));
		
    }
    
    /**
     * P and O are variables.
     */
	public void test_keyOrder_triples_xPO() {

        final IVariableOrConstant<IV> S = con(1), P = var(), O = var();

		final IPredicate pred = new SPOPredicate(getName(), S, P, O);

		assertSameIteratorAnyOrder(
				new IKeyOrder[] { SPOKeyOrder.SPO },
				getFilteredKeyOrderIterator(pred,
						(Iterator) SPOKeyOrder.tripleStoreKeyOrderIterator()));

	}

	/*
	 * quads
	 */

	/**
	 * Quads - all bound.
	 */
	public void test_keyOrder_quads_xxxx() {

		final IVariableOrConstant<IV> S = con(1), P = con(2), O = con(3), C = con(4);

		final IPredicate pred = new SPOPredicate(getName(), S, P, O, C);

		assertSameIteratorAnyOrder(
				new IKeyOrder[] { //
						SPOKeyOrder.SPOC,//
						SPOKeyOrder.POCS,//
						SPOKeyOrder.OCSP,//
						SPOKeyOrder.CSPO,//
						SPOKeyOrder.PCSO,//
						SPOKeyOrder.SOPC,//
				},
				getFilteredKeyOrderIterator(pred,
						(Iterator) SPOKeyOrder.quadStoreKeyOrderIterator()));

	}

	/**
	 * Quads - S bound.
	 */
	public void test_keyOrder_quads_xPOC() {

		final IVariableOrConstant<IV> S = con(1), P = var(), O = var(), C = var();

		final IPredicate pred = new SPOPredicate(getName(), S, P, O, C);

		assertSameIteratorAnyOrder(
				new IKeyOrder[] { //
				SPOKeyOrder.SPOC,//
				SPOKeyOrder.SOPC,//
				},
				getFilteredKeyOrderIterator(pred,
						(Iterator) SPOKeyOrder.quadStoreKeyOrderIterator()));

	}

	/**
	 * Quads - P bound.
	 */
	public void test_keyOrder_quads_SxOC() {

		final IVariableOrConstant<IV> S = var(), P = con(2), O = var(), C = var();

		final IPredicate pred = new SPOPredicate(getName(), S, P, O, C);

		assertSameIteratorAnyOrder(
				new IKeyOrder[] { //
						SPOKeyOrder.POCS,//
						SPOKeyOrder.PCSO,//
				},
				getFilteredKeyOrderIterator(pred,
						(Iterator) SPOKeyOrder.quadStoreKeyOrderIterator()));

	}

	/**
	 * Quads - O bound.
	 */
	public void test_keyOrder_quads_SPxC() {

		final IVariableOrConstant<IV> S = var(), P = var(), O = con(3), C = var();

		final IPredicate pred = new SPOPredicate(getName(), S, P, O, C);

		assertSameIteratorAnyOrder(
				new IKeyOrder[] { //
						SPOKeyOrder.OCSP,//
				},
				getFilteredKeyOrderIterator(pred,
						(Iterator) SPOKeyOrder.quadStoreKeyOrderIterator()));

	}

	/**
	 * Quads - C bound.
	 */
	public void test_keyOrder_quads_SPOx() {

		final IVariableOrConstant<IV> S = var(), P = var(), O = var(), C = con(4);

		final IPredicate pred = new SPOPredicate(getName(), S, P, O, C);

		assertSameIteratorAnyOrder(
				new IKeyOrder[] { //
						SPOKeyOrder.CSPO,//
				},
				getFilteredKeyOrderIterator(pred,
						(Iterator) SPOKeyOrder.quadStoreKeyOrderIterator()));

	}

	/**
	 * Quads - SP bound.
	 */
	public void test_keyOrder_quads_xxOC() {

		final IVariableOrConstant<IV> S = con(1), P = con(2), O = var(), C = var();

		final IPredicate pred = new SPOPredicate(getName(), S, P, O, C);

		assertSameIteratorAnyOrder(
				new IKeyOrder[] { //
						SPOKeyOrder.SPOC,//
				},
				getFilteredKeyOrderIterator(pred,
						(Iterator) SPOKeyOrder.quadStoreKeyOrderIterator()));

	}

	/**
	 * Quads - SO bound.
	 */
	public void test_keyOrder_quads_xPxC() {

		final IVariableOrConstant<IV> S = con(1), P = var(), O = con(3), C = var();

		final IPredicate pred = new SPOPredicate(getName(), S, P, O, C);

		assertSameIteratorAnyOrder(
				new IKeyOrder[] { //
						SPOKeyOrder.SOPC,//
				},
				getFilteredKeyOrderIterator(pred,
						(Iterator) SPOKeyOrder.quadStoreKeyOrderIterator()));

	}

	/**
	 * Quads - SC bound.
	 */
	public void test_keyOrder_quads_xPOx() {

		final IVariableOrConstant<IV> S = con(1), P = var(), O = var(), C = con(4);

		final IPredicate pred = new SPOPredicate(getName(), S, P, O, C);

		assertSameIteratorAnyOrder(
				new IKeyOrder[] { //
						SPOKeyOrder.CSPO,//
				},
				getFilteredKeyOrderIterator(pred,
						(Iterator) SPOKeyOrder.quadStoreKeyOrderIterator()));

	}

	/**
	 * Quads - PO bound.
	 */
	public void test_keyOrder_quads_SxxC() {

		final IVariableOrConstant<IV> S = var(), P = con(1), O = con(2), C = var();

		final IPredicate pred = new SPOPredicate(getName(), S, P, O, C);

		assertSameIteratorAnyOrder(
				new IKeyOrder[] { //
						SPOKeyOrder.POCS,//
				},
				getFilteredKeyOrderIterator(pred,
						(Iterator) SPOKeyOrder.quadStoreKeyOrderIterator()));

	}

	/**
	 * Quads - PC bound.
	 */
	public void test_keyOrder_quads_SxOx() {

		final IVariableOrConstant<IV> S = var(), P = con(1), O = var(), C = con(4);

		final IPredicate pred = new SPOPredicate(getName(), S, P, O, C);

		assertSameIteratorAnyOrder(
				new IKeyOrder[] { //
						SPOKeyOrder.PCSO,//
				},
				getFilteredKeyOrderIterator(pred,
						(Iterator) SPOKeyOrder.quadStoreKeyOrderIterator()));

	}

	/**
	 * Quads - OC bound.
	 */
	public void test_keyOrder_quads_SPxx() {

		final IVariableOrConstant<IV> S = var(), P = var(), O = con(3), C = con(4);

		final IPredicate pred = new SPOPredicate(getName(), S, P, O, C);

		assertSameIteratorAnyOrder(
				new IKeyOrder[] { //
						SPOKeyOrder.OCSP,//
				},
				getFilteredKeyOrderIterator(pred,
						(Iterator) SPOKeyOrder.quadStoreKeyOrderIterator()));

	}

	/**
	 * Quads - POC bound.
	 */
	public void test_keyOrder_quads_Sxxx() {

		final IVariableOrConstant<IV> S = var(), P = con(2), O = con(3), C = con(4);

		final IPredicate pred = new SPOPredicate(getName(), S, P, O, C);

		assertSameIteratorAnyOrder(
				new IKeyOrder[] { //
						SPOKeyOrder.POCS,//
				},
				getFilteredKeyOrderIterator(pred,
						(Iterator) SPOKeyOrder.quadStoreKeyOrderIterator()));

	}

	/**
	 * Quads - SOC bound.
	 */
	public void test_keyOrder_quads_xPxx() {

		final IVariableOrConstant<IV> S = con(1), P = var(), O = con(3), C = con(4);

		final IPredicate pred = new SPOPredicate(getName(), S, P, O, C);

		assertSameIteratorAnyOrder(
				new IKeyOrder[] { //
						SPOKeyOrder.OCSP,//
				},
				getFilteredKeyOrderIterator(pred,
						(Iterator) SPOKeyOrder.quadStoreKeyOrderIterator()));

	}

	/**
	 * Quads - SPC bound.
	 */
	public void test_keyOrder_quads_xxOx() {

		final IVariableOrConstant<IV> S = con(1), P = con(2), O = var(), C = con(4);

		final IPredicate pred = new SPOPredicate(getName(), S, P, O, C);

		assertSameIteratorAnyOrder(
				new IKeyOrder[] { //
						SPOKeyOrder.CSPO,//
						SPOKeyOrder.PCSO,//
				},
				getFilteredKeyOrderIterator(pred,
						(Iterator) SPOKeyOrder.quadStoreKeyOrderIterator()));

	}

	/**
	 * Quads - SPO bound.
	 */
	public void test_keyOrder_quads_xxxC() {

		final IVariableOrConstant<IV> S = con(1), P = con(2), O = con(3), C = var();

		final IPredicate pred = new SPOPredicate(getName(), S, P, O, C);

		assertSameIteratorAnyOrder(
				new IKeyOrder[] { //
						SPOKeyOrder.SPOC,//
						SPOKeyOrder.SOPC,//
				},
				getFilteredKeyOrderIterator(pred,
						(Iterator) SPOKeyOrder.quadStoreKeyOrderIterator()));

	}

}

/*
SPOKeyOrder.SPOC,//
SPOKeyOrder.POCS,//
SPOKeyOrder.OCSP,//
SPOKeyOrder.CSPO,//
SPOKeyOrder.PCSO,//
SPOKeyOrder.SOPC,//
*/