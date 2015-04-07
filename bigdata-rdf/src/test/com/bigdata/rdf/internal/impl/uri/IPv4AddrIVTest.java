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
package com.bigdata.rdf.internal.impl.uri;

import java.net.UnknownHostException;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.junit.Test;

import com.bigdata.rdf.model.BigdataLiteral;

public class IPv4AddrIVTest extends TestCase {

	private final static transient Logger log = Logger
			.getLogger(IPv4AddrIVTest.class);

	@Test
	public void testIPAddress() {
		String testCase = "192.168.1.100";

		@SuppressWarnings("unused")
		IPv4AddrIV<BigdataLiteral> addr = null;
		
		try {
			addr = new IPv4AddrIV<BigdataLiteral>(testCase);
		} catch (UnknownHostException uh)
		{
			log.info(uh);
			fail();
		}
		
		assert(true);
		
	}
	
	@Test
	public void testIPAddress1() {
		String testCase = "66.249.211.254";

		@SuppressWarnings("unused")
		IPv4AddrIV<BigdataLiteral> addr = null;
		
		try {
			addr = new IPv4AddrIV<BigdataLiteral>(testCase);
		} catch (UnknownHostException uh)
		{
			log.info(uh);
			fail();
		}
		
		assert(true);
		
	}

	@Test
	public void testIPAddress2() {
		String testCase = "66.249.71.255";

		@SuppressWarnings("unused")
		IPv4AddrIV<BigdataLiteral> addr = null;
		
		try {
			addr = new IPv4AddrIV<BigdataLiteral>(testCase);
		} catch (UnknownHostException uh)
		{
			log.info(uh);
			fail();
		}
		
		assert(true);
		
	}

	@Test
	public void testIPAddress3() {
		String testCase = "66.249.71.253/32";

		@SuppressWarnings("unused")
		IPv4AddrIV<BigdataLiteral> addr = null;
		
		try {
			addr = new IPv4AddrIV<BigdataLiteral>(testCase);
		} catch (UnknownHostException uh)
		{
			log.info(uh);
			fail();
		}
		
		assert(true);
		
	}

	@Test
	public void testIPAddressCIDR() {
		String testCase = "192.168.1.100/32";
		
		boolean test = true;
		
		@SuppressWarnings("unused")
		IPv4AddrIV<BigdataLiteral> addr = null;
		
		try {
			addr = new IPv4AddrIV<BigdataLiteral>(testCase);
		} catch (UnknownHostException uh)
		{
			log.info(uh);
			test = false;
		}
		
		assertEquals(true, test);
		

		
	}

	@Test
	public void testIPAddressCIDR2() {
		String testCase = "192.168.1.100/24";
		
		boolean test = true;
		
		@SuppressWarnings("unused")
		IPv4AddrIV<BigdataLiteral> addr = null;
		
		try {
			addr = new IPv4AddrIV<BigdataLiteral>(testCase);
		} catch (UnknownHostException uh)
		{
			log.info(uh);
			test = false;
		}
		
		assertEquals(true, test);
		
	}
		

	@Test
	public void testIPAddressBadCIDR() {
		String testCase = "192.168.1.100/323";
		
		boolean test = false;
		
		@SuppressWarnings("unused")
		IPv4AddrIV<BigdataLiteral> addr = null;
		
		try {
			addr = new IPv4AddrIV<BigdataLiteral>(testCase);
		} catch (UnknownHostException uh)
		{
			log.info(uh);
			test = true;
		}
		
		assertEquals(test,true);
		
	}

	@Test
	public void testIPAddressBad() {
		String testCase = "192.168.1.300";
		
		boolean test = false;
		
		@SuppressWarnings("unused")
		IPv4AddrIV<BigdataLiteral> addr = null;
		
		try {
			addr = new IPv4AddrIV<BigdataLiteral>(testCase);
		} catch (UnknownHostException uh)
		{
			log.info(uh);
			test = true;
		}
		
		assertEquals(test,true);
	}

	@Test
	public void testIPAddressBad2() {
		String testCase = "192.168.1";
		
		boolean test = false;
		
		@SuppressWarnings("unused")
		IPv4AddrIV<BigdataLiteral> addr = null;
		
		try {
			addr = new IPv4AddrIV<BigdataLiteral>(testCase);
		} catch (UnknownHostException uh)
		{
			log.info(uh);
			test = true;
		}
		
		assertEquals(test,true);
	}

	@Test
	public void testDeviceAddress() {
		String testCase = "dev:1434498383:110:1427217760000";
		
		boolean test = false;
		
		@SuppressWarnings("unused")
		IPv4AddrIV<BigdataLiteral> addr = null;
		
		try {
			addr = new IPv4AddrIV<BigdataLiteral>(testCase);
		} catch (UnknownHostException uh)
		{
			log.info(uh);
			test = true;
		}
		
		assertEquals(test,true);
	}
	
	

}