/*
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
package com.bigdata.ganglia;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.TestCase;

import com.bigdata.ganglia.xdr.XDROutputBuffer;

/**
 * Test suite for encode/decode of the Ganglia 3.1 wire format messages. The
 * messages in this test suite were derived from a mixture of capture (via
 * {@link #main(String[])} and mock messages generated using
 * <code>gmetric</code> with known characteristics.
 */
public class TestGangliaMessageEncodeDecode31 extends TestCase {

	public TestGangliaMessageEncodeDecode31() {
	}

	public TestGangliaMessageEncodeDecode31(final String name) {
		super(name);
	}
	
	/**
	 * Unit test for a heartbeat message.
	 */
	public void test_metric_heartbeat_uint32() {

		// declared-by: GangliaMetadataMessage{ recordType=METADATA,
		// hostName=bigdata10, metricName=heartbeat, spoof=false,
		// metricType=UINT32, metricName2=heartbeat, units=, slope=unspecified,
		// tmax=20, dmax=0,
		// extraValues={TITLE="heartbeat"},DESC="Last heartbeat"},GROUP="core"}}}
		final IGangliaMetadataMessage decl = new GangliaMetadataMessage(
				"bigdata10",//hostName
				"heartbeat",//metricName
				false,// spoof
				GangliaMessageTypeEnum.UINT32,//metricType
				"heartbeat",//metricName2
				"",//units
				GangliaSlopeEnum.unspecified,//slope
				20,//tmax
				0,//dmax
				AbstractMetrics.getMap(IGangliaAttributes.GROUP_CORE,
						"heartbeat", "Last heartbeat")
				);

		assertEncodeDecode(null/* decl */, decl);
		
		// GangliaMetricMessage{recordType=UINT32, hostName=bigdata10,
		// metricName=heartbeat, spoof=false, format=%u,
		// value=1326920615 (valueClass=Long)}
		final IGangliaMetricMessage expected = new GangliaMetricMessage(
				GangliaMessageTypeEnum.UINT32,//recordType
				"bigdata10",//hostName
				"heartbeat",// metricName
				false, // spoof
				"%u", // format
				Long.valueOf(1326920615L)
				);

		final byte[] actualData = assertEncodeDecode(decl, expected);

		final byte[] expectedData = new byte[] { 0, 0, 0, -124, 0, 0, 0, 9, 98,
				105, 103, 100, 97, 116, 97, 49, 48, 0, 0, 0, 0, 0, 0, 9, 104,
				101, 97, 114, 116, 98, 101, 97, 116, 0, 0, 0, 0, 0, 0, 0, 0, 0,
				0, 2, 37, 117, 0, 0, 79, 23, 51, -89 };

		if (!Arrays.equals(expectedData, actualData)) {

			fail("expect: " + Arrays.toString(expectedData) + ", actual="
					+ Arrays.toString(actualData));
			
		}
		
//		declared-by: GangliaMetadataMessage{ recordType=METADATA, hostName=bigdata10, metricName=heartbeat, spoof=false, metricType=UINT32, metricName2=heartbeat, units=, slope=unspecified, tmax=20, dmax=0, extraValues={TITLE="heartbeat",DESC="Last heartbeat",GROUP="core"}}
//		GangliaMetricMessage{recordType=UINT32, hostName=bigdata10, metricName=heartbeat, spoof=false, format=%u, value=1326920615 (valueClass=Long)}
//		RECEIVED RECORD : [0, 0, 0, -124, 0, 0, 0, 9, 98, 105, 103, 100, 97, 116, 97, 49, 48, 0, 0, 0, 0, 0, 0, 9, 104, 101, 97, 114, 116, 98, 101, 97, 116, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 37, 117, 0, 0, 79, 23, 51, -89]

	}
	
	/**
	 * Unit test for a cpu_num message.
	 */
	public void test_metric_cpu_num_uint16() {

		// declared-by: GangliaMetadataMessage{ recordType=METADATA,
		// hostName=bigdata10, metricName=cpu_num, spoof=false,
		// metricType=UINT16, metricName2=cpu_num, units=CPUs, slope=zero,
		// tmax=1200, dmax=0,
		// extraValues={TITLE="CPU Count",DESC="Total number of CPUs",GROUP="cpu"}}
		final IGangliaMetadataMessage decl = new GangliaMetadataMessage(
				"bigdata10",//hostName
				"cpu_num",//metricName
				false,// spoof
				GangliaMessageTypeEnum.UINT16,//metricType
				"cpu_num",//metricName2
				"CPUs",//units
				GangliaSlopeEnum.zero,//slope
				1200,//tmax
				0,//dmax
				AbstractMetrics.getMap(IGangliaAttributes.GROUP_CPU,
						"CPU Count", "Total number of CPUs")
				);

		assertEncodeDecode(null/* decl */, decl);
		
		// GangliaMetricMessage{recordType=UINT16, hostName=bigdata10,
		// metricName=cpu_num, spoof=false, format=%hu, value=4
		// (valueClass=Integer)}
		final IGangliaMetricMessage expected = new GangliaMetricMessage(
				GangliaMessageTypeEnum.UINT16,//recordType
				"bigdata10",//hostName
				"cpu_num",// metricName
				false, // spoof
				"%hu", // format
				Integer.valueOf(4)
				);

		final byte[] actualData = assertEncodeDecode(decl, expected);

		final byte[] expectedData = new byte[] { 0, 0, 0, -127, 0, 0, 0, 9, 98,
				105, 103, 100, 97, 116, 97, 49, 48, 0, 0, 0, 0, 0, 0, 7, 99,
				112, 117, 95, 110, 117, 109, 0, 0, 0, 0, 0, 0, 0, 0, 3, 37,
				104, 117, 0, 0, 0, 0, 4 };
		
		if (!Arrays.equals(expectedData, actualData)) {

			fail("expect: " + Arrays.toString(expectedData) + ", actual="
					+ Arrays.toString(actualData));
			
		}
		
	}

	public void test_metric_cpu_speed_uint32() {

		// declared-by: GangliaMetadataMessage{ recordType=METADATA,
		// hostName=bigdata10, metricName=cpu_speed, spoof=false,
		// metricType=UINT32, metricName2=cpu_speed, units=MHz, slope=zero,
		// tmax=1200, dmax=0,
		// extraValues={TITLE="CPU Speed",DESC="CPU Speed in terms of MHz",GROUP="cpu"}}
		final IGangliaMetadataMessage decl = new GangliaMetadataMessage(
				"bigdata10",// hostName
				"cpu_speed",// metricName
				false,// spoof
				GangliaMessageTypeEnum.UINT32,// metricType
				"cpu_speed",// metricName2
				"MHz",// units
				GangliaSlopeEnum.zero,// slope
				1200,// tmax
				0,// dmax
				AbstractMetrics.getMap(IGangliaAttributes.GROUP_CPU,
						"CPU Speed", "CPU Speed in terms of MHz"));

		assertEncodeDecode(null/* decl */, decl);

		// GangliaMetricMessage{recordType=UINT32, hostName=bigdata10,
		// metricName=cpu_speed, spoof=false, format=%u, value=2701
		// (valueClass=Long)}
		final IGangliaMetricMessage expected = new GangliaMetricMessage(
				GangliaMessageTypeEnum.UINT32,//recordType
				"bigdata10",//hostName
				"cpu_speed",// metricName
				false, // spoof
				"%u", // format
				Long.valueOf(2701)
				);

		final byte[] actualData = assertEncodeDecode(decl, expected);

		final byte[] expectedData = new byte[] { 0, 0, 0, -124, 0, 0, 0, 9, 98,
				105, 103, 100, 97, 116, 97, 49, 48, 0, 0, 0, 0, 0, 0, 9, 99,
				112, 117, 95, 115, 112, 101, 101, 100, 0, 0, 0, 0, 0, 0, 0, 0,
				0, 0, 2, 37, 117, 0, 0, 0, 0, 10, -115 };
		
		if (!Arrays.equals(expectedData, actualData)) {

			fail("expect: " + Arrays.toString(expectedData) + ", actual="
					+ Arrays.toString(actualData));
			
		}

	}
	
	public void test_metric_mem_free_float() {

		// declared-by: GangliaMetadataMessage{ recordType=METADATA,
		// hostName=bigdata11, metricName=mem_free, spoof=false,
		// metricType=FLOAT, metricName2=mem_free, units=KB, slope=both,
		// tmax=180, dmax=0,
		// extraValues={TITLE="Free Memory",DESC="Amount of available memory",GROUP="memory"}}
		final IGangliaMetadataMessage decl = new GangliaMetadataMessage(
				"bigdata11",// hostName
				"mem_free",// metricName
				false,// spoof
				GangliaMessageTypeEnum.FLOAT,// metricType
				"mem_free",// metricName2
				"KB",// units
				GangliaSlopeEnum.both,// slope
				180,// tmax
				0,// dmax
				AbstractMetrics.getMap(IGangliaAttributes.GROUP_MEMORY,
						"Free Memory", "Amount of available memory"));

		assertEncodeDecode(null/* decl */, decl);

		// GangliaMetricMessage{recordType=FLOAT, hostName=bigdata11,
		// metricName=mem_free, spoof=false, format=%.0f, value=1.5857544E7
		// (valueClass=Float)}
		final IGangliaMetricMessage expected = new GangliaMetricMessage(
				GangliaMessageTypeEnum.FLOAT,//recordType
				"bigdata11",//hostName
				"mem_free",// metricName
				false, // spoof
				"%.0f", // format
				Float.valueOf("1.5857544E7")
				);

		final byte[] actualData = assertEncodeDecode(decl, expected);

		final byte[] expectedData=
//		new byte[] { 0, 0, 0, -124, 0, 0, 0, 9, 98,
//				105, 103, 100, 97, 116, 97, 49, 48, 0, 0, 0, 0, 0, 0, 9, 99,
//				112, 117, 95, 115, 112, 101, 101, 100, 0, 0, 0, 0, 0, 0, 0, 0,
//				0, 0, 2, 37, 117, 0, 0, 0, 0, 10, -115 };
		new byte[]{0, 0, 0, -122,
				 0, 0, 0, 9, 98, 105, 103, 100, 97, 116, 97, 49, 49, 0, 0, 0, 0, 0, 0, 8,
				 109, 101, 109, 95, 102, 114, 101, 101, 0, 0, 0, 0, 0, 0, 0, 4, 37, 46,
				 48, 102, 75, 113, -9, -120};
		
		if (!Arrays.equals(expectedData, actualData)) {

			fail("expect: " + Arrays.toString(expectedData) + ", actual="
					+ Arrays.toString(actualData));
			
		}

	}
	
	/**
	 * Verify that we can encode and decode a record.
	 * 
	 * @param decl
	 *            The metadata declaration IFF the <i>expected</i> record is an
	 *            {@link IGangliaMetricMessage} and <code>null</code> otherwise.
	 * @param expected
	 *            The record to be encoded and decoded.
	 * 
	 * @return The encoded record.
	 */
	protected byte[] assertEncodeDecode(final IGangliaMetadataMessage decl,
			final IGangliaMessage expected) {

		if (expected == null)
			throw new IllegalArgumentException();

		// TODO move to setUp() / clear in tearDown().
		final XDROutputBuffer xdr = new XDROutputBuffer(
				IGangliaDefaults.BUFFER_SIZE);
		final GangliaMessageEncoder31 messageEncoder = new GangliaMessageEncoder31();
		final GangliaMessageDecoder31 messageDecoder = new GangliaMessageDecoder31();

		if (expected.isMetricRequest()) {
		
			// Encode request message.
			messageEncoder.writeRequest(xdr,
					(IGangliaRequestMessage) expected);
		
		} else if (expected.isMetricMetadata()) {
			
			// Encode metadata message.
			messageEncoder.writeMetadata(xdr,
					(IGangliaMetadataMessage) expected);

		} else if (expected.isMetricValue()) {

			/*
			 * Note: requires metric metadata record for metric value
			 * encode/decode.
			 */
			if (decl == null)
				throw new IllegalArgumentException(
						"Declaration required for metric value record");

			// Encode the metric value record.
			messageEncoder.writeMetric(xdr, decl,
					(IGangliaMetricMessage) expected);
			
		} else {

			throw new AssertionError();
			
		}

		// Decode the message.
		final IGangliaMessage actual = messageDecoder.decode(xdr.getBuffer(),
				0/* offset */, xdr.getLength());
		
		// Verify message compares as equal().
		assertEquals("messages not equal()", expected, actual);

		/*
		 * Return an exact length copy of the encoded record.
		 */
		final byte[] actualData = new byte[xdr.getLength()];
		
		System.arraycopy(xdr.getBuffer()/* src */, 0/* srcPos */,
				actualData/* dest */, 0/* destPos */, actualData.length);
		
		return actualData;
	}

	/**
	 * Utility for capturing data records from the wire and identifying errors
	 * in decode/encode of {@link IGangliaMessage}. This captures both the
	 * packet and the decoded record. If there is a decode/encode problem, then
	 * the raw packet data, the relevant metadata declaration (if any), the
	 * decoded {@link IGangliaMessage}, and the re-encoded raw packet data are
	 * all written to <code>stdout</code>.
	 * <p>
	 * This utility can be used to verify encode/decode on a running ganglia
	 * cluster. Just start the utility and let it run. It will observe all
	 * ganglia traffic and report problems if it is unable to decode/encode any
	 * packets. The utility by itself is just a listener. It will not send out
	 * any packets. You can bounce some <code>gmond</code> instances on the
	 * cluster to help drive the ganglia network traffic and ensure that this
	 * utility sees all messages which the ganglia network is capable of
	 * producing.
	 * <p>
	 * Note: Until we have the metadata record, we will not be able to round
	 * trip a metric value record and hence can not report any errors in the
	 * metric record round trip until we have picked up the metadata declaration
	 * for a given metric. So, you can run this a long time to be assured that
	 * everything is Ok or you can just bounce once of the <code>gmond</code>
	 * instances on the cluster.
	 * <p>
	 * Note: You can also mock up specific messages using <code>gmetric</code>
	 * and use this utility to observe their decode.
	 * 
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

		final String hostName = GangliaService.getCanonicalHostName();

		final IGangliaMessageHandler handler = new IGangliaMessageHandler() {

			@Override
			public void accept(final IGangliaMessage msg) {
				// NOP
			}

		};

		// Used to identify cases where round trip fails.
		final GangliaMessageEncoder31 messageGenerator = new GangliaMessageEncoder31();

		final GangliaState gangliaState = new GangliaState(hostName,
				new NOPMetadataFactory());
		
		final XDROutputBuffer xdr = new XDROutputBuffer(IGangliaDefaults.BUFFER_SIZE);

		final AtomicLong nreceived = new AtomicLong();

		final AtomicLong nerrors = new AtomicLong();
		
		final GangliaListener listener = new GangliaListener(handler) {
			
            protected IGangliaMessage decodeRecord(final byte[] data,
                    final int off, final int len) {
				
                final IGangliaMessage msg = super.decodeRecord(data, off, len);

				if(msg != null) {
				
                    accept(data, off, len, msg, gangliaState, messageGenerator,
                            xdr, nreceived, nerrors);

				}

				return msg;
				
			}
		};

		System.out.println("Listening for packets.");
		
		// runs until killed.
		listener.call();

	}
	
    static protected void accept(final byte[] data, final int off,
            final int len, final IGangliaMessage msg,
            final GangliaState gangliaState,
            final GangliaMessageEncoder31 messageGenerator,
            final XDROutputBuffer xdr, final AtomicLong nreceived,
            final AtomicLong nerrors) {

        nreceived.incrementAndGet();
		
		final byte[] expected = new byte[len];
		
        System.arraycopy(data,/* src */
                off/* srcPos */, expected/* dst */, 0/* destPos */,
                expected.length);

		/*
		 * Note: In order to test regeneration of the message we need to track
		 * the metadata declarations so we have it on hand when we need to
		 * generate a metric value record.
		 */
		final byte[] actual;
		{

			if (msg.isMetricMetadata()) {

				/*
				 * Add it to the state so we can resolve it when we need to
				 * round trip a metric message below.
				 */
				gangliaState.putIfAbsent((IGangliaMetadataMessage) msg);

				/*
				 * Format the message and extract it into a byte[].
				 */
				messageGenerator.writeMetadata(xdr,
						(IGangliaMetadataMessage) msg);

				actual = new byte[xdr.getLength()];

				System.arraycopy(xdr.getBuffer()/* src */, 0/* srcPos */,
						actual/* dst */, 0/* destPos */, actual.length);

			} else if (msg.isMetricRequest()) {

				/*
				 * Format the message and extract it into a byte[].
				 */
				messageGenerator
						.writeRequest(xdr, (IGangliaRequestMessage) msg);

				actual = new byte[xdr.getLength()];

				System.arraycopy(xdr.getBuffer()/* src */, 0/* srcPos */,
						actual/* dst */, 0/* destPos */, actual.length);

			} else if (msg.isMetricValue()) {

				final IGangliaMetadataMessage decl = gangliaState
						.getMetadata(msg.getMetricName());

				if (decl != null) {

					/*
					 * Format the message and extract it into a byte[].
					 */
					messageGenerator.writeMetric(xdr, decl,
							(IGangliaMetricMessage) msg);

					actual = new byte[xdr.getLength()];

					System.arraycopy(xdr.getBuffer()/* src */, 0/* srcPos */,
							actual/* dst */, 0/* destPos */, actual.length);

				} else {

					/*
					 * We can not encode a metric value record w/o a
					 * declaration.
					 */
					actual = null;

				}

			} else {

				// Unknown message type.
				throw new AssertionError();

			}

		}

		/*
		 * Write out the message.
		 */
		
		final String okStr  = "RECEIVED RECORD : ";
		final String errStr = "PRODUCED RECORD : ";

		final boolean roundTripError;

		if (actual != null && !Arrays.equals(expected, actual)) {

			nerrors.incrementAndGet();

			roundTripError = true;

		} else {

			roundTripError = false;

		}

		/*
		 * Note: use || true if you want to see everthing and || false to just
		 * see the errors.
		 * 
		 * Note: Everything is on stdout so the lines do not get out of order.
		 */
		if (roundTripError || false) {

			System.out.println("--- nreceived=" + nreceived + ", nerrors="
					+ nerrors + "---");

			if (msg.isMetricValue()) {

				/*
				 * The metadata message which provides the declaration for that
				 * metric record.
				 */
				final IGangliaMetadataMessage decl = gangliaState
						.getMetadata(msg.getMetricName());

				// The declaration.
				System.out.println("declared-by: " + decl.toString());

			}

			// The decoded message.
			System.out.println(msg.toString());

			// The raw bytes.
			System.out.println(okStr + Arrays.toString(expected));

			if (roundTripError) {

				/*
				 * If we fail to encode record the record correctly, then write
				 * out the data that we generated when we tried to encode the
				 * record.
				 */

				System.out.println(errStr + Arrays.toString(actual));

			}

		}

	}

}
