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

import java.util.Map;

import com.bigdata.ganglia.util.UnsignedUtil;
import com.bigdata.ganglia.xdr.XDROutputBuffer;

/**
 * Class for generating Ganglia 3.1 protocol messages.
 * 
 * @see <code>ganglia/lib/gm_protocol.x</code>
 */
public class GangliaMessageEncoder31 implements IGangliaMessageEncoder {

//	private static final Logger log = Logger
//			.getLogger(GangliaMessageGenerator.class);

	public void writeRequest(final XDROutputBuffer xdr,
			final IGangliaRequestMessage msg) {

		xdr.reset();
		// Fixed metric_id header.
		xdr.writeInt(GangliaMessageTypeEnum.REQUEST.value()); // record type.
		xdr.writeString(msg.getHostName()); // host name
		xdr.writeString(msg.getMetricName()); // metric name
		xdr.writeInt(msg.isSpoof() ? 1 : 0); // spoof = False
		
	}

	public void writeMetadata(final XDROutputBuffer xdr,
			final IGangliaMetadataMessage decl) {
		
		xdr.reset();
		// Fixed metric_id header.
		xdr.writeInt(GangliaMessageTypeEnum.METADATA.value()); // record type.
		xdr.writeString(decl.getHostName()); // host name
		xdr.writeString(decl.getMetricName()); // metric name
		xdr.writeInt(decl.isSpoof() ? 1 : 0); // spoof = False
		// Metric metadata follows the fixed format header.
		xdr.writeString(decl.getMetricType().getGType()); // ganglia metric type.
		xdr.writeString(decl.getMetricName()); // metric name.
		xdr.writeString(decl.getUnits()); // units
		xdr.writeInt(decl.getSlope().value()); // slope : zero, positive,
												// negative, both. This is
												// related to RRDTool.
		xdr.writeInt(decl.getTMax()); // tmax, the maximum time between metrics
		xdr.writeInt(decl.getDMax()); // dmax, the maximum time before delete

		/*
		 * The group(s) in the web UI with which this metric will be associated.
		 */
		{
			final Map<String, String[]> extraValues = decl.getExtraValues();
			xdr.writeInt(extraValues.size()); // #of such groups.
			for (Map.Entry<String, String[]> e : extraValues.entrySet()) {
				for(final String name : e.getValue()) {
					xdr.writeString(e.getKey()); // name.
					xdr.writeString(name); // value
				}
			}
		}

	}

	public void writeMetric(final XDROutputBuffer xdr,
			final IGangliaMetadataMessage decl,
			final IGangliaMetricMessage msg) {
		
		xdr.reset();
		if (false) {
			/*
			 * Send all metric as strings.
			 * 
			 * Note: The potential drawbacks of doing this are that the string
			 * data might not be efficiently represented in the RRD files (I do
			 * not know if this is true) and might not be correctly aggregated
			 * (I suspect that this is true).
			 */
			xdr.writeInt(GangliaMessageTypeEnum.STRING.value());
			xdr.writeString(msg.getHostName()); // hostName
			xdr.writeString(msg.getMetricName()); // metric name
			xdr.writeInt(msg.isSpoof() ? 1 : 0); // spoof = False
			xdr.writeString("%s"); // always if sending metric as String.
			xdr.writeString(msg.getStringValue()); // Send the metric value.
		} else {
			/*
			 * Send metric using correct XDR data type.
			 */
			final GangliaMessageTypeEnum metricType = decl.getMetricType();
			xdr.writeInt(metricType.value()); // Type specific code.
			xdr.writeString(msg.getHostName()); // hostName
			xdr.writeString(msg.getMetricName()); // metric name
			xdr.writeInt(msg.isSpoof() ? 1 : 0); // spoof = False
			/*
			 * Send out printf format string.
			 */
			// based on the value type.
			// xdr.writeString(metricType.getFormat());
			// as given in the message.
			xdr.writeString(msg.getFormat());
			/*
			 * Send out the value using the data type specified in the metadata
			 * record for this metric.
			 */
			switch(metricType) {
			case DOUBLE:
				xdr.writeDouble(msg.getNumericValue().doubleValue());
				break;
			case FLOAT:
				xdr.writeFloat(msg.getNumericValue().floatValue());
				break;
			case INT16:
				xdr.writeShort(msg.getNumericValue().shortValue());
				break;
			case INT32:
				xdr.writeInt(msg.getNumericValue().intValue());
				break;
			case STRING:
				xdr.writeString(msg.getStringValue());
				break;
			case UINT16: {
				/*
				 * The Java value is an signed int whose value is the same as
				 * the desired unsigned int16 value, but whose bits need to be
				 * converted into an unsigned int16 value.
				 */
				final int v = msg.getNumericValue().intValue();
				if (v < 0) {
					throw new RuntimeException(
							"Negative integer for uint16 value: " + v
									+ ", decl=" + decl + ", msg=" + msg);
				} else if (v > UnsignedUtil.MAX_UINT16) {
					throw new RuntimeException(
							"Signed integer exceeds uint16 range: " + v
									+ ", decl=" + decl + ", msg=" + msg);
				}
				// Convert bits to unsigned representation of the same value.
				final int x = UnsignedUtil.encode(v); 
						//v - Short.MAX_VALUE;
				xdr.writeShort((short) x);
				break;
			}
			case UINT32: {
				/*
				 * The Java value is an signed long whose value is the same as
				 * the desired unsigned int32 value, but whose bits need to be
				 * converted into an unsigned int32 value.
				 */
				final long v = msg.getNumericValue().longValue();
				if (v < 0) {
					throw new RuntimeException(
							"Negative long for uint32 value: " + v + ", decl="
									+ decl + ", msg=" + msg);
				} else if (v > UnsignedUtil.MAX_UINT32) {
					throw new RuntimeException(
							"Signed long exceeds uint32 range: " + v
									+ ", decl=" + decl + ", msg=" + msg);
				}
				// Convert bits to unsigned representation of the same value.
				final long x = UnsignedUtil.encode(v);
				// v - Integer.MAX_VALUE;
				xdr.writeInt((int) x);
				break;
			}
			default:
				throw new UnsupportedOperationException("metricType="
						+ metricType + ", metric=" + msg);
			}
		}

	}

}
