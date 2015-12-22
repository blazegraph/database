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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.bigdata.ganglia.xdr.XDRInputBuffer;

/**
 * Class decodes a Ganglia 3.1 wire format message.
 */
public class GangliaMessageDecoder31 implements IGangliaMessageDecoder {

	static private final Logger log = Logger.getLogger(GangliaMessageDecoder31.class);

	public IGangliaMessage decode(final byte[] data, final int off,
			final int len) {

		final XDRInputBuffer xdr = new XDRInputBuffer(data, off, len);

		final GangliaMessageTypeEnum recordType;

		try {

			recordType = GangliaMessageTypeEnum.valueOf(xdr.readInt());

		} catch (IllegalArgumentException ex) {

			log.warn("Unknown record type: " + ex);

			return null;
			
		}

		if (recordType.value() < GangliaMessageTypeEnum.METADATA.value()) {

			/*
			 * The 3.1 wire format begins with byte 128. Values less than that
			 * were part of the 2.5 wire format. This code does not handle the
			 * earlier wire format, but it would be easy enough to decode. See
			 * the class which generates the 2.5 wire format messages for the
			 * XDR "schema".
			 */
			
			log.warn("Ganglia 2.5 record type: " + recordType);

			return null;

		}
		
		/*
		 * Decode the metric_id header. This header is common to all ganglia
		 * 3.1 messages.
		 * 
		 * [hostName:string][metricName:string][spoof:int]
		 */

		final String hostName = xdr.readString();
		
		final String metricName = xdr.readString();
		
		final boolean spoof = xdr.readInt() == 0 ? false : true;

		switch (recordType) {
		case METADATA: {
			
			/*
			 * Decode a ganglia metric declaration record.
			 */
			
			// metric metadata record.
			final String metricTypeStr = xdr.readString();
			final String metricName2 = xdr.readString();
			final String units = xdr.readString();
			final GangliaSlopeEnum slope = GangliaSlopeEnum.valueOf(xdr
					.readInt());
			final int tmax = xdr.readInt();
			final int dmax = xdr.readInt();
			final Map<String, String[]> extraValues;
			final int nextra = xdr.readInt();
			if (nextra == 0) {
				extraValues = Collections.emptyMap();
			} else {
				extraValues = new LinkedHashMap<String, String[]>();
				for (int i = 0; i < nextra; i++) {
					final String name = xdr.readString();
					final String value = xdr.readString();
					final String[] a = extraValues.get(name);
					if (a == null) {
						extraValues.put(name, new String[] { value });
					} else {
						final String[] b = new String[a.length + 1];
						System.arraycopy(a/* src */, 0/* srcPos */, b/* dest */,
								0/* destPos */, a.length);
						b[a.length] = value;
						extraValues.put(name, b);
					}
				}
			}

			// translate gtype into type safe enum
			final GangliaMessageTypeEnum metricType = GangliaMessageTypeEnum
					.fromGType(metricTypeStr);
			
			final IGangliaMetadataMessage msg = new GangliaMetadataMessage(
					hostName, metricName, spoof, metricType, metricName2,
					units, slope, tmax, dmax, extraValues);
			
			return msg;
			
		}
		case REQUEST: {

			/*
			 * Ganglia sends out request messages when a service wants to obtain
			 * fresh metrics. The request record itself does not have any
			 * additional fields.
			 */

			final IGangliaMessage msg = new GangliaRequestMessage(hostName,
					metricName, spoof);

			return msg;

		}
		case DOUBLE:
		case FLOAT:
		case INT32:
		case INT16:
		case STRING:
		case UINT32:
		case UINT16: {
			/*
			 * Decode a metric value.
			 * 
			 * [format:string][value]
			 * 
			 * where the encoding for the value depends on the recordType.
			 */
			// metric value record.
			final String format = xdr.readString();
			final Object value;
			switch (recordType) {
			case DOUBLE:
				value = xdr.readDouble();
				break;
			case FLOAT:
				value = xdr.readFloat();
				break;
			case INT32:
				value = xdr.readInt();
				break;
			case INT16:
				value = xdr.readShort();
				break;
			case UINT32:
				value = xdr.readUInt();
				break;
			case UINT16:
				value = xdr.readUShort();
				break;
			case STRING:
				value = xdr.readString();
				break;
			default:
				throw new AssertionError();
			}

			final IGangliaMetricMessage msg = new GangliaMetricMessage(
					recordType, hostName, metricName, spoof, format, value);

			return msg;
		}
		default: {

			break;

		}
		}

		/*
		 * Something we are not handling.
		 */
		
		log.warn("Not handled: " + recordType);
		
		return null;

	}

}
