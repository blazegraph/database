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

import java.util.HashMap;
import java.util.Map;

/**
 * Type safe enumeration of the Ganglia 3.1 wire format
 * <code>Ganglia_msg_formats</code> from <code>lib/gm_protocol.h</code>
 * (generated when you build ganglia from the source).
 * <p>
 * Note: The Ganglia 3.1 wire format packets begin at 128 to avoid confusion
 * with the previous wire format.
 */
public enum GangliaMessageTypeEnum {

	/**
	 * Ganglia metric metadata declaration record.
	 */
	METADATA(128, false, null/* n/a */, null/* n/a */, null/* n/a */), //

	/**
	 * Ganglia metric record with unsigned short value.
	 * <p>
	 * This data type is NOT automatically selected for any Java data values. It
	 * is only used if there is an explicit {@link IGangliaMetadataMessage}
	 * declaring the metric as having this datatype.
	 * {@link IGangliaMetricMessage} associated with this ganglia data type by
	 * an {@link IGangliaMetadataMessage} are internally modeled using a
	 * non-negative Java {@link Integer}. However, when they are sent out on the
	 * wire they are written out using the printf format string for an unsigned
	 * int16 value.
	 */
	UINT16(128 + 1, true, "uint16", "%hu", null/* n/a */), //

	/**
	 * Ganglia metric record with short value.
	 */
	INT16(128 + 2, true, "int16", "%hi", new Class[]{Short.class}), //

	/**
	 * Ganglia metric record with int32 value.
	 */
	INT32(128 + 3, true, "int32", "%i", new Class[]{Integer.class}), //

	/**
	 * Ganglia metric record with unsigned int32 value.
	 * <p>
	 * This data type is NOT automatically selected for any Java data values. It
	 * is only used if there is an explicit {@link IGangliaMetadataMessage}
	 * declaring the metric as having this datatype.
	 * {@link IGangliaMetricMessage} associated with this ganglia data type by
	 * an {@link IGangliaMetadataMessage} are internally modeled using a
	 * non-negative Java {@link Long}. However, when they are sent out on the
	 * wire they are written out using the printf format string for an unsigned
	 * int32 value.
	 */
	UINT32(128 + 4, true, "uint32", "%u", null/* n/a */), //

	/**
	 * Ganglia metric record with string value.
	 */
	STRING(128 + 5, true, "string", "%s", new Class[]{String.class}),//
	
	/**
	 * Ganglia metric record with float value.
	 */
	FLOAT(128 + 6, true, "float", "%f", new Class[]{Float.class}), //
	
	/**
	 * Ganglia metric record with double value.
	 */
	DOUBLE(128 + 7, true, "double", "%lf", new Class[] { Double.class,
			Long.class }), //
	
	/**
	 * Ganglia request record (requests a metadata record for the named metric).
	 */
	REQUEST(128 + 8, false, null/* n/a */, null/* n/a */, null/* n/a */);

	/**
	 * 
	 * @param v
	 *            The byte code for the record type.
	 * @param isMetric
	 *            <code>true</code> iff this record type represents a metric
	 *            value (versus metadata about a metric or a request for a
	 *            metric).
	 * @param gtype
	 *            The string name of the ganglia data type.
	 * @param format
	 *            The <code>printf</code> string used for that ganglia data
	 *            type.
	 * @param javaClasses
	 *            The Java {@link Number} class(es) which are mapped onto a
	 *            given ganglia data type.
	 */
	private GangliaMessageTypeEnum(final int v, final boolean isMetric,
			final String gtype, final String format, final Class<?>[] javaClasses) {
		this.v = v;
		this.isMetric = isMetric;
		this.gtype = gtype;
		this.format = format;
		this.javaClasses = javaClasses;
		/*
		 * Note: you can not insert into the static map here (not permitted).
		 */
	}

	private final int v;
	private final boolean isMetric;
	private final String gtype;
	private final String format;
	private final Class<?>[] javaClasses;
	
	/** The byte value used in the wire format. */
	public int value() {
		return v;
	}
	
	/**
	 * Return <code>true</code> if this is a metric value record.
	 */
	public boolean isMetric() {
		return isMetric;
	}

	/**
	 * The ganglia datatype name (uint, etc). This is only defined for record
	 * types which correspond to a metric value.
	 */
	public String getGType() {
		return gtype;
	}
	
	/**
	 * Return the <code>printf</code> format string suitable for use with the
	 * ganglia data type.
	 */
	public String getFormat() {
		return format;
	}

	/**
	 * Return <code>true</code> iff this is a numeric metric type.
	 */
	public boolean isNumeric() {

		switch (this) {
		case DOUBLE:
		case FLOAT:
		case INT16:
		case INT32:
		case UINT16:
		case UINT32:
			return true;
		case STRING:
			return false;
		case METADATA:
		case REQUEST:
			return false;
		default:
			throw new AssertionError();
		}
		
	}
	
//	/**
//	 * Return the Java data type which corresponds to the ganglia metric type.
//	 * 
//	 * @return The data type -or- <code>null</code> if this is NOT a metric type
//	 *         (i.e., return <code>null</code> if it is either a
//	 *         {@link #METADATA} or {@link #REQUEST}).
//	 */
//	public Class<?> getJavaType() {
//		return javaClass;
//	}
	
	/**
	 * Return the type safe enum for the record type value.
	 * 
	 * @param v
	 *            The value.
	 *            
	 * @return The enum.
	 */
	static public GangliaMessageTypeEnum valueOf(final int v) {
		switch (v) {
		case 128:
			return METADATA;
		case 128 + 1:
			return UINT16;
		case 128 + 2:
			return INT16;
		case 128 + 3:
			return INT32;
		case 128 + 4:
			return UINT32;
		case 128 + 5:
			return STRING;
		case 128 + 6:
			return FLOAT;
		case 128 + 7:
			return DOUBLE;
		case 128 + 8:
			return REQUEST;
		default:
			throw new IllegalArgumentException("value=" + v);
		}
	}

	/**
	 * Translate gtype (uint32, float, string, etc) into type safe enum.
	 * 
	 * @throws IllegalArgumentException
	 *             if the argument is none of the data types used by ganglia.
	 */
	static public GangliaMessageTypeEnum fromGType(final String metricType) {

		final GangliaMessageTypeEnum e = gtype2Enum.get(metricType);

		if (e == null)
			throw new IllegalArgumentException("metricType=" + metricType);

		return e;

	}

	/**
	 * Return the best match {@link GangliaMessageTypeEnum} for a java data
	 * value.
	 * <p>
	 * Note: This method is intended for dynamic declarations in which a value
	 * to be reported is collected from the Java application and an appropriate
	 * {@link IGangliaMetadataMessage} needs to be constructed based on that
	 * value object to declare the metric to the ganglia network.
	 * <p>
	 * This method SHOULD NOT be used to discover the enum when you already have
	 * that information. In particular, do NOT use this method when you are
	 * looking at an {@link IGangliaMetricMessage} associated with one of the
	 * <strong>unsigned</strong> ganglia data types. Unsigned ganglia data types
	 * are represented by a wider primitive data type in java (uint16 as int and
	 * uint32 as long). Various methods correctly translate between the wire
	 * format for those messages when encoding and decoding. The Java data type
	 * used for such messages will NOT be correctly translated back into the
	 * actual {@link GangliaMessageTypeEnum} by this method. For example, a
	 * {@link Integer} is translated by this method into {@link #UINT32} rather
	 * than {@link #UINT16} since this method assumes that the Java object is
	 * expressing a signed numeric value rather than an unsigned numeric value).
	 * Also, this method will translate a Java {@link Long} into {@link #DOUBLE}
	 * in order to convey as much information as possible to ganglia (which
	 * lacks an int64 data type).
	 * 
	 * @param value
	 *            The data value.
	 * 
	 * @return The best match {@link GangliaMessageTypeEnum}.
	 * 
	 * @throws IllegalArgumentException
	 *             if the <i>value</i> is <code>null</code>.
	 * @throws IllegalArgumentException
	 *             if the <i>value</i> is not one of those registered for use
	 *             with this enum.
	 */
	static public GangliaMessageTypeEnum forJavaValue(final Object value) {

		if (value == null)
			throw new UnsupportedOperationException();

		final Class<?> cls = value.getClass();

		final GangliaMessageTypeEnum e = javaClass2Enum.get(cls);

		if (e == null)
			throw new IllegalArgumentException();

		return e;

	}
	
	/**
	 * Map used to translate ganglia type strings into the corresponding
	 * {@link GangliaMessageTypeEnum}.
	 */
	private static final Map<String, GangliaMessageTypeEnum> gtype2Enum;

	/**
	 * Map used to obtain the best match {@link GangliaMessageTypeEnum} given a
	 * natural Java data type.
	 */
	private static final Map<Class<?>, GangliaMessageTypeEnum> javaClass2Enum;

	static {

		gtype2Enum = new HashMap<String, GangliaMessageTypeEnum>();

		javaClass2Enum = new HashMap<Class<?>, GangliaMessageTypeEnum>();

		for (GangliaMessageTypeEnum e : values()) {

			gtype2Enum.put(e.getGType(), e);

			if(e.javaClasses != null) {

				for(Class<?> cls : e.javaClasses) {

					javaClass2Enum.put(cls, e);
					
				}
				
			}

		}

		// map.put("uint16", UINT16);
		// map.put("int16", INT16);
		// map.put("uint32", UINT32);
		// map.put("int32", INT32);
		// map.put("string", STRING);
		// map.put("float", FLOAT);
		// map.put("double", DOUBLE);

	}

}
