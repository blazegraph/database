package benchmark.testdriver;

import java.io.*;
import java.util.*;

public class Query {
	private int nr;
	private Object[] parameters;
	private Integer[] parameterFills;
	private Byte[] parameterTypes;
	// Some parameters define additional information that will be stored here
	private Object[] additionalParameterInfo;
	private Vector<String> queryStrings;
	private Byte queryType;
	private QueryMix queryMix;
	private String parameterChar;
	private String[] rowNames;// which rows to look at for validation
	private static Map<String, Byte> parameterMapping;

	// Parameter constants
	public static final byte PRODUCT_PROPERTY_NUMERIC = 1;
	public static final byte PRODUCT_FEATURE_URI = 2;
	public static final byte PRODUCT_TYPE_URI = 3;
	public static final byte CURRENT_DATE = 4;
	public static final byte WORD_FROM_DICTIONARY1 = 5;
	public static final byte PRODUCT_URI = 6;
	public static final byte REVIEW_URI = 7;
	public static final byte COUNTRY_URI = 8;
	public static final byte OFFER_URI = 9;
	public static final byte CONSECUTIVE_MONTH = 10;
	public static final byte UPDATE_TRANSACTION_DATA = 11;
	public static final byte PRODUCER_URI = 12;

	// Initialize Parameter mappings
	static {
		parameterMapping = new HashMap<String, Byte>();
		parameterMapping.put("ProductPropertyNumericValue",
				PRODUCT_PROPERTY_NUMERIC);
		parameterMapping.put("ProductFeatureURI", PRODUCT_FEATURE_URI);
		parameterMapping.put("ProductTypeURI", PRODUCT_TYPE_URI);
		parameterMapping.put("CurrentDate", CURRENT_DATE);
		parameterMapping.put("Dictionary1", WORD_FROM_DICTIONARY1);
		parameterMapping.put("ProductURI", PRODUCT_URI);
		parameterMapping.put("ReviewURI", REVIEW_URI);
		parameterMapping.put("CountryURI", COUNTRY_URI);
		parameterMapping.put("OfferURI", OFFER_URI);
		parameterMapping.put("ConsecutiveMonth", CONSECUTIVE_MONTH);
		parameterMapping.put("UpdateTransactionData", UPDATE_TRANSACTION_DATA);
		parameterMapping.put("ProducerURI", PRODUCER_URI);
	}

	// query type constants
	public static final byte SELECT_TYPE = 1;
	public static final byte DESCRIBE_TYPE = 2;
	public static final byte CONSTRUCT_TYPE = 3;
	public static final byte UPDATE_TYPE = 4;

	public Query(String queryString, String parameterDescription, String c) {
		parameterChar = c;
		init(queryString, parameterDescription);
	}

	public Query(File queryFile, File parameterDescriptionFile, String c) {
		parameterChar = c;
		String queryString = "";
		String parameterDescriptionString = "";

		BufferedReader descriptionReader = null;
		BufferedReader queryReader = null;
		try {
			queryReader = new BufferedReader(new InputStreamReader(
					new FileInputStream(queryFile)));
			StringBuffer sb = new StringBuffer();

			while (true) {
				String line = queryReader.readLine();
				if (line == null)
					break;
				else {
					sb.append(line);
					sb.append("\n");
				}
			}
			queryString = sb.toString();

			// Now the parameter description
			descriptionReader = new BufferedReader(new InputStreamReader(
					new FileInputStream(parameterDescriptionFile)));
			sb = new StringBuffer();

			while (true) {
				String line = descriptionReader.readLine();
				if (line == null)
					break;
				else {
					sb.append(line);
					sb.append("\n");
				}
			}
			parameterDescriptionString = sb.toString();

		} catch (IOException e) {
			System.err.println(e.getMessage());
			System.exit(-1);
		} finally {
			try {
				if (descriptionReader != null)
					descriptionReader.close();
				if (queryReader != null)
					queryReader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		init(queryString, parameterDescriptionString);
	}

	/*
	 * Initialize the Query
	 */
	private void init(String queryString, String parameterDescription) {
		queryStrings = processQueryString(queryString);
		queryType = SELECT_TYPE;// default: Select query

		if (queryStrings == null) {
			System.err.println("Error in Query");
			System.exit(-1);
		}

		processParameters(queryString, parameterDescription);

		parameters = new Object[parameterTypes.length];
	}

	/*
	 * Sets the Query Parameter Types and their places in the query, namely the
	 * two arrays parameterFills and parameterTypes
	 */
	private void processParameters(String queryString,
			String parameterDescription) {
		// parameterType Array
		Vector<Byte> parameterT = new Vector<Byte>();
		Vector<Object> additionalP = new Vector<Object>();
		// StringTokenizer for the parameter description String
		StringTokenizer paramTokenizer = new StringTokenizer(
				parameterDescription);
		// Mapping of Parameter names to their array position
		HashMap<String, Integer> mapping = new HashMap<String, Integer>();

		Integer index = 0;// Array index for ParameterTypes

		// Read Query Description
		while (paramTokenizer.hasMoreTokens()) {
			String line = paramTokenizer.nextToken();
			// Skip uninteresting lines
			if (!line.contains("="))
				continue;

			int offset = line.indexOf("=");
			// Parameter name
			String parameter = line.substring(0, offset);

			offset++;
			// Parameter Type
			String paramType = line.substring(offset);

			// If special parameter querytype is given save query type for later
			// use
			if (parameter.toLowerCase().equals("querytype")) {
				byte qType = getQueryType(paramType);
				if (qType == 0) {
					System.err.println("Invalid query type chosen."
							+ " Using default: Select");
				} else
					queryType = qType;
			}// else get Parameter
			else {
				String addPI = null;
				if(paramType.contains("_")) {
					String[] paramSplit = paramType.split("_", 2);
					paramType = paramSplit[0];
					addPI = paramSplit[1];
				}
				Byte byteType = getParamType(paramType);
				Object additionParameterInfo = null;
				if(addPI != null)
					additionParameterInfo = getAdditionParameterInfo(byteType, addPI);

				if (byteType == 0) {
					System.err.println("Unknown Type: " + paramType);
					System.exit(-1);
				}
				mapping.put(parameter, index++);
				additionalP.add(additionParameterInfo);
				parameterT.add(byteType);
			}
		}

		parameterTypes = new Byte[parameterT.size()];
		additionalParameterInfo = new Object[parameterT.size()];
		for (int i = 0; i < parameterT.size(); i++) {
			parameterTypes[i] = parameterT.elementAt(i);
			additionalParameterInfo[i] = additionalP.elementAt(i);
		}

		// fill parameterFills
		Vector<Integer> paramFills = new Vector<Integer>();

		int index1 = 0;
		int index2 = -1;
		while (queryString.indexOf(parameterChar, index2 + 1) != -1) {

			index1 = queryString.indexOf(parameterChar, index2 + 1);

			index2 = queryString.indexOf(parameterChar, index1 + 1);

			String parameter = queryString.substring(index1 + 1, index2);

			paramFills.add(mapping.get(parameter));
		}

		parameterFills = new Integer[paramFills.size()];
		for (int i = 0; i < paramFills.size(); i++)
			parameterFills[i] = paramFills.elementAt(i);
	}
	
	/*
	 * For some parameter types additional information can be defined
	 */
	private Object getAdditionParameterInfo(byte type, String additionalInfo) {
		Object returnValue = additionalInfo;
		if(type==Query.CONSECUTIVE_MONTH) {
			try {
				returnValue = Integer.parseInt(additionalInfo);
			} catch(NumberFormatException e) {
				System.err.println("Illegal parameter for ConsecutiveMonth: " + additionalInfo);
				System.exit(-1);
			}
		}
		return returnValue;
	}

	/*
	 * get the byte type representation of this parameter string
	 */
	private byte getParamType(String stringType) {
		Byte param = parameterMapping.get(stringType);
		if (param != null)
			return param;
		else
			return 0;
	}

	/*
	 * get the byte type representation of this query type string
	 */
	private byte getQueryType(String stringType) {
		if (stringType.toLowerCase().equals("select"))
			return SELECT_TYPE;
		else if (stringType.toLowerCase().equals("describe"))
			return DESCRIBE_TYPE;
		else if (stringType.toLowerCase().equals("construct"))
			return CONSTRUCT_TYPE;
		else if (stringType.toLowerCase().equals("update"))
			return UPDATE_TYPE;
		else
			return 0;
	}

	/*
	 * Get the Query String components without the parameter Strings
	 */
	private Vector<String> processQueryString(String queryString) {
		Vector<String> queryStrings = new Vector<String>();

		int index1 = 0;
		int index2 = -1;
		while (queryString.contains(parameterChar)) {

			index1 = queryString.indexOf(parameterChar, index2 + 1);
			if (index1 == -1) {
				index2++;
				break;
			}

			queryStrings.add(queryString.substring(index2 + 1, index1));

			index2 = queryString.indexOf(parameterChar, index1 + 1);
			if (index2 == -1)
				return null;// Error: Shouldn't happen
		}

		if (index2 == -1)
			index2++;

		queryStrings.add(queryString.substring(index2));
		return queryStrings;
	}

	public void setParameters(Object[] param) {
		if (parameters.length == param.length)
			parameters = param;
		else {
			System.err.println("Invalid parameter count.");
			System.exit(-1);
		}
	}

	/*
	 * returns a String of the Query with query parameters filled in.
	 */
	public String getQueryString() {
		StringBuilder s = new StringBuilder();

		s.append(queryStrings.get(0));
		for (int i = 1; i < queryStrings.size(); i++) {
			s.append(parameters[parameterFills[i - 1]]);
			s.append(queryStrings.get(i));
		}

		return s.toString();
	}

	public Byte[] getParameterTypes() {
		return parameterTypes;
	}

	public Object getAdditionalParameterInfo(int indexOfParameter) {
		return additionalParameterInfo[indexOfParameter];
	}

	public int getNr() {
		return nr;
	}

	public void setNr(int nr) {
		this.nr = nr;
	}

	public Byte getQueryType() {
		return queryType;
	}

	public QueryMix getQueryMix() {
		return queryMix;
	}

	public void setQueryMix(QueryMix queryMix) {
		this.queryMix = queryMix;
	}

	public String[] getRowNames() {
		return rowNames;
	}

	public void setRowNames(String rowNames[]) {
		this.rowNames = rowNames;
	}
}
