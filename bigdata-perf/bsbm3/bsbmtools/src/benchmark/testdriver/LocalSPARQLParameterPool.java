package benchmark.testdriver;

import java.io.*;
import java.util.*;

import benchmark.generator.DateGenerator;
import benchmark.model.*;
import benchmark.vocabulary.*;

import benchmark.vocabulary.XSD;

public class LocalSPARQLParameterPool extends AbstractParameterPool {
	private BufferedReader updateFileReader = null;
	private GregorianCalendar publishDateMin = new GregorianCalendar(2007,5,20);
	
	public LocalSPARQLParameterPool(File resourceDirectory, Long seed) {
		init(resourceDirectory, seed);
	}
	
	public LocalSPARQLParameterPool(File resourceDirectory, Long seed, File updateDatasetFile) {
		init(resourceDirectory, seed);
		try {
			updateFileReader = new BufferedReader(new FileReader(updateDatasetFile));
		} catch (FileNotFoundException e) {
			System.out.println("Could not open update dataset file: " + e.getMessage());
			e.printStackTrace();
			System.exit(-1);
		}
	}
	
	@Override
	public Object[] getParametersForQuery(Query query) {
		Byte[] parameterTypes = query.getParameterTypes();
		Object[] parameters = new Object[parameterTypes.length];
		ArrayList<Integer> productFeatureIndices = new ArrayList<Integer>();
		ProductType pt = null;
		GregorianCalendar randomDate = null;
		
		for(int i=0;i<parameterTypes.length;i++) {
			if(parameterTypes[i]==Query.PRODUCT_TYPE_URI) {
				pt = getRandomProductType();
				parameters[i] = pt.toString();
			}
			else if(parameterTypes[i]==Query.PRODUCT_FEATURE_URI)
				productFeatureIndices.add(i);
			else if(parameterTypes[i]==Query.PRODUCT_PROPERTY_NUMERIC)
				parameters[i] = getProductPropertyNumeric();
			else if(parameterTypes[i]==Query.PRODUCT_URI)
				parameters[i] = getRandomProductURI();
			else if(parameterTypes[i]==Query.CURRENT_DATE)
				parameters[i] = currentDateString;
			else if(parameterTypes[i]==Query.COUNTRY_URI)
				parameters[i] = "<" + ISO3166.find((String)countryGen.getRandom()) + ">";
			else if(parameterTypes[i]==Query.REVIEW_URI)
				parameters[i] = getRandomReviewURI();
			else if(parameterTypes[i]==Query.WORD_FROM_DICTIONARY1)
				parameters[i] = getRandomWord();
			else if(parameterTypes[i]==Query.OFFER_URI)
				parameters[i] = getRandomOfferURI();
			else if(parameterTypes[i]==Query.UPDATE_TRANSACTION_DATA)
				parameters[i] = getUpdateTransactionData();
			else if(parameterTypes[i]==Query.CONSECUTIVE_MONTH) {
				if(randomDate==null)
					randomDate = getRandomPublishDate();
				int monthNr = (Integer)query.getAdditionalParameterInfo(i);
				parameters[i] = getConsecutiveMonth(randomDate, monthNr);
			} else if(parameterTypes[i]==Query.PRODUCER_URI)
				parameters[i] = getRandomProducerURI();
			else
				parameters[i] = null;
		}
		
		if(productFeatureIndices.size()>0 && pt == null) {
			System.err.println("Error in parameter generation: Asked for product features without product type.");
			System.exit(-1);
		}
		
		String[] productFeatures = getRandomProductFeatures(pt, productFeatureIndices.size());
		for(int i=0;i<productFeatureIndices.size();i++) {
			parameters[productFeatureIndices.get(i)] = productFeatures[i];
		}
		
		return parameters;
	}
	
	/*
	 * Get date string for ConsecutiveMonth
	 */
	private String getConsecutiveMonth(GregorianCalendar date, int monthNr) {
		GregorianCalendar gClone = (GregorianCalendar)date.clone();
		gClone.add(GregorianCalendar.DAY_OF_MONTH, 28*monthNr);
		return DateGenerator.formatDate(gClone);
	}
	
	/*
	 * Get number distinct random Product Feature URIs of a certain Product Type
	 */
	private String[] getRandomProductFeatures(ProductType pt, Integer number) {
		ArrayList<Integer> pfs = new ArrayList<Integer>();
		String[] productFeatures = new String[number];
		
		ProductType temp = pt;
		while(temp!=null) {
			List<Integer> tempList = temp.getFeatures();
			if(tempList!=null)
				pfs.addAll(temp.getFeatures());
			temp = temp.getParent();
		}
		
		if(pfs.size() < number) {
			System.err.println(pt.toString() + " doesn't contain " + number + " different Product Features!");
			System.exit(-1);
		}
		
		for(int i=0;i<number;i++) {
			Integer index = valueGen.randomInt(0, pfs.size()-1);
			productFeatures[i] = ProductFeature.getURIref(pfs.get(index));
			pfs.remove(index);
		}
		
		return productFeatures;
	}
	
	/*
	 * Get a random Product Type URI
	 */
	private ProductType getRandomProductType() {
		Integer index = valueGen.randomInt(0, productTypeLeaves.length-1);
		
		return productTypeLeaves[index];
	}
	
	/*
	 * Get a random date from (today-365) to (today-56)
	 */
	private GregorianCalendar getRandomPublishDate() {
		Integer dayOffset = valueGen.randomInt(0, 309);
		GregorianCalendar gClone = (GregorianCalendar)publishDateMin.clone();
		gClone.add(GregorianCalendar.DAY_OF_MONTH, dayOffset);
		return gClone;
	}
	
	/*
	 * Get a random Product URI
	 */
	private String getRandomProductURI() {
		Integer productNr = valueGen.randomInt(1, productCount);
		Integer producerNr = getProducerOfProduct(productNr);
		
		return Product.getURIref(productNr, producerNr);
	}
	
	/*
	 * Get a random Offer URI
	 */
	private String getRandomOfferURI() {
		Integer offerNr = valueGen.randomInt(1, offerCount);
		Integer vendorNr = getVendorOfOffer(offerNr);
		
		return Offer.getURIref(offerNr, vendorNr);
	}
	
	/*
	 * Get a random Review URI
	 */
	private String getRandomReviewURI() {
		Integer reviewNr = valueGen.randomInt(1, reviewCount);
		Integer ratingSiteNr = getRatingsiteOfReviewer(reviewNr);
		
		return Review.getURIref(reviewNr, ratingSiteNr);
	}
	
	/*
	 * Get a random producer URI
	 */
	private String getRandomProducerURI() {
		Integer producerNr = valueGen.randomInt(1, producerOfProduct.length-1);
		
		return Producer.getURIref(producerNr);
	}
	
	/*
	 * Get random word from word list
	 */
	private String getRandomWord() {
		Integer index = valueGen.randomInt(0, wordList.length-1);
		
		return wordList[index];
	}
	
	/*
	 * Returns the ProducerNr of given Product Nr.
	 */
	private Integer getProducerOfProduct(Integer productNr) {
		Integer producerNr = Arrays.binarySearch(producerOfProduct, productNr);
		if(producerNr<0)
			producerNr = - producerNr - 1;
		
		return producerNr;
	}
	
	/*
	 * Returns the ProducerNr of given Product Nr.
	 */
	private Integer getVendorOfOffer(Integer offerNr) {
		Integer vendorNr = Arrays.binarySearch(vendorOfOffer, offerNr);
		if(vendorNr<0)
			vendorNr = - vendorNr - 1;
		
		return vendorNr;
	}
	
	/*
	 * Returns the Rating Site Nr of given Review Nr
	 */
	private Integer getRatingsiteOfReviewer(Integer reviewNr) {
		Integer ratingSiteNr = Arrays.binarySearch(ratingsiteOfReview, reviewNr);
		if(ratingSiteNr<0)
			ratingSiteNr = - ratingSiteNr - 1;
		
		return ratingSiteNr;
	}
	
	/*
	 * Returns a random number between 1-500
	 */
	private Integer getProductPropertyNumeric() {
		return valueGen.randomInt(1, 500);
	}
	
	/*
	 * Return the triples to inserted into the store
	 */
	private String getUpdateTransactionData() {
		StringBuilder s = new StringBuilder();
		String line = null;
		try {
			while((line=updateFileReader.readLine()) != null) {
				if(line.equals("#__SEP__"))
					break;
				s.append(line);
				s.append("\n");
			}
		} catch (IOException e) {
			System.err.println("Error reading update data from file: " + e.getMessage());
			e.printStackTrace();
			System.exit(-1);
		}
		return s.toString();
	}

	@Override
	protected String formatDateString(GregorianCalendar date) {
		return "\"" + DateGenerator.formatDateTime(date) + "\"^^<" + XSD.DateTime + ">";
	}
}
