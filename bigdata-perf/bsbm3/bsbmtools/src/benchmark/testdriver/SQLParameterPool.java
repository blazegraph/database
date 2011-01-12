package benchmark.testdriver;

import java.io.File;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.List;

import benchmark.generator.DateGenerator;
import benchmark.model.ProductType;

public class SQLParameterPool extends AbstractParameterPool {
	
	public SQLParameterPool(File resourceDirectory, Long seed) {
		init(resourceDirectory, seed);
	}

	/*
	 * (non-Javadoc)
	 * @see benchmark.testdriver.AbstractParameterPool#getParametersForQuery(benchmark.testdriver.Query)
	 */
	@Override
    public Object[] getParametersForQuery(Query query) {
		Byte[] parameterTypes = query.getParameterTypes();
		Object[] parameters = new Object[parameterTypes.length];
		ArrayList<Integer> productFeatureIndices = new ArrayList<Integer>();
		ProductType pt = null;
		
		for(int i=0;i<parameterTypes.length;i++) {
			if(parameterTypes[i]==Query.PRODUCT_TYPE_URI) {
				pt = getRandomProductType();
				parameters[i] = pt.getNr();
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
				parameters[i] = countryGen.getRandom();
			else if(parameterTypes[i]==Query.REVIEW_URI)
				parameters[i] = getRandomReviewURI();
			else if(parameterTypes[i]==Query.WORD_FROM_DICTIONARY1)
				parameters[i] = getRandomWord();
			else if(parameterTypes[i]==Query.OFFER_URI)
				parameters[i] = getRandomOfferURI();
			else
				parameters[i] = null;
		}
		
		if(productFeatureIndices.size()>0 && pt == null) {
			System.err.println("Error in parameter generation: Asked for product features without product type.");
			System.exit(-1);
		}
		
		Integer[] productFeatures = getRandomProductFeatures(pt, productFeatureIndices.size());
		for(int i=0;i<productFeatureIndices.size();i++) {
			parameters[productFeatureIndices.get(i)] = productFeatures[i];
		}
		
		return parameters;
	}
	
	/*
	 * Get number distinct random Product Feature URIs of a certain Product Type
	 */
	private Integer[] getRandomProductFeatures(ProductType pt, Integer number) {
		ArrayList<Integer> pfs = new ArrayList<Integer>();
		Integer[] productFeatures = new Integer[number];
		
		ProductType temp = pt;
		while(temp!=null) {
			List<Integer> tempList = temp.getFeatures();
			if(tempList!=null)
				pfs.addAll(tempList);
			temp = temp.getParent();
		}
		
		if(pfs.size() < number) {
			System.err.println(pt.toString() + " doesn't contain " + number + " different Product Features!");
			System.exit(-1);
		}
		
		for(int i=0;i<number;i++) {
			Integer index = valueGen.randomInt(0, pfs.size()-1);
			productFeatures[i] = pfs.get(index);
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
	 * Get a random Product URI
	 */
	private Integer getRandomProductURI() {
		return valueGen.randomInt(1, productCount);
	}
	
	/*
	 * Get a random Offer URI
	 */
	private Integer getRandomOfferURI() {
		return valueGen.randomInt(1, offerCount);
	}
	
	/*
	 * Get a random Review URI
	 */
	private Integer getRandomReviewURI() {
		return valueGen.randomInt(1, reviewCount);
	}
	
	/*
	 * Get random word from word list
	 */
	private String getRandomWord() {
		Integer index = valueGen.randomInt(0, wordList.length-1);
		
		return wordList[index];
	}
	
	
	/*
	 * Returns a random number between 1-500
	 */
	private Integer getProductPropertyNumeric() {
		return valueGen.randomInt(1, 500);
	}


	@Override
	protected String formatDateString(GregorianCalendar date) {
		return DateGenerator.formatDate(currentDate);
	}
}
