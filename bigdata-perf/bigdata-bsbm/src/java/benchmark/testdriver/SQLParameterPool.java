package benchmark.testdriver;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import benchmark.generator.DateGenerator;
import benchmark.generator.Generator;
import benchmark.generator.RandomBucket;
import benchmark.generator.ValueGenerator;
import benchmark.model.ProductType;

public class SQLParameterPool extends AbstractParameterPool {
	private ValueGenerator valueGen;
	private RandomBucket countryGen;
	private GregorianCalendar currentDate;
	private String currentDateString;
	private ProductType[] productTypeLeaves;
	private HashMap<String,Integer> wordHash;
	private String[] wordList;
	private Integer[] producerOfProduct;
	private Integer[] vendorOfOffer;
	private Integer[] ratingsiteOfReview;
	private Integer productCount;
	private Integer reviewCount;
	private Integer offerCount;
	
	public SQLParameterPool(File resourceDirectory, Long seed) {
		Random seedGen = new Random(seed);
		valueGen = new ValueGenerator(seedGen.nextLong());
		countryGen = Generator.createCountryGenerator(seedGen.nextLong());
		
		init(resourceDirectory);
	}
	
	private void init(File resourceDir) {
		//Read in the Product Type hierarchy from resourceDir/pth.dat
		ObjectInputStream productTypeInput;
		File pth = new File(resourceDir, "pth.dat");
		try {
			productTypeInput = new ObjectInputStream(new FileInputStream(pth));
			productTypeLeaves = (ProductType[]) productTypeInput.readObject();
		} catch(IOException e) {
			System.err.println("Could not open or process file " + pth.getAbsolutePath());
			System.err.println(e.getMessage());
			System.exit(-1);
		}
		catch(ClassNotFoundException e) { System.err.println(e); }

		//Product-Producer Relationships from resourceDir/pp.dat
		File pp = new File(resourceDir, "pp.dat");
		ObjectInputStream productProducerInput;
		try {
			productProducerInput = new ObjectInputStream(new FileInputStream(pp));
			producerOfProduct = (Integer[]) productProducerInput.readObject();
			scalefactor = producerOfProduct[producerOfProduct.length-1];
		} catch(IOException e) {
			System.err.println("Could not open or process file " + pp.getAbsolutePath());
			System.err.println(e.getMessage());
			System.exit(-1);
		}
		catch(ClassNotFoundException e) { System.err.println(e); }
	
		//Offer-Vendor Relationships from resourceDir/vo.dat
		File vo = new File(resourceDir, "vo.dat");
		ObjectInputStream offerVendorInput;
		try {
			offerVendorInput = new ObjectInputStream(new FileInputStream(vo));
			vendorOfOffer = (Integer[]) offerVendorInput.readObject();
		} catch(IOException e) {
			System.err.println("Could not open or process file " + pp.getAbsolutePath());
			System.err.println(e.getMessage());
			System.exit(-1);
		}
		catch(ClassNotFoundException e) { System.err.println(e); }
		
		//Review-Rating Site Relationships from resourceDir/rr.dat
		File rr = new File(resourceDir, "rr.dat");
		ObjectInputStream reviewRatingsiteInput;
		try {
			reviewRatingsiteInput = new ObjectInputStream(new FileInputStream(rr));
			ratingsiteOfReview = (Integer[]) reviewRatingsiteInput.readObject();
		} catch(IOException e) {
			System.err.println("Could not open or process file " + rr.getAbsolutePath());
			System.err.println(e.getMessage());
			System.exit(-1);
		}
		catch(ClassNotFoundException e) { System.err.println(e); }
		//Current date and words of Product labels from resourceDir/cdlw.dat
		File cdlw = new File(resourceDir, "cdlw.dat");
		ObjectInputStream currentDateAndLabelWordsInput;
		try {
			currentDateAndLabelWordsInput = new ObjectInputStream(new FileInputStream(cdlw));
			productCount = currentDateAndLabelWordsInput.readInt();
			reviewCount = currentDateAndLabelWordsInput.readInt();
			offerCount = currentDateAndLabelWordsInput.readInt();
			currentDate = (GregorianCalendar) currentDateAndLabelWordsInput.readObject();
			currentDateString = DateGenerator.formatDate(currentDate);
			wordHash = (HashMap<String, Integer>) currentDateAndLabelWordsInput.readObject();
			wordList = wordHash.keySet().toArray(new String[0]);
		} catch(IOException e) {
			System.err.println("Could not open or process file " + cdlw.getAbsolutePath());
			System.err.println(e.getMessage());
			System.exit(-1);
		}
		catch(ClassNotFoundException e) { System.err.println(e); }
	}
	
	/*
	 * (non-Javadoc)
	 * @see benchmark.testdriver.AbstractParameterPool#getParametersForQuery(benchmark.testdriver.Query)
	 */
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
}
