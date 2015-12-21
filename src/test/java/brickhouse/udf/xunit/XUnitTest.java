package brickhouse.udf.xunit;

import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.Assert;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.log4j.Logger;


public class XUnitTest {
	private static final Logger LOG = Logger.getLogger(XUnitTest.class);
	private XUnitUDTF exploderUDTF = null;
	private XUnitUDTF xploder = null;
	private ObjectInspector[] oiList = {
		ObjectInspectorFactory.getStandardListObjectInspector(getStructOI()),
		PrimitiveObjectInspectorFactory.javaIntObjectInspector,
		PrimitiveObjectInspectorFactory.javaBooleanObjectInspector
	};
	
	private HashMap<String, Long> counterMap = new HashMap<String, Long>();
	private int forwards = 0;
	
	// The struct defining segmentation dimensions always has three fields.
	static String[] fieldNames = {"dim", "attr_names", "attr_values"};

	/**
	 * We can use the same OI for all instances of a seg dimension struct.
	 */
	StandardStructObjectInspector getStructOI() {
		ArrayList<String> fieldNameList = new ArrayList<String>(Arrays.asList(fieldNames));
		ObjectInspector stringOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		ArrayList<ObjectInspector> oiList = new ArrayList<ObjectInspector>(3);
		oiList.add(stringOI);
		oiList.add(ObjectInspectorFactory.getStandardListObjectInspector(stringOI));
		oiList.add(ObjectInspectorFactory.getStandardListObjectInspector(stringOI));
		return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNameList, oiList);
	}

	/**
	 * A convenience method for validating a seg dimension struct object instance.
	 */
	void validateStructObject(Object struct, String dim, List<String> attrNames, List<String> attrVals) {
		StandardStructObjectInspector soi = getStructOI();
		List<? extends StructField> fields = soi.getAllStructFieldRefs();
		Assert.assertEquals(dim, soi.getStructFieldData(struct, fields.get(0)));
		Assert.assertEquals(attrNames, soi.getStructFieldData(struct, fields.get(1)));
		Assert.assertEquals(attrVals, soi.getStructFieldData(struct, fields.get(2)));
	}
	
	/**
	 * A convenience method for creating a seg dimension struct object instance.
	 */
	ArrayList<Object> getStructObject(String dim, List<String> attrNames, List<String> attrVals, boolean validate) {
		ArrayList<Object> struct = new ArrayList<Object>(3);
		struct.add(dim);
		struct.add(attrNames);
		struct.add(attrVals);
		if (validate) {
			validateStructObject(struct, dim, attrNames, attrVals);
		}
		return struct;
	}

	ArrayList<Object> getStructObject(String dim, List<String> attrNames, List<String> attrVals) {
		return getStructObject(dim, attrNames, attrVals, true);
	}
	
	/**
	 * Generates an instance of a seg dim struct with two attribute levels.
	 */
	Object getOrdTwoStruct(String ordVal, String subordVal) {
		List<String> attrNameList = new ArrayList<String>(2);
		attrNameList.add(0, "ord");
		attrNameList.add(1, "subord");
		List<String> attrValueList = new ArrayList<String>(2);
		attrValueList.add(0, ordVal);
		attrValueList.add(1, subordVal);
		return getStructObject("odim", attrNameList, attrValueList);
	}
	
	/**
	 * Generates an instance of a seg dim struct with three attribute levels.
	 */
	Object getAlphaThreeStruct(String alphaVal, String betaVal, String gammaVal) {
		List<String> attrNameList = new ArrayList<String>(3);
		attrNameList.add(0, "alpha");
		attrNameList.add(1, "beta");
		attrNameList.add(2, "gamma");
		List<String> attrValueList = new ArrayList<String>(3);
		attrValueList.add(0, alphaVal);
		attrValueList.add(1, betaVal);
		attrValueList.add(2, gammaVal);
		return getStructObject("adim", attrNameList, attrValueList);
	}
	
	Object getOneAttrStruct(String dimName, String attrName, String attrVal, boolean validate) {
		List<String> attrNameList = new ArrayList<String>(1);
		attrNameList.add(0, attrName);
		List<String> attrValueList = new ArrayList<String>(1);
		attrValueList.add(0, attrVal);
		return getStructObject(dimName, attrNameList, attrValueList, validate);
	}
	
	Object getEventStruct(String eventVal) {
		return getOneAttrStruct("event", "e", eventVal, false);
	}
	
	Object getSpamStruct(String spamVal) {
		return getOneAttrStruct("spam", "is_spam", spamVal, false);
	}

	Object getAgeStruct(String ageVal) {
		return getOneAttrStruct("age", "bucket", ageVal, false);
	}
	
	Object getCohortStruct(String cohortVal) {
		return getOneAttrStruct("cohort", "signup", cohortVal, false);
	}

	Object getEthnicityStruct(String ethnicityVal) {
		return getOneAttrStruct("ethnicity", "e", ethnicityVal, false);
	}
	
	Object getGenderStruct(String genderVal) {
		return getOneAttrStruct("gender", "sex", genderVal, false);
	}

	Object getGeoStruct(String continent, String country, String state) {
		List<String> attrNameList = new ArrayList<String>(3);
		attrNameList.add(0, "continent");
		attrNameList.add(1, "country");
		attrNameList.add(2, "state");
		List<String> attrValueList = new ArrayList<String>(3);
		attrValueList.add(0, continent);
		attrValueList.add(0, country);
		attrValueList.add(0, state);
		return getStructObject("geo", attrNameList, attrValueList, false);
	}
	
	Object getPlatformStruct(String superPlatform, String platform) {
		List<String> attrNameList = new ArrayList<String>(2);
		attrNameList.add(0, "p");
		attrNameList.add(1, "p2");
		List<String> attrValueList = new ArrayList<String>(2);
		attrValueList.add(0, superPlatform);
		attrValueList.add(0, platform);
		return getStructObject("platform", attrNameList, attrValueList, false);
	}
	
	Object getReligionStruct(String religionVal) {
		return getOneAttrStruct("religion", "r", religionVal, false);
	}
	
	Object getRetentionStruct(String retentionVal) {
		return getOneAttrStruct("retention", "r", retentionVal, false);
	}

	Object getSexPrefStruct(String sexPrefVal) {
		return getOneAttrStruct("sex_pref", "pref", sexPrefVal, false);
	}

	Object[] getMeetmeEventStructArray() {
		Object[] structs = {
				// required
				getEventStruct("meetme"),
				getSpamStruct("nonspammer-validated"),
				// custom
				getOneAttrStruct("custom", "c1_meetme__vote", "Y", false),
				getOneAttrStruct("custom", "c2_meetme__is_match", "1", false),
				getOneAttrStruct("custom", "c3_meetme__platform", "Web", false),
				// optional
				getAgeStruct("25-34"),
				getCohortStruct("7"),
				getEthnicityStruct("Other"),
				getGenderStruct("F"),
				getGeoStruct("NA", "USA", "CA"),
				getPlatformStruct("Desktop", "Desktop Web"),
				getReligionStruct("O"),
				getSexPrefStruct("G")
			};
		return structs;
	}
	
	Object[] getSpammerMeetmeEventStructArray() {
		Object[] structs = {
				// required
				getEventStruct("meetme"),
				getSpamStruct("spammer-validated"),
				// custom
				getOneAttrStruct("custom", "c1_meetme__vote", "Y", false),
				getOneAttrStruct("custom", "c2_meetme__is_match", "1", false),
				getOneAttrStruct("custom", "c3_meetme__platform", "Web", false),
				// optional
				getAgeStruct("25-34"),
				getCohortStruct("7"),
				getEthnicityStruct("Other"),
				getGenderStruct("F"),
				getGeoStruct("NA", "USA", "CA"),
				getPlatformStruct("Desktop", "Desktop Web"),
				getReligionStruct("O"),
				getSexPrefStruct("G")
			};
		return structs;
	}
	
	Object[] getProcessArgs(Object[] structs, int max, boolean globalFlag) {
		ArrayList<Object> structList = new ArrayList<Object>();
		for (Object struct : structs) {
			structList.add(struct);
		}		
		Object[] dims = { structList, new Integer(max), new Boolean(globalFlag) };
		return dims;
	}
	Object[] getProcessArgs(Object[] structs, int max) {
		return getProcessArgs(structs, max, true);
	}
	Object[] getProcessArgs(Object[] structs) {
		return getProcessArgs(structs, 3, true);
	}

	@SuppressWarnings("deprecation")
	void initExploder(Class<? extends XUnitUDTF> exploderClazz) {
		try {
			exploderUDTF = (XUnitUDTF)exploderClazz.newInstance();
			exploderUDTF.initialize(this.oiList);
			xploder = spy(exploderUDTF);

			//doNothing().when(xploder).incrCounter(anyString(), anyLong());
			doAnswer(new Answer<Void>() { 
				public Void answer(InvocationOnMock inv) {
					Object[] args = inv.getArguments();
					String counterName = (String)args[0];
					long counter = (long)args[1];
					LOG.debug("call to set " + counterName + " to " + counter);
					counterMap.put(counterName, new Long(counter));
					return null;
				}
			}).when(xploder).incrCounter(anyString(), anyLong());

			//doNothing().when(xploder).forwardXUnit(anyString());
			doAnswer(new Answer<Void>() { 
				public Void answer(InvocationOnMock inv) {
					Object[] args = inv.getArguments();
					LOG.debug("mocked XUnit forward: " + args[0]);
					forwards++;
					return null;
				}
			}).when(xploder).forwardXUnit(anyString());
			
			
			
		} catch (Throwable t) {
			throw new RuntimeException(t);
		}
	}
	
	///////////////////////
	// SET UP / TAKE DOWN
	///////////////////////
	
	@Before
	public void setUp() { ; }
	
	/////////
	// TESTS
	/////////

	@Test
	public void initStandardExploder() {
		initExploder(XUnitExplodeUDTF.class);
	}
	
	@Test
	public void initTaggedExploder() {
		initExploder(TaggedXUnitExplodeUDTF.class);
	}
	
	@Test
	public void initConstrainedExploder() {
		initExploder(ConstrainedXUnitExplodeUDTF.class);
	}
	
	@Ignore
	@Test
	public void explode1d1a() throws HiveException {
		initExploder(XUnitExplodeUDTF.class);
		List<String> nList = new ArrayList<String>(1);
		nList.add(0, "alpha");
		List<String> vList = new ArrayList<String>(1);
		vList.add(0, "a");
		Object[] structs = { getStructObject("adim", nList, vList) };
		xploder.process(getProcessArgs(structs, 1, true));
	}
	
	@Ignore
	@Test
	public void explode1d2a() throws HiveException {
		initExploder(XUnitExplodeUDTF.class);
		Object[] structs = { getOrdTwoStruct("1", "2") };
		xploder.process(getProcessArgs(structs));
	}

	@Ignore
	@Test
	public void explode2d() throws HiveException {
		initExploder(XUnitExplodeUDTF.class);
		Object[] structs = {
			getOrdTwoStruct("1", "2"), 
			getAlphaThreeStruct("a", "b", "c") 
		};
		xploder.process(getProcessArgs(structs));
	}

	@Ignore
	@Test
	public void explodeEvent4dchoose3() throws HiveException {
		initExploder(XUnitExplodeUDTF.class);
		Object[] structs = {
			getOrdTwoStruct("1", "2"), 
			getAlphaThreeStruct("a", "b", "c"), 
			getEventStruct("s_page_view_api"),
			getSpamStruct("nonspammer-validated")
		};
		int maxDepth = 3;
		boolean global = false;
		xploder.process(getProcessArgs(structs, maxDepth, global));
	}

	@Ignore
	@Test
	public void taggedExplodeEvent4dchoose3() throws HiveException {
		initExploder(TaggedXUnitExplodeUDTF.class);
		Object[] structs = {
			getOrdTwoStruct("1", "2"), 
			getAlphaThreeStruct("a", "b", "c"), 
			getEventStruct("s_page_view_api"),
			getSpamStruct("nonspammer-validated")
		};
		int maxDepth = 3;
		boolean global = false;
		xploder.process(getProcessArgs(structs, maxDepth, global));
	}

	@Ignore
	@Test
	public void constrainedExplodeEvent4dchoose3() throws HiveException {
		initExploder(ConstrainedXUnitExplodeUDTF.class);
		Object[] structs = {
			getOrdTwoStruct("1", "2"), 
			getAlphaThreeStruct("a", "b", "c"), 
			getEventStruct("s_page_view_api"),
			getSpamStruct("nonspammer-validated")
		};
		int maxDepth = 3;
		boolean global = false;
		xploder.process(getProcessArgs(structs, maxDepth, global));
	}

	@Ignore
    @Test
    public void eventWithCustomTaggedExplode() throws UDFArgumentException, HiveException {
		initExploder(TaggedXUnitExplodeUDTF.class);
		Object[] structs = getMeetmeEventStructArray();
		int maxDepth = 4;
		boolean global = false;
		xploder.process(getProcessArgs(structs, maxDepth, global));
		LOG.info("event w/ customs, taggedExplode: forwards=" + forwards);
		LOG.info(counterMap);
    }

	@Ignore
    @Test
    public void eventWithCustomConstrainedExplode() throws UDFArgumentException, HiveException {
		initExploder(ConstrainedXUnitExplodeUDTF.class);
		doAnswer(new Answer<Void>() { 
			public Void answer(InvocationOnMock inv) {
				Object[] args = inv.getArguments();
				LOG.info((String)args[0]);
				forwards++;
				return null;
			}
		}).when(xploder).forwardXUnit(anyString());
		
		Object[] structs = getMeetmeEventStructArray();
		int maxDepth = 4;
		boolean global = false;
		xploder.process(getProcessArgs(structs, maxDepth, global));
		LOG.info("event w/ customs, constrained: forwards=" + forwards);
		LOG.info(counterMap);
    }

	@Ignore
	@Test
    public void eventWithCustomConstrainedExplodex100() throws UDFArgumentException, HiveException {
		initExploder(ConstrainedXUnitExplodeUDTF.class);
		Object[] structs = getMeetmeEventStructArray();
		int maxDepth = 4;
		boolean global = false;
		long t0 = System.currentTimeMillis();
		for (int i=0; i<100; i++) {
			xploder.process(getProcessArgs(structs, maxDepth, global));
		}
		long t1 = System.currentTimeMillis();
		LOG.info("avg explode time (ms): constrained: " + ((t1-t0) / 100));
    }

    @Test
    public void compareEventWithCustomExplodes() throws UDFArgumentException, HiveException {
		Object[] structs = getMeetmeEventStructArray();
		int maxDepth = 4;
		boolean global = false;
    	// explode with ConstrainedXUnitExplodeUDTF
		initExploder(ConstrainedXUnitExplodeUDTF.class);
    	final HashSet<String> cSet = new HashSet<String>();
		doAnswer(new Answer<Void>() { 
			public Void answer(InvocationOnMock inv) {
				Object[] args = inv.getArguments();
				cSet.add((String)args[0]);
				forwards++;
				return null;
			}
		}).when(xploder).forwardXUnit(anyString());

		long t0 = System.currentTimeMillis();
		xploder.process(getProcessArgs(structs, maxDepth, global));
		long t1 = System.currentTimeMillis();

    	// explode with TaggedXUnitExplodeUDTF
		initExploder(TaggedXUnitExplodeUDTF.class);
    	final HashSet<String> tSet = new HashSet<String>();
		doAnswer(new Answer<Void>() { 
			public Void answer(InvocationOnMock inv) {
				Object[] args = inv.getArguments();
				tSet.add((String)args[0]);
				forwards++;
				return null;
			}
		}).when(xploder).forwardXUnit(anyString());
		long t2 = System.currentTimeMillis();
		xploder.process(getProcessArgs(structs, maxDepth, global));
		long t3 = System.currentTimeMillis();
		
		LOG.info("sizes: constrained=" + cSet.size() + ", tagged=" + tSet.size());
		LOG.info("explode time (ms): constrained: " + (t1-t0) + ", tagged=" + (t3-t2));

		HashSet<String> cc = new HashSet<String>(cSet);
		cc.removeAll(tSet);
		LOG.info("in C not T:");
		for (String cXunit : cc)
			LOG.info(cXunit);
		
		HashSet<String> tc = new HashSet<String>(tSet);
		tc.removeAll(cSet);
		LOG.info("\nin T not C:");
		for (String tXunit : tc)
			LOG.info(tXunit);
    }
    
	@Ignore
    @Test
    public void DAUTaggedExplode() throws UDFArgumentException, HiveException {
		initExploder(TaggedXUnitExplodeUDTF.class);
		Object[] structs = {
				getSpamStruct("nonspammer-validated"),
				getEventStruct("meetme"),
				getAgeStruct("25-34"),
				getGeoStruct("NA", "USA", "CA"),
				getPlatformStruct("Desktop", "Desktop Web")
			};
		int maxDepth = 3;
		boolean global = true;
		xploder.process(getProcessArgs(structs, maxDepth, global));
    }
	
    @Ignore
    @Test
    public void DAUConstrainedExplode() throws UDFArgumentException, HiveException {
		initExploder(ConstrainedXUnitExplodeUDTF.class);
		Object[] structs = {
				getSpamStruct("nonspammer-validated"),
				getEventStruct("meetme"),
				getAgeStruct("25-34"),
				getGeoStruct("NA", "USA", "CA"),
				getPlatformStruct("Desktop", "Desktop Web")
			};
		int maxDepth = 4;
		boolean global = true;
		xploder.process(getProcessArgs(structs, maxDepth, global));
		LOG.info("DAU, constrained: forwards=" + forwards);
		//LOG.info(counterMap);
    }
}