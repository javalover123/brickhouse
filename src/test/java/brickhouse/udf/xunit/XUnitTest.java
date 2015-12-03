package brickhouse.udf.xunit;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;

import org.junit.Before;
import org.junit.Test;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.log4j.Logger;


public class XUnitTest extends TestCase {
	private static final Logger LOG = Logger.getLogger( XUnitTest.class);
	private XUnitExplodeUDTF xploder = null;
	private ObjectInspector[] oiList = {
		ObjectInspectorFactory.getStandardListObjectInspector(getStructOI()),
		PrimitiveObjectInspectorFactory.javaIntObjectInspector,
		PrimitiveObjectInspectorFactory.javaBooleanObjectInspector
	};
	
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
		assertEquals(dim, soi.getStructFieldData(struct, fields.get(0)));
		assertEquals(attrNames, soi.getStructFieldData(struct, fields.get(1)));
		assertEquals(attrVals, soi.getStructFieldData(struct, fields.get(2)));
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
	
	@Test
	public void testProcessOneOne() throws UDFArgumentException, HiveException {
		XUnitExplodeUDTF xploder = new XUnitExplodeUDTF();
		ObjectInspector[] oiList = { ObjectInspectorFactory.getStandardListObjectInspector(getStructOI()) };
		xploder.initialize(oiList);
		
		List<String> nList = new ArrayList<String>(1);
		nList.add(0, "alpha");
		List<String> vList = new ArrayList<String>(1);
		vList.add(0, "a");
		
		Object struct = getStructObject("adim", nList, vList);
		validateStructObject(struct, "adim", nList, vList);
		
		ArrayList<Object> structList = new ArrayList<Object>(1);
		structList.add(struct);
		Object[] dims = { structList };
		//xploder.process(dims);
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
	
	@Before
	public void setUp() throws UDFArgumentException {
		this.xploder = new ConstrainedXUnitExplodeUDTF();
		this.xploder.initialize(this.oiList);
	}
	
	//@Test
	public void xtestProcessOneOne() throws UDFArgumentException, HiveException {
		List<String> nList = new ArrayList<String>(1);
		nList.add(0, "alpha");
		List<String> vList = new ArrayList<String>(1);
		vList.add(0, "a");
		Object[] structs = { getStructObject("adim", nList, vList) };
		xploder.process(getProcessArgs(structs));
	}
	
	//@Test
	public void xtestProcessOrd() throws UDFArgumentException, HiveException {
		Object[] structs = { getOrdTwoStruct("1", "2") };
		xploder.process(getProcessArgs(structs));
	}

	
	//@Test
	public void xtestProcessOrdAlpha() throws UDFArgumentException, HiveException {
		Object[] structs = {
			getOrdTwoStruct("1", "2"), 
			getAlphaThreeStruct("a", "b", "c") 
		};
		xploder.process(getProcessArgs(structs));
	}

	//@Test
	public void testProcessFourDim() throws UDFArgumentException, HiveException {
		Object[] structs = {
			getOrdTwoStruct("1", "2"), 
			getAlphaThreeStruct("a", "b", "c"), 
			getEventStruct("s_page_view_api"),
			getSpamStruct("nonspammer-validated")
		};
		xploder.process(getProcessArgs(structs, 3, false));
	}

    @Test
    public void testEventExplodeXUnit() throws UDFArgumentException, HiveException {
		Object[] structs = {
				getEventStruct("meetme"),
				getSpamStruct("nonspammer-validated"),
				getAgeStruct("25-34"),
				getOneAttrStruct("custom", "c1_profile_view__friends", "NA", false),
				getOneAttrStruct("custom", "c2_profile_view__platform", "Web", false),
				getOneAttrStruct("custom", "test", "W2", false),
				getGeoStruct("NA", "USA", "CA"),
				getPlatformStruct("Desktop", "Desktop Web")
			};
		int maxDepth = 3;
		xploder.process(getProcessArgs(structs, maxDepth, false));
    }

    //@Test
    public void xtestEventTaggedExplodeXUnit() throws UDFArgumentException, HiveException {
		Object[] structs = {
				getEventStruct("meetme"),
				getSpamStruct("nonspammer-validated"),
				getAgeStruct("25-34"),
				getOneAttrStruct("custom", "c1_profile_view__friends", "NA", false),
				getOneAttrStruct("custom", "c2_profile_view__platform", "Web", false),
				getOneAttrStruct("custom", "test", "W2", false),
				getGeoStruct("NA", "USA", "CA"),
				getPlatformStruct("Desktop", "Desktop Web")
			};
		int maxDepth = 3;
    	GenericUDTF tXploder = new TaggedXUnitExplodeUDTF();
		tXploder.process(getProcessArgs(structs, maxDepth, false));
    }

    @Test
    public void testDAUExplodeXUnit() throws UDFArgumentException, HiveException {
		Object[] structs = {
				getSpamStruct("nonspammer-validated"),
				getEventStruct("meetme"),
				getAgeStruct("25-34"),
				getGeoStruct("NA", "USA", "CA"),
				getPlatformStruct("Desktop", "Desktop Web")
			};
		int maxDepth = 3;
		xploder.process(getProcessArgs(structs, maxDepth, true));
    }
	
    //@Test
    public void xtestDAUTaggedExplodeXUnit() throws UDFArgumentException, HiveException {
		Object[] structs = {
				getSpamStruct("nonspammer-validated"),
				getEventStruct("meetme"),
				getAgeStruct("25-34"),
				getGeoStruct("NA", "USA", "CA"),
				getPlatformStruct("Desktop", "Desktop Web")
			};
		int maxDepth = 3;
    	GenericUDTF tXploder = new TaggedXUnitExplodeUDTF();
    	tXploder.process(getProcessArgs(structs, maxDepth, true));
    }
}