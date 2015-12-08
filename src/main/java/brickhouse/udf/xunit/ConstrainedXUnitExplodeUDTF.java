package brickhouse.udf.xunit;
/**
 * Copyright 2015 if(we).co
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */


import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.paukov.combinatorics.Factory;
import org.paukov.combinatorics.Generator;
import org.paukov.combinatorics.ICombinatoricsVector;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.log4j.Logger;

/**
 * When we explode a set of dimensions into xunits, we limit potential paths to
 * a maximum length -- that is, to a maximum number of the available dimensions
 * used for a particular xunit.  An xunit is equivalent to a _combination_ of
 * up to d dimensions, where d is the max path depth.
 * 
 * So, if we have 11 dimensions available and we have a max depth of 2, paths
 * can contain either one of the 11, or two.  The total number of valid paths is
 * the sum of "n choose r" (or "nCr") with n=11 and r=2 plus nCr(11,1):
 * 
 * d=2: 11! / (2! * (11-2)!) = 11! / (2! * 9!) = 39916800 / (2 * 362880) = 55
 * d=1: 11! / (1! * (11-1)!) = 11! / 10! = 39916800 / 3628800 = 11
 * total combos up to d=2: 66
 * 
 * A deeper set of paths, say 4 deep, gives us the above plus nCr(11,4) and
 * nCr(11,3), or:
 * 
 * 11! / (4! * (11-4)!) = 11! / (4! * 7!) = 39916800 / (24 * 5040) = 330
 * 11! / (3! * (11-3)!) = 11! / (3! * 8!) = 39916800 / (6 * 40320) = 165
 * total combos up to d=4: 11 + 55 + 165 + 330 = 561
 * 
 * The complete set adds nCr for (11,0), (11,5), (11,6), (11,7), (11,8), (11,9),
 * (11,10), and (11,11) (i.e. -- 1, 462, 330, 165, 55, 11, 1) for 1586 total.
 * 
 * This class' parent class, XUnitExplodeUDTF, relies on a recursive, depth-
 * first traversal of a sorted tree, with the shortest paths emitted by the
 * leaves. That approach eliminates generation of equivalent paths, but requires
 * complete traversal of the nodes in the tree.  That is, for an 11 dimensional
 * case, we generate all 1586 dimensional combos even though we know we'll toss
 * out 2/3 of them for having more than the maximum number of dimensions.  Scale
 * up to more dimensions and it gets even worse. And when we add the multiplying
 * effect of ypaths (to handle attributes) into the mix, we're burning a bunch
 * of cycles for no benefit.  And this happens for every event.
 * 
 * Unsurprisingly, this class tries to avoid those wasted cycles.We use an off
 * the shelf library (https://github.com/dpaukov/combinatoricslib) to generate
 * the nCr cases we care about more efficiently.  We also take advantage of some
 * domain-specific details of our use case, allowing us to tailor the explode
 * around event, spam, and custom segmentation dimensions.
 */
@Description(
		name="constrained_xunit_explode", 
		value="_FUNC_(array<struct<dim:string,attr_names:array<string>,attr_values:array<string>>, int, boolean) - ",
		extended="SELECT _FUNC_(uid, ts), uid, ts, event_type from foo;")
public class ConstrainedXUnitExplodeUDTF extends XUnitExplodeUDTF {
	private static final Logger LOG = Logger.getLogger( ConstrainedXUnitExplodeUDTF.class);

	protected Object spamStruct = null;
	protected Object eventStruct = null;
	protected boolean globalFlag = true;
	
	////////////////////////////////
	// overrides of parent methods
	////////////////////////////////

	@Override
	public boolean shouldIncludeXUnit(XUnitDesc xunit ) {
		if (xunit.toString().contains("/custom/c")) {
			return xunit.numDims() <= (maxDims + 1);
		}
		return xunit.numDims() <= maxDims;
	}
	
	@Override
	public void process(Object[] args) throws HiveException {
    	Map<String, Object> dimMap = new HashMap<String, Object>();
    	for (Object struct: listInspector.getList(args[0])) {
    		String dimName = structDimName(struct);
    		if (dimName == "custom" && structAttrNames(struct).size() > 0) {
    			// prevent key collision in the map for custom dims
    			dimName = dimName + structAttrNames(struct).get(0);
    		}
    		dimMap.put(dimName, struct);
    	}
    	
		if(maxDimInspector != null) {
			maxDims = maxDimInspector.get( args[1]);
		}

		if( globalFlagInspector != null ) {
			globalFlag = globalFlagInspector.get( args[2] );
		}
		if(globalFlag) {
			forwardXUnit(GLOBAL_UNIT );
		}
		
		try {
			Collection<XUnitDesc> xunits = constrainedExplode(dimMap, false);
			for (XUnitDesc xunit : xunits) {
				if(shouldIncludeXUnit(xunit)) {
					forwardXUnit(xunit.toString());
				} else {
					LOG.warn("unexpected filter kick-in for " + xunit.toString());
				}
			}
			incrCounter("NumXUnits", xunits.size());
		} catch(IllegalArgumentException illArg) {
			LOG.error("Error generating XUnits", illArg);
		}
	}

	//////////////////////////
	// class-specific methods
	//////////////////////////
    protected Collection<XUnitDesc> constrainedExplode(Map<String, Object> dimMap, boolean sort) {
    	Collection<ICombinatoricsVector<String>> dimCombinations = null;
    	int minDims = (maxDims <= 2) ? maxDims : 2;

		if (maxDims > minDims) {
			// for combinations, we work with the map keys only
	    	HashSet<String> reqDims = new HashSet<String>();
	    	HashSet<String> optDims = new HashSet<String>(dimMap.keySet());

	    	if (globalFlag) {
		    	// for DAU (i.e. -- globalFlag==true), all optional & no custom dims

	    		// first grab all the [1,minDims] combos
				dimCombinations = blindCombos(optDims, 1, minDims);
	    		
	    		if (dimMap.containsKey("spam")) {
		    		spamStruct = dimMap.get("spam");
					String spamVal = structAttrValues(spamStruct).get(0);
	    			if (spamVal == "nonspammer-validated") {
	    				reqDims.add(structDimName(spamStruct));
	    				optDims.remove(structDimName(spamStruct));
	    				// non-spammer, so generate XUnits out to maxDims
	    	    		dimCombinations.addAll(reqCombos(reqDims, optDims, minDims, maxDims));
	    			}
	    		}
	    	} else {
	    		// event dim is _required_, customs may be present
	    		if(!dimMap.containsKey("event"))
	    			throw new RuntimeException("non-global explode must have an event");

	    		// shift dims from optDims to reqDims as needed
	    		eventStruct = dimMap.get("event");
	    		reqDims.add(structDimName(eventStruct));
	        	optDims.remove(structDimName(eventStruct));
	        	// start with the event by itself -- the minimum valid vector
	    		dimCombinations = new LinkedList<ICombinatoricsVector<String>>();
	    		dimCombinations.add(Factory.createVector(reqDims));

	        	String spamVal = null;
	        	if (dimMap.containsKey("spam")) {
		    		spamStruct = dimMap.get("spam");
					spamVal = structAttrValues(spamStruct).get(0);
	    			if (spamVal == "nonspammer-validated") {
	    				reqDims.add(structDimName(spamStruct));
	    				optDims.remove(structDimName(spamStruct));
	    				// with spam added as required, add the 2-dim vector
	    	    		dimCombinations.add(Factory.createVector(reqDims));
	    			}
		    	}

		    	// shift custom dims from optDims to customDims 
	    		HashSet<String> customDims = new HashSet<String>();
		    	for(String key: dimMap.keySet()) {
		    		if (key.startsWith("custom")) {
		    			customDims.add(key);
		    			optDims.remove(key);
		    		}
		    	}

		    	// combos without the req dims are unimportant
		    	minDims = (minDims < reqDims.size()) ? reqDims.size() : minDims;

		    	if (reqDims.contains("spam")) {
			    	// start with non-custom combos
			    	dimCombinations.addAll(reqCombos(reqDims, optDims, minDims, maxDims));

			    	// add combos that include a custom dim to maxDims+1
			    	if (!customDims.isEmpty()) {
			    		dimCombinations.addAll(customCombos(reqDims, customDims, optDims, minDims, maxDims+1));
			    	}
		    	} else {
			    	// spammy non-custom combos to depth of 2
			    	dimCombinations.addAll(reqCombos(reqDims, optDims, minDims, 2));

			    	// spammy combos that include a custom dim to depth of 3
			    	//if (!customDims.isEmpty()) {
			    	//	dimCombinations.addAll(customCombos(reqDims, customDims, optDims, minDims, 3));
			    	//}
		    	}
	    	}
		}

    	
    	List<XUnitDesc> xunits = generateXUnits(dimCombinations, dimMap);
    	LOG.debug("pre-sort xunits: " + xunits.size());
    	if (sort) {
    		TreeSet<XUnitDesc> xset = new TreeSet<XUnitDesc>(xunits);
        	LOG.info("post-sort xunits: " + xset.size());
    		return xset;
    	}
    	return xunits;
    }

    /**
     * All dims are optional.  This just returns all combinations of the dims
     * up to the maxDepth.
     */
    protected List<ICombinatoricsVector<String>> blindCombos(Set<String> optional, int minDepth, int maxDepth) {
    	if (maxDepth < minDepth) {
    		LOG.warn("blindCombos called with maxDepth < minDepth");
    		return Collections.<ICombinatoricsVector<String>> emptyList();
    	}
    	
    	List<ICombinatoricsVector<String>> dimCombos = new LinkedList<ICombinatoricsVector<String>>();
		ICombinatoricsVector<String> optVec = Factory.createVector(optional);
		Generator<String> optGen = Factory.createSimpleCombinationGenerator(optVec, maxDepth);
		for (ICombinatoricsVector<String> opt : optGen) {
			dimCombos.add(opt);
		}
		if (maxDepth > minDepth)
			dimCombos.addAll(blindCombos(optional, minDepth, maxDepth-1));
		return dimCombos;
    }

    /**
     * Required are required, customs are cycled through, added as required.
     * This is usually called with a higher depth.  This will ONLY return 
     * combinations with at least one custom dimension included.
     */
    protected List<ICombinatoricsVector<String>> customCombos(Set<String> required, Set<String> custom, Set<String> optional, int minDepth, int maxDepth) {
    	if (custom == null || custom.isEmpty())
    		return Collections.<ICombinatoricsVector<String>> emptyList();

    	if (required != null && required.size() + custom.size() == 0)
    		return Collections.<ICombinatoricsVector<String>> emptyList();

    	if (required == null)
    		required = new HashSet<String>();
    	
    	if ((required.size() + 1) <= maxDepth) {
        	List<ICombinatoricsVector<String>> customCombos = new LinkedList<ICombinatoricsVector<String>>();
			Set<String>effCustom = new HashSet<String>(custom); // modifiable
			for (String cDim : custom) {
				LOG.debug("cDim: " + cDim);
				int ersz = required.size() + 1;
				List<String> effRoot = new ArrayList<String>(required);
				effRoot.add(cDim);
				effCustom.remove(cDim); // effCustom shrinks on each loop

				int eosz = optional.size() + effCustom.size();
				List<String> effOpt = new ArrayList<String>(eosz);
				effOpt.addAll(optional);
				effOpt.addAll(effCustom);
				ICombinatoricsVector<String> effOptVec = Factory.createVector(effOpt);
				Generator<String> optGen = Factory.createSimpleCombinationGenerator(effOptVec, (maxDepth - ersz));
				for (ICombinatoricsVector<String> opt : optGen) {
					LOG.debug("opt: " + opt);
					int cSz = effRoot.size() + opt.getSize();
					List<String> combo = new ArrayList<String>(cSz);
					combo.addAll(effRoot);
					combo.addAll(opt.getVector());
					LOG.debug("combo: " + combo);
					customCombos.add(Factory.createVector(combo));
				}
				
				if (maxDepth > minDepth) {
					// recurse to get lower depth combos
					customCombos.addAll(customCombos(required, custom, optional, minDepth, maxDepth-1));
				}
			}
			return customCombos;
    	}
    	
		return Collections.<ICombinatoricsVector<String>> emptyList();
    }
    
    /**
     * Required are required, optionals are optional.  We do not consider any
     * customs in this method.  This is usually called with maxDims as maxDepth.
     */
    protected List<ICombinatoricsVector<String>> reqCombos(Set<String> required, Set<String> optional, int minDepth, int maxDepth) {
    	if (required == null || required.isEmpty())
    		return Collections.<ICombinatoricsVector<String>> emptyList();

    	if (required.size() <= maxDepth) {
        	List<ICombinatoricsVector<String>> reqCombos = new LinkedList<ICombinatoricsVector<String>>();
			ICombinatoricsVector<String> effOptVec = Factory.createVector(optional);
			Generator<String> optGen = Factory.createSimpleCombinationGenerator(effOptVec, (maxDepth - required.size()));
			for (ICombinatoricsVector<String> opt : optGen) {
				LOG.debug("opt: " + opt);
				int cSz = required.size() + opt.getSize();
				List<String> combo = new ArrayList<String>(cSz);
				combo.addAll(required);
				combo.addAll(opt.getVector());
				LOG.debug("combo: " + combo);
				reqCombos.add(Factory.createVector(combo));
			}
			if (maxDepth > minDepth) {
				// recurse to get lower depth combos
				reqCombos.addAll(reqCombos(required, optional, minDepth, maxDepth-1));
			}
			return reqCombos;
    	}
		return Collections.<ICombinatoricsVector<String>> emptyList();
    }
    
    protected List<XUnitDesc> generateXUnits(Collection<ICombinatoricsVector<String>> dimCombos, Map<String, Object> dimMap) {
    	List<XUnitDesc> xunits = new LinkedList<XUnitDesc>();
    	for (ICombinatoricsVector<String> combo : dimCombos) {
    		LOG.debug("generating XUnit from: " + combo);
    		// create a combo-specific list of struct objects
    		List<Object>structList = new ArrayList<Object>(combo.getSize());
    		for (String dim: new TreeSet<String>(combo.getVector()) ) {
    			structList.add(dimMap.get(dim));
    		}
    		xunits.addAll(recursiveXUnitGeneration(structList));
    	}
    	return xunits;
    }
    
    protected List<XUnitDesc> recursiveXUnitGeneration(List<Object>structList) {
    	//
    	// SLOW! -- hitting generateYPaths ,multiple times, extrac call overhead
    	//
    	if (structList.size() < 1)
    		throw new RuntimeException("rx() recursed below 1 element");
    	
    	List<XUnitDesc> xunits = new LinkedList<XUnitDesc>();
		Object struct = structList.get(0);
    	if (structList.size() == 1) {
    		for (YPathDesc yp : generateYPaths(struct)) {
    			xunits.add(fromPath(yp));
			}
    	} else {
    		for( XUnitDesc sub: recursiveXUnitGeneration(structList.subList(1, structList.size()))) {
    	    	for (YPathDesc yp : generateYPaths(struct)) {
    	    		xunits.add(sub.addYPath(yp));
    			}
    		}
    	}
    	return xunits;
    }
	
    ////////////////
	// struct utils
    ////////////////
	protected String structDimName(Object struct) {
    	return structInspector.getStructFieldData(struct, dimField).toString();
    }

	@SuppressWarnings("unchecked")
	protected List<String> structAttrNames(Object struct) {
		return (List<String>)(structInspector.getStructFieldData(struct, attrNamesField));
	}
    
    @SuppressWarnings("unchecked")
	protected List<String> structAttrValues(Object struct) {
    	return (List<String>)(structInspector.getStructFieldData(struct, attrValuesField));
    }

    protected String structCollectionToString(Collection<Object> structCollection) {
		StringBuilder sb = new StringBuilder();
		sb.append('[');
		boolean first = true;
		for (Object struct: structCollection) {
			if (first) {
				first = false;
			} else {
				sb.append(',');
			}
			sb.append('<');
			sb.append(structDimName(struct));
			sb.append('>');
			sb.append(structAttrNames(struct).toString());
			sb.append('=');
			sb.append(structAttrValues(struct).toString());
		}
		sb.append(']');
		return sb.toString();
    }
}
