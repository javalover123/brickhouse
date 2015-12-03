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
 * The parent class, XUnitExplodeUDTF, relies on a recursive, depth-first
 * traversal of a sorted tree, with the shortest paths emitted by the leaves.
 * That approach eliminates generation of equivalent paths, but requires
 * complete traversal of the nodes in the tree.  That is, for an 11 dimensional
 * case, we generate all 1586 dimensional combos even though we know we'll toss
 * out 2/3 of them for having more than the maximum number of dimensions.  Scale
 * up to more dimensions and it gets even worse. And when we add the multiplying
 * effect of ypaths (to handle attributes) into the mix, we're burning a bunch
 * of cycles for no benefit.  And this happens for every event.
 * 
 * This class tries to minimize the number of wasted cycles.  We use an off the
 * shelf library (https://github.com/dpaukov/combinatoricslib) to generate the
 * nCr cases we care about more efficiently.  We also take advantage of some
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
	
    // overrides of parent methods
    @Override
	public boolean shouldIncludeXUnit(XUnitDesc xunit ) {
    	//LOG.info("should include: [" + (xunit.numDims() <= maxDims) + "] : " + xunit);
    	return xunit.numDims() <= maxDims;	
	}
    
	@Override
	public void process(Object[] args) throws HiveException {
    	Map<String, Object> dimMap = new HashMap<String, Object>();
    	for (Object struct: listInspector.getList(args[0])) {
    		dimMap.put(structDimName(struct), struct);
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
			for (XUnitDesc xunit : constrainedExplode(dimMap)) {
				if(shouldIncludeXUnit(xunit)) {
					forwardXUnit(xunit.toString());
				} else {
					LOG.warn("unexpected filter kick-in for " + xunit.toString());
				}
			}
		} catch(IllegalArgumentException illArg) {
			LOG.error("Error generating XUnits", illArg);
		}
	}

    @Override
    protected void incrCounter( String counterName, long counter) {
    	LOG.info("XUnitExplode counter incremented by " + counter);
     }
    
	@Override
	protected void forwardXUnit( String xunit) throws HiveException {
		LOG.info(" Forwarding XUnit " + xunit);
	}
	
	// class-specific methods
    
    protected Collection<XUnitDesc> constrainedExplode(Map<String, Object> dimMap) {
    	Collection<ICombinatoricsVector<String>> dimCombinations = null;
    	
    	// we handle event and spam dimensions differently, so pull them first
    	String spamVal = null;
    	if (dimMap.containsKey("event")) {
    		eventStruct = dimMap.get("event");
    	}
    	if (dimMap.containsKey("spam")) {
    		spamStruct = dimMap.get("spam");
			spamVal = structAttrValues(spamStruct).get(0);
    	}
   	
    	// TODO: pull custom dim structs?
    	
    	if (eventStruct != null && !globalFlag) {
    		// event-centric processing -- all paths _must_have_an_event_
        	Map<String, Object> reqDimMap = new HashMap<String, Object>(2);
        	Map<String, Object> optDimMap = new HashMap<String, Object>(dimMap);
    		reqDimMap.put(structDimName(eventStruct), eventStruct);
    		optDimMap.remove(structDimName(eventStruct));
    		if (spamVal == "nonspammer-validated") {
    			// process to target max depth
    			reqDimMap.put(structDimName(spamStruct), spamStruct);
        		optDimMap.remove(structDimName(spamStruct));
    			dimCombinations = combos(reqDimMap.keySet(), optDimMap.keySet(), maxDims);
    		} else {
    			// reduce max depth to 2 (i.e. -- event + spam)
    			dimCombinations = combos(reqDimMap.keySet(), optDimMap.keySet(), 2);
    		}
    	} else if (globalFlag) {
    		// DAU processing
    		if (spamVal == "nonspammer-validated") {
    	    	Map<String, Object> reqDimMap = new HashMap<String, Object>(1);
    	    	Map<String, Object> optDimMap = new HashMap<String, Object>(dimMap);
    			// process to max target depth
    			reqDimMap.put(structDimName(spamStruct), spamStruct);
        		optDimMap.remove(structDimName(spamStruct));
        		dimCombinations = combos(reqDimMap.keySet(), optDimMap.keySet(), maxDims);
    		} else {
    			// reduce max depth to 2
    			dimCombinations = combos(Collections.<String>emptySet(), dimMap.keySet(), 2);
    		}
    	} else {
    		// Uh-oh.
    		LOG.warn("Non-global call on row without event defined. " + 
    				structCollectionToString(dimMap.values()));
    	}
    	
    	return generateXUnits(dimCombinations, dimMap);
    }

    protected List<ICombinatoricsVector<String>> combos(Set<String> required, Set<String> optional, int maxDepth) {
    	List<ICombinatoricsVector<String>> dimCombos = new LinkedList<ICombinatoricsVector<String>>();
 
    	if (required != null && required.size() > maxDepth) {
    		String m = "Max depth " + maxDepth + " and " + required.size() + " dimensions " + required;
    		LOG.error(m);
    		throw new RuntimeException(m);
    	}
    	
    	// get the required combos set up first
		int rSize = (required != null) ? required.size() : 0;
		Generator<String> reqGen = null;
		if (rSize > 0) {
			ICombinatoricsVector<String> reqVec = Factory.createVector(required);
			reqGen = Factory.createSimpleCombinationGenerator(reqVec, rSize);
	    }
		
		// now get the subsets of the max-length optional combos
		int oSize = (optional != null) ? optional.size() : 0;
		Generator<String> optGen = null;
		if (oSize > 0 && maxDims > rSize) {
			ICombinatoricsVector<String> optVec = Factory.createVector(optional);
			int maxOpt = (oSize > (maxDims - rSize)) ? (maxDims - rSize) : oSize;
			optGen = Factory.createSimpleCombinationGenerator(optVec, maxOpt);
		}

		if (reqGen != null) {
			for (ICombinatoricsVector<String> root : reqGen) {
				if (optGen == null) {
					dimCombos.add(root);
				} else {
					for (ICombinatoricsVector<String> opt : optGen) {
						Generator<String> subGen = Factory.createSubSetGenerator(opt);
						for (ICombinatoricsVector<String> subOpt : subGen) {
							int cSz = root.getSize() + subOpt.getSize();
							List<String> combo = new ArrayList<String>(cSz);
							combo.addAll(root.getVector());
							combo.addAll(subOpt.getVector());
							dimCombos.add(Factory.createVector(combo));
						}
					}
				}
			}
		} else if (optGen != null) {
			for (ICombinatoricsVector<String> opt : optGen) {
				Generator<String> subGen = Factory.createSubSetGenerator(opt);
				for (ICombinatoricsVector<String> subOpt : subGen) {
					dimCombos.add(subOpt);
				}
			}
		}
    	return dimCombos;
    }

    
    protected List<XUnitDesc> generateXUnits(Collection<ICombinatoricsVector<String>> dimCombos, Map<String, Object> dimMap) {
    	List<XUnitDesc> xunits = new LinkedList<XUnitDesc>();
    	for (ICombinatoricsVector<String> combo : dimCombos) {
    		// create a combo-specific list of struct objects
    		List<Object>structList = new ArrayList<Object>(combo.getSize());
    		for (String dim: new TreeSet<String>(combo.getVector()) ) {
    			structList.add(dimMap.get(dim));
    		}
    		xunits.addAll(rx(structList));
    	}
    	return xunits;
    }

    protected List<XUnitDesc> rx(List<Object>structList) {
    	if (structList.size() < 1)
    		throw new RuntimeException("rx() recursed below 1 element");
    	
    	List<XUnitDesc> xunits = new LinkedList<XUnitDesc>();
		Object struct = structList.get(0);
    	if (structList.size() == 1) {
    		for (YPathDesc yp : generateYPaths(struct)) {
    			xunits.add(fromPath(yp));
			}
    	} else {
    		for( XUnitDesc sub: rx(structList.subList(1, structList.size()))) {
    	    	for (YPathDesc yp : generateYPaths(struct)) {
    	    		xunits.add(sub.addYPath(yp));
    			}
    		}
    	}
    	return xunits;
    }

	// struct utils
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
