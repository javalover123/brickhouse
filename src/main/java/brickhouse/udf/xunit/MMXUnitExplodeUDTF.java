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
 *
 */


import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.log4j.Logger;

@Description(
		name="mm_xunit_explode", 
		value="_FUNC_(array<struct<dim:string,attr_names:array<string>,attr_values:array<string>>, int, boolean) - ",
		extended="SELECT _FUNC_(uid, ts), uid, ts, event_type from foo;")
public class MMXUnitExplodeUDTF extends XUnitExplodeUDTF {
	private static final Logger LOG = Logger.getLogger( MMXUnitExplodeUDTF.class);
	protected boolean eventExplode = false;

	@Override
	public void process(Object[] args) throws HiveException {
		@SuppressWarnings("unchecked")
		List<Object>  dimValuesList = (List<Object>) listInspector.getList(args[0]);

		if( globalFlagInspector != null ) {
			boolean globalFlag = globalFlagInspector.get( args[2]);
			if(globalFlag) {
				forwardXUnit(GLOBAL_UNIT );
			} else {
				eventExplode = true;
			}
		} else {
			forwardXUnit(GLOBAL_UNIT );
		}

		if(maxDimInspector != null) {
			maxDims = maxDimInspector.get( args[1]);
		}

		try {
			//eventExplode = true;

			if(dimValuesList != null && dimValuesList.size() > 0  ) {

				Object firstStruct = dimValuesList.get(0);
				List<Object> otherStructs = dimValuesList.subList( 1, dimValuesList.size() );
				List<XUnitDesc> allCombos = combinations( firstStruct, otherStructs);

				int xunitsGenerated = 0;
				int xunitsFiltered = 0;
				for( XUnitDesc xunit : allCombos ) {
					String xunitVal = xunit.toString() ;
					int numDims = xunit.numDims();

					//When exploding events the following logic should be followed:
					// 1. Only explode xunits that have /event/e=* in the xunit
					// 2. For xunits of dimension size 2, only explode xunits that have /spam/* segment
					// 3. For xunits of dimension size greater or equal to 3, only expose xunits that have /spam/is_spam=nonspammer* ypath present
					if(eventExplode) {
						if(numDims > 1) {
							if(xunitVal.contains("/event/e=") && xunitVal.contains("/spam/") ) {
								//System.out.println("PASS 1: " + xunitVal);
								if(numDims >= 3) {
									if(xunitVal.contains("/spam/is_spam=nonspammer")) {
										forwardXUnit(xunitVal);
										xunitsGenerated++;
									} else {
										xunitsFiltered ++;
									}
								} else {
									forwardXUnit(xunitVal);
									xunitsGenerated++;
								}
							} else {
								xunitsFiltered ++;
								//System.out.println("FILTER1: " + xunitVal);
							}
						} else {
							if(xunitVal.contains("/event/e=") ) {
								//System.out.println("PASS 2: " + xunitVal);
								forwardXUnit(xunitVal);
								xunitsGenerated ++;
							} else {
								xunitsFiltered ++;
								//System.out.println("FILTER2: " + xunitVal);
							}
						}
					} else {
						// When exploding DAU xunits, following logic should be followed:
						// 1. Explode any xunits that only have single dimensions
						// 2. Explode only xunits of dimension size > 1 that have /spam/is_spam=nonspammer* segment present
						if(numDims > 1) {
							if(xunitVal.contains("/spam/is_spam=nonspammer") ) {
								forwardXUnit(xunitVal);
								xunitsGenerated ++;
							} else {
								xunitsFiltered ++;
							}
						} else {
							forwardXUnit(xunitVal);
							xunitsGenerated ++;
						}
					}
				}
				incrCounter("NumXUnitsExploded", xunitsGenerated);
				incrCounter("NumXUnitsFiltered" , xunitsFiltered);
			}
		} catch(IllegalArgumentException illArg) {
			LOG.error("Error generating XUnits", illArg);
		}
	}
	
	protected boolean hasEvent(String xunit) {
		return xunit.contains("/event/e=");
	}
	
	protected boolean hasCustom(String xunit) {
		return xunit.contains("/custom/c");
	}
	
	protected boolean nonSpammer(String xunit) {
		return xunit.matches(".*(/spam/is_spam=nonspammer).*");
	}
	
	@Override
	public boolean shouldIncludeXUnit(XUnitDesc xunit ) {
		int numDims = maxDims;
		int xunitNumDims = xunit.numDims();

		String xunitVal = xunit.toString();
		if(hasCustom(xunitVal)) {
			numDims ++;
		}
		return xunitNumDims <= numDims;
	}

    @Override
    protected void incrCounter( String counterName, long counter) {
    	LOG.info("XUnitExplode counter incremented by " + counter);
     }
    
	@Override
	protected void forwardXUnit( String xunit) throws HiveException {
		LOG.info(" Forwarding XUnit " + xunit);
	}
}
