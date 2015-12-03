package brickhouse.udf.xunit;

import java.util.Arrays;


//// XXX Create Class which models the XUnit 
/// Make them immutable, like strings,
/// So we can build them up from previous
public class XUnitDesc implements Comparable<XUnitDesc> {
	private YPathDesc[] _ypaths;
	
	public XUnitDesc( YPathDesc yp) {
       _ypaths = new YPathDesc[]{ yp };
	}
	public XUnitDesc( YPathDesc[] yps) {
       _ypaths = yps;
	}

	public int compareTo(XUnitDesc other) {
		return toString().compareTo(other.toString());
	}
	
	public XUnitDesc addYPath(YPathDesc yp) {
	   YPathDesc[] newYps = new YPathDesc[ _ypaths.length + 1];
	   //// Prepend the YPath ..
	   newYps[0] = yp;
	   for(int i=1; i<newYps.length; ++i) {
		  newYps[i] = _ypaths[i -1];
	   }
	   return new XUnitDesc( newYps);
	}
	
	public XUnitDesc extend(YPathDesc yp) {
		// post-pends the new yp
		YPathDesc[] newYps = Arrays.copyOf(_ypaths, _ypaths.length + 1);
		newYps[_ypaths.length] = yp;
		return new XUnitDesc(newYps);
	}
	
	public int numDims() { return _ypaths.length; }
	
	public String toString() {
		YPathDesc[] sortedYPs = Arrays.copyOf(_ypaths, _ypaths.length);
		Arrays.sort(sortedYPs); // sort the copy
		StringBuilder sb = new StringBuilder();
		sb.append( sortedYPs[0].toString() );
		for(int i=1; i<sortedYPs.length; ++i) {
	       sb.append(',');
	       sb.append( sortedYPs[i].toString() );
		}
		return sb.toString();
	}
	
}