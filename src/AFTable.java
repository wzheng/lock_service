import java.io.*;
import java.util.*;

/**
 *  This is a table that stores affinity factors betwwen partitions.
 *  The higher the AF, the more related the partitions
 */

public class AFTable {

    public class Pair {
	public Integer p1;
	public Integer p2;

	public Pair(Integer i, Integer j) {
	    p1 = i;
	    p2 = j;
	}

	@Override
	public boolean equals(Object obj) {
	    if (obj == null) {
		return false;
	    }
	    if (obj == this) {
		return true;
	    }
	    if (!(obj instanceof Pair)) {
		return false;
	    }
	    
	    Pair p = (Pair) obj;
	    if ( ((p.p1 == this.p1) && (p.p2 == this.p2)) || ((p.p1 == this.p2) && (p.p2 == this.p1)) ) {
		return true;
	    }
	    return false;
	}

	public int hashCode() {
	    return p1 * p2;
	}
    }

    private HashMap<Pair, Integer> afTable;
    
    public AFTable() {
	afTable = new HashMap<Pair, Integer>();
    }

    // increment by 1
    public void increment(int partition1, int partition2) {
	Pair p = new Pair(partition1, partition2);
	Integer i = afTable.get(p);
	if (i == null) {
	    afTable.put(p, new Integer(1));
	} else {
	    afTable.put(p, new Integer(i.intValue() + 1));
	}
    }

}