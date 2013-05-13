package main;
import java.io.*;
import java.util.*;

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
	if ( ((p.p1.equals(this.p1)) && (p.p2.equals(this.p2))) || ((p.p1.equals(this.p2)) && (p.p2.equals(this.p1))) ) {
	    return true;
	}
	return false;
    }

    @Override
    public int hashCode() {
	return p1 * p2;
    }

    @Override
    public String toString() {
	return ("( " + p1.toString() + ", " + p2.toString() + " )");
    }
}
