package benchmarks;

import java.lang.Math;


public class Util {

    public static int randNum(int lower, int higher) {
	return lower + (int)(Math.random() * (higher - lower + 1));
    }
    
}