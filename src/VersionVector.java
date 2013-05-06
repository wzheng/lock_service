import java.io.*;
import java.util.*;

/**
 * Version vector, used by PartitionTable
 */

public class VersionVector {

    private int[] vv;

    public VersionVector(int size) {
        vv = new int[size];

        for (int i = 0; i < size; i++) {
            vv[i] = 0;
        }
    }

    public void increment(int pos) {
        vv[pos] = vv[pos] + 1;
    }

}