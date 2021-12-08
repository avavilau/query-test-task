package org.query.calc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class QueryCalcImpl implements QueryCalc {
    static final int RETURN_LIMIT = 10;

    /**
     * A class which stores 2 doubles.
     * TODO: possibly use Lombok to implement setters/getters/constructors. Possibly use some existing pair class
     */
    public static class DoublePair implements Comparable<DoublePair> {
        private double d1, d2;
        public DoublePair(double d1, double d2) {
            this.d1 = d1;
            this.d2 = d2;
        }

        public int compareTo(DoublePair o) {
            return Double.compare(d1, o.d1);
        }

        public double getD1() {
            return d1;
        }

        public double getD2() {
            return d2;
        }

        public void setD1(double d1) {
            this.d1 = d1;
        }

        public void setD2(double d2) {
            this.d2 = d2;
        }
    }

    /*
     A class which stores a double and a line number from the original table.
    */
    public static class DoubleLineNum implements Comparable<DoubleLineNum> {
        private double d;
        private int lineNum;

        public DoubleLineNum(double d, int lineNum) {
            this.d = d;
            this.lineNum = lineNum;
        }

        public int compareTo(DoubleLineNum o) {
            // reverse order to make sure we sort in descending order
            int ret = Double.compare(o.d, this.d);
            if (ret == 0) {
                return Integer.compare(this.lineNum, o.lineNum);
            } else {
                return ret;
            }
        }

        public double getDouble() {
            return d;
        }

        public int getLineNum() {
            return lineNum;
        }

        public void setDouble(double d) {
            this.d = d;
        }

        public void setLineNum(int lineNum) {
            this.lineNum = lineNum;
        }
    }

    @Override
    public void select(Path t1, Path t2, Path t3, Path output) throws IOException {
        // - t1 is a file contains table "t1" with two columns "a" and "x". First line is a number of rows, then each
        //  line contains exactly one row, that contains two numbers parsable by Double.parse(): value for column a and
        //  x respectively.See test resources for examples.
        // - t2 is a file contains table "t2" with columns "b" and "y". Same format.
        // - t3 is a file contains table "t3" with columns "c" and "z". Same format.
        // - output is table stored in the same format: first line is a number of rows, then each line is one row that
        //  contains two numbers: value for column a and s.
        //
        // Number of rows of all three tables lays in range [0, 1_000_000].
        // It's guaranteed that full content of all three tables fits into RAM.
        // It's guaranteed that full outer join of at least one pair (t1xt2 or t2xt3 or t1xt3) of tables can fit into RAM.
        //
        // TODO: Implement following query, put a reasonable effort into making it efficient from perspective of
        //  computation time, memory usage and resource utilization (in that exact order). You are free to use any lib
        //  from a maven central.
        //
        // SELECT a, SUM(x * y * z) AS s FROM 
        // t1 LEFT JOIN (SELECT * FROM t2 JOIN t3) AS t
        // ON a < b + c
        // GROUP BY a
        // STABLE ORDER BY s DESC
        // LIMIT 10;
        // 
        // Note: STABLE is not a standard SQL command. It means that you should preserve the original order. 
        // In this context it means, that in case of tie on s-value you should prefer value of a, with a lower row number.
        // In case multiple occurrences, you may assume that group has a row number of the first occurrence.

        /*
        First, I'm going to assume that each table contains n rows. The problem description doesnt say anything about
        this. In the real world, its possible to implement multiple strategies and then apply them depending on the data
        characteristics.

        There is a  join on 2 sides of a < b+c. Left side contains n rows while the right side contains n*n rows.
        There are 2 options of how they can be implemented:

        1. For every element on the right, find all a's such as a<b+c. Accumulate x*y*z in each a. This computation will
        take O(n*n*n). Sorting t1 by a isn't going to help all that much since we would still need to scan O(n) rows
        in t1 (a < b+c). Then, we would scan t1 and find top10. So, its O(n^3)

        2. Compute t2 join t3 in ram. (Store only {b+c, y*z}). Sort by b+c, iterate from the top and keep the running
        sum. Replace b*c with the sum(b*c) for such b,c that b+c > the given b+c

        3. Read t1 and store it in a hashmap with a as a key. For value of a, lookup b+c which are greater than a and
        sum precomputed xyz. Store these sums in the hashtable along with the row number for the first given value of a

        4.  Find top 10 in the hashtable using a sorted container. Use row number from the original table to break ties.

        ToDo:
        - more testing and some benchmarking

        More optimization opportunities:
        I use standard Java containers here to store objects which contain either 2 doubles or a double and an integer.
        This will incure a pretty significant cost of an object pointers and pointer indirection. Also, this will not
        be good for the CPU cash. We should look into using a different language or may be some sort of a native
        memory allocation technique

        I'm pretty sure, many operations here could be parallelized. May be by using streams library. It could also
        be done manually.


         */

        // Read t3 into RAM
        ArrayList<DoublePair> at3 = null;
        try (BufferedReader t3reader = Files.newBufferedReader(t3)) {
            String line = t3reader.readLine();
            int numLines = Integer.parseInt(line);
            at3 = new ArrayList<>(numLines);
            while ((line = t3reader.readLine()) != null) {
                String[] a = line.split(" ");
                at3.add(new DoublePair(Double.parseDouble(a[0]), Double.parseDouble(a[1])));
            }
        }

        // Read t2 and store join of t2 and t3 in RAM
        // at2JoinAt3 is (t2.b+t3.c, t2.y*t3.z)
        ArrayList<DoublePair> t2xt3 = null;
        try (BufferedReader t2reader = Files.newBufferedReader(t2)) {
            String line = t2reader.readLine();
            int numLines = Integer.parseInt(line);
            t2xt3 = new ArrayList<>(numLines * at3.size());
            while ((line = t2reader.readLine()) != null) {
                String[] a = line.split(" ");
                Double d1 = Double.parseDouble(a[0]);
                Double d2 = Double.parseDouble(a[1]);

                for (DoublePair dp3 : at3) {
                    t2xt3.add(new DoublePair(dp3.d1+d1, dp3.d2*d2));
                }
            }
        }

        // sort at2JoinAt3 by b+c
        Collections.sort(t2xt3);

        // replace y*z with the sum of y*z
        double runningSum = 0.0;
        for (int i=t2xt3.size()-1; i >= 0; i--) {
            runningSum += t2xt3.get(i).getD2();
            t2xt3.get(i).setD2(runningSum);
        }

        // Read t1 into hashtable and compute sums of xyz for each value of a. Also store the row number as a tie breaker
        HashMap<Double, DoubleLineNum> hmt1 = new HashMap<>();
        try (BufferedReader t1reader = Files.newBufferedReader(t1)) {
            String line = t1reader.readLine();
            int numLines = Integer.parseInt(line);
            int lineNum = 0;
            while ((line = t1reader.readLine()) != null) {
                String[] splitLine = line.split(" ");
                Double a = Double.parseDouble(splitLine[0]);
                Double x = Double.parseDouble(splitLine[1]);

                // find all b+c which are greater than a
                int i = Collections.binarySearch(t2xt3, new DoublePair(a, 0.0));
                if (i < 0) {
                    i = -i-1;
                }
                while (i < t2xt3.size() && t2xt3.get(i).getD1() == a) {
                    i++;
                }
                double xyz = 0.0;
                if (i < t2xt3.size()) {
                    xyz = x * t2xt3.get(i).getD2();
                }

                if (hmt1.containsKey(a)) {
                    // If this value has been seen, accumulate the sum but keep the row number for the first occurence
                    // of a
                    hmt1.get(a).setDouble(hmt1.get(a).getDouble()+xyz);
                } else {
                    hmt1.put(a, new DoubleLineNum(xyz, lineNum));
                }
                lineNum++;
            }
        }

        // Use the sorted map to find top 10
        TreeMap<DoubleLineNum, Double> tm = new TreeMap<>();
        for (Map.Entry<Double, DoubleLineNum> entry : hmt1.entrySet()) {
            if (tm.size() < RETURN_LIMIT || tm.lastKey().compareTo(entry.getValue()) == 1) {
                if (tm.size() == RETURN_LIMIT) {
                    // Do not grow the sorted map to more than 10 elements
                    tm.pollLastEntry();
                }
                tm.put(entry.getValue(), entry.getKey());
            }
        }

        try (BufferedWriter writer = Files.newBufferedWriter(output)) {
            writer.write(String.format("%d\n", tm.size()));
            for (Map.Entry<DoubleLineNum, Double> entry : tm.entrySet()) {
                writer.write(String.format("%f %f\n", entry.getValue(), entry.getKey().getDouble()));
            }
        }
    }
}
