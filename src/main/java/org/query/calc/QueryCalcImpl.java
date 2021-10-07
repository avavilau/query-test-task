package org.query.calc;

import it.unimi.dsi.fastutil.doubles.Double2DoubleRBTreeMap;
import it.unimi.dsi.fastutil.doubles.Double2DoubleSortedMap;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;

public class QueryCalcImpl implements QueryCalc {

    private static final int LIMIT = 10;

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
        // TODO: Implement following query, put a reasonable effort into making it efficiently from perspective of
        //  computation time, memory usage and resource utilization (in that exact order). You are free to use any lib
        //  from a maven central.
        //
        // SELECT a, SUM(x * y * z) as s
        // FROM t1
        // JOIN t2
        // JOIN t3
        // WHERE a < b + c
        // GROUP BY a
        // STABLE ORDER BY s DESC
        // Limit 10
        // 
        // Note: STABLE is not a standard SQL command. It means that you should preserve the original order. 
        // In this context it means, that in case of tie on s-value you should prefer value of a, with a lower row number.
        // In case multiple occurrences, you may assume that group has a row number of the first occurrence.

        Row[] values1 = readTable(t1);
        Row[] values2 = readTable(t2);
        Row[] values3 = readTable(t3);

        // Sort t1 by key to cache the aggregated (y * z) sum. This breaks stability by row number,
        //  so we need to include the RN in the final sort condition
        Arrays.sort(values1, Comparator.comparingDouble(Row::getKey).reversed());

        // Reverse order makes it easier to work with `subMap` method
        Double2DoubleRBTreeMap sumMap = new Double2DoubleRBTreeMap(Comparator.reverseOrder());

        for (Row r1 : values2) {
            for (Row r2 : values3) {
                double sum = r1.getKey() + r2.getKey();
                double mult = r1.getValue() * r2.getValue();
                sumMap.mergeDouble(sum, mult, Double::sum);
            }
        }

        Row[] answer = new Row[values1.length];

        int i = 0;
        // if there are duplicated `t1.a`, the output size is less than values1.length
        int outputSize = 0;
        double prevKey = Double.MAX_VALUE;
        // cache (y * z) sum for An. (y * z) sum for A(n+1) can be calculated as `sum(An) + delta sum`. This ensures there is only 1 pass over (a + b) sums
        double yzSum = 0d;
        while (i < values1.length) {
            Row row = values1[i++];
            double key = row.getKey();
            // Corner case, we won't have `b + c` greater than `a` if `a` is `Double.MAX_VALUE`
            if (key != Double.MAX_VALUE) {
                // t2 + t3 records for which join condition return true on this iteration, but not on previous iteration
                Double2DoubleSortedMap deltaJoinRecords = sumMap.subMap(prevKey, key);
                yzSum += deltaJoinRecords.values().doubleStream().sum();
                // Set upper bound for the next iteration as current key
                prevKey = key;
            }

            // Aggregate `a` values
            double aggregatedXs = row.getValue();
            while (i < values1.length && values1[i].getKey() == key) {
                aggregatedXs += values1[i++].getValue();
            }

            answer[outputSize++] = new Row(key, aggregatedXs * yzSum, row.getRowNumber());
        }

        // Stable sort by `s` and rowNumber
        Arrays.sort(answer, 0, outputSize,
                Comparator.comparingDouble(Row::getValue).reversed().thenComparingInt(Row::getRowNumber));
        if (outputSize > LIMIT) {
            answer = Arrays.copyOfRange(answer, 0, LIMIT);
            outputSize = LIMIT;
        }

        writeResult(output, answer, outputSize);

        // Running time analysis. `O(n) = n2 * n3 * log(n2 * n3)` determined by cross product of  t2, t3:
        //   1. n1 * log(n1) - t1 sort by `a`
        //   2. n2 * n3 * log(n2 * n3) - cross product t2, t3 and TreeMap population
        //   3. n2 * n3 - rolling `y * z` sum
        //   4. n1 * log(n2 * n3) - join condition `a < b + c` using subMap and `x * y * z` sum
        //   5. n1 * log(n1) - final sort by `s` and row number
    }

    private Row[] readTable(Path p) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(p)) {
            int nEntries = Integer.parseInt(reader.readLine());
            Row[] table = new Row[nEntries];
            String line;
            int recordId = 0;
            while ((line = reader.readLine()) != null) {
                String[] split = line.split(" ");
                table[recordId] = new Row(Double.parseDouble(split[0]), Double.parseDouble(split[1]), recordId + 1);
                recordId++;
            }
            return table;
        }
    }

    private void writeResult(Path output, Row[] result, int size) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(output)) {
            writer.write(Integer.toString(size));
            writer.newLine();

            for (Row r : result) {
                writer.write(String.format("%.6f %.6f", r.getKey(), r.getValue()));
                writer.newLine();
            }

            writer.flush();
        }
    }
}
