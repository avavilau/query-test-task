package org.query.calc;

import it.unimi.dsi.fastutil.doubles.Double2DoubleMap;
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

        // Sort t1 by key to group it by key later. This breaks stability by row number, so we need to include the RN in the final sort condition
        Arrays.sort(values1, Comparator.comparingDouble(Row::getKey).reversed());

        JoinAggregationStrategy joinAggregationStrategy = findOptimalStrategy(values1, values2, values3);

        Row[] answer = new Row[values1.length];

        int i = 0;
        // if there are duplicated `t1.a`, the output size is less than values1.length
        int outputSize = 0;
        while (i < values1.length) {
            Row row = values1[i++];
            double key = row.getKey();
            double yzSum = joinAggregationStrategy.getYzAggregation(key);
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

        // Running time analysis. Both JoinAggregationStrategy implementation have running time complexity O(n^2 * log(n)).
        // However, we make sure the 2 smallest arrays are used in the quadratic operation.
        //   1. n1 * log(n1) - t1 sort by `a`
        //   2. n^2 * log(n) - Join with aggregation
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

    private JoinAggregationStrategy findOptimalStrategy(Row[] values1, Row[] values2, Row[] values3) {
        int crossProductV1V2 = values1.length * values2.length;
        int crossProductV1V3 = values1.length * values3.length;
        int crossProductV2V3 = values2.length * values3.length;

        int smallest = Math.min(crossProductV1V2, Math.min(crossProductV1V3, crossProductV2V3));

        if (smallest == crossProductV2V3) {
            // running time n2 * n3 * log(n2 * n3)
            return new AdditionJoinAggregationStrategy(values2, values3);
        } else if (smallest == crossProductV1V2) {
            // running time n1 * n2 * log(n3)
            return new SubtractionJoinAggregationStrategy(values2, values3);
        } else {
            // running time n1 * n3 * log(n2)
            return new SubtractionJoinAggregationStrategy(values3, values2);
        }
    }

    private interface JoinAggregationStrategy {

        double getYzAggregation(double key);
    }

    private static class AdditionJoinAggregationStrategy implements JoinAggregationStrategy {

        private double yzSum;
        private double prevKey;
        private final Double2DoubleRBTreeMap sumMap;

        // Constructor running time: n2 * n3 * log(n2 * n3) - cross product t2, t3 and TreeMap population
        AdditionJoinAggregationStrategy(Row[] values2, Row[] values3) {
            // cache (y * z) sum for An. (y * z) sum for A(n+1) can be calculated as `sum(An) + delta sum`. This ensures there is only 1 pass over (a + b) sums
            yzSum = 0d;
            prevKey = Double.MAX_VALUE;

            // Reverse order makes it easier to work with `subMap` method
            sumMap = new Double2DoubleRBTreeMap(Comparator.reverseOrder());

            for (Row r1 : values2) {
                for (Row r2 : values3) {
                    double sum = r1.getKey() + r2.getKey();
                    double mult = r1.getValue() * r2.getValue();
                    sumMap.mergeDouble(sum, mult, Double::sum);
                }
            }
        }

        // aggregation calculation running time: log(n2 * n3) - join condition `a < b + c` using subMap
        @Override
        public double getYzAggregation(double key) {
            // Corner case, we won't have `b + c` greater than `a` if `a` is `Double.MAX_VALUE`
            if (key != Double.MAX_VALUE) {
                // t2 + t3 records for which join condition return true on this iteration, but not on previous iteration
                Double2DoubleSortedMap deltaJoinRecords = sumMap.subMap(prevKey, key);
                yzSum += deltaJoinRecords.values().doubleStream().sum();
                // Set upper bound for the next iteration as current key
                prevKey = key;
            }

            return yzSum;
        }
    }

    private static class SubtractionJoinAggregationStrategy implements JoinAggregationStrategy {

        private static final double DELTA = 1e-8;

        private final Double2DoubleRBTreeMap subtractMap;
        private final Double2DoubleRBTreeMap searchMap;
        private final double[] rollingSearchMapSum;

        // Constructor running time: `n * log(n) + N * log(N) + N`, where N > n - TreeMap population and running sum
        SubtractionJoinAggregationStrategy(Row[] smallerTable, Row[] biggerTable) {
            // save smallerTable to reversed tree map, so we can stop traversal as soon as there is no term in searchMap
            // that gives sum greater that join key. It is better than sort because we can merge duplicated keys.
            subtractMap = new Double2DoubleRBTreeMap(Comparator.reverseOrder());
            for (Row r : smallerTable) {
                subtractMap.mergeDouble(r.getKey(), r.getValue(), Double::sum);
            }

            // save biggerTable to reversed tree map, so we can find all keys that will give sum greater than `a` in logarithmic time.
            searchMap = new Double2DoubleRBTreeMap(Comparator.reverseOrder());
            for (Row r : biggerTable) {
                searchMap.mergeDouble(r.getKey(), r.getValue(), Double::sum);
            }

            // Pre-calculate all possible `z` sums
            rollingSearchMapSum = new double[searchMap.size()];
            int rollingSearchIndex = 0;
            for (double value : searchMap.values()) {
                if (rollingSearchIndex == 0) {
                    rollingSearchMapSum[rollingSearchIndex] = value;
                } else {
                    rollingSearchMapSum[rollingSearchIndex] = rollingSearchMapSum[rollingSearchIndex - 1] + value;
                }
                rollingSearchIndex++;
            }

        }

        // Aggregation running time: n * log(N), where N > n - iteration over subtractMap and search in searchMap
        @Override
        public double getYzAggregation(double key) {
            double yzSum = 0;
            for (Double2DoubleMap.Entry e : subtractMap.double2DoubleEntrySet()) {
                // `case-2` contains a nasty record for which machine subtraction gives lower result than it should actually be:
                //   18.9..95 instead of 0.19. SearchMap returns an additional record in this case and final result is invalid.
                //   Cause `searchMap.headMap` returns everything greater than `diff` and we do not operate with values that
                //   have scales similar to DELTA, we can simply add DELTA to the subtraction (when subtraction is valid this
                //   small addition won't make any difference). Working with BigDecimal will make the app mich slower.
                double diff = key - e.getDoubleKey() + DELTA;
                Double2DoubleSortedMap joinRecords = searchMap.headMap(diff);
                if (joinRecords.isEmpty()) {
                    // The rest of the records are lower than `e`, we won't find any key in searchMap for them.
                    break;
                } else {
                    yzSum += e.getDoubleValue() * rollingSearchMapSum[joinRecords.size() - 1];
                }
            }
            return yzSum;
        }
    }
}
