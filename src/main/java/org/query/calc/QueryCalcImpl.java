package org.query.calc;

import com.google.common.collect.ComparisonChain;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

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

        List<double[]> t1table = readTable(t1);
        List<double[]> t2table = readTable(t2);
        List<double[]> t3table = readTable(t3);

        LinkedHashMap<Double, Double> grouped = joinAndGroup(t1table, t2table, t3table);

        PriorityQueue<ResultItem> ordered = order(grouped);

        List<ResultItem> result = limit(ordered);

        writeResultTable(output, result);
    }

    private LinkedHashMap<Double, Double> joinAndGroup(List<double[]> t1table,
                                                       List<double[]> t2table,
                                                       List<double[]> t3table) {
        LinkedHashMap<Double, Double> result = new LinkedHashMap<>(t1table.size());
        for (double[] t1row : t1table) {
            Double sum = result.get(t1row[0]);
            if (sum == null) {
                sum = 0d;
            }
            for (double[] t2row : t2table) {
                for (double[] t3row : t3table) {
                    if (t1row[0] < t2row[0] + t3row[0]) {
                        sum += t1row[1] * t2row[1] * t3row[1];
                    }
                }
            }
            result.put(t1row[0], sum);
        }
        return result;
    }

    private PriorityQueue<ResultItem> order(LinkedHashMap<Double, Double> grouped) {
        PriorityQueue<ResultItem> heap = new PriorityQueue<>(grouped.size(), (o1, o2) -> ComparisonChain.start()
                .compare(o2.row[1], o1.row[1])
                .compare(o1.order, o2.order)
                .result());

        int order = 0;
        for (Map.Entry<Double, Double> entry : grouped.entrySet()) {
            heap.add(new ResultItem(new double[]{entry.getKey(), entry.getValue()}, order++));
        }
        return heap;
    }

    private List<ResultItem> limit(PriorityQueue<ResultItem> ordered) {
        int resultSize = Math.min(ordered.size(), LIMIT);
        List<ResultItem> result = new ArrayList<>(resultSize);
        for (int i = 0; i < resultSize; i++) {
            ResultItem item = ordered.poll();
            result.add(item);
        }
        return result;
    }

    private void writeResultTable(Path output, List<ResultItem> result) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(output)) {
            writer.write(result.size() + "\n");
            for (ResultItem item : result) {
                writer.write(item.row[0] + " " + item.row[1] + "\n");
            }
        }
    }

    private List<double[]> readTable(Path file) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(file)) {
            int size = Integer.parseInt(reader.readLine());
            List<double[]> result = new ArrayList<>(size);
            for (int i = 1; i <= size; i++) {
                String[] split = reader.readLine().split(" ");
                double[] row = new double[]{Double.parseDouble(split[0]), Double.parseDouble(split[1])};
                result.add(row);
            }
            return result;
        }
    }

    class ResultItem {
        double[] row;
        int order;

        public ResultItem(double[] row, int order) {
            this.row = row;
            this.order = order;
        }
    }

}
