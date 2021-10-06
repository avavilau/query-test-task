package org.query.calc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class QueryCalcImpl implements QueryCalc {
    /**
     * Implementation notes. Lets assume three tables have n1, n2 and n3 number of records accordingly.
     * Time complexity O(n2 * n3 + n1 * Log(n2 * n3))
     * Space complexity O(n1 + n2 * n3)
     */
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
        // Also it's guaranteed that full outer join of at least one pair of tables can fit into RAM.
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

        // could be read in parallel
        //List<List<double[]>> l23 = Stream.of(t2, t3).parallel().map(this::readTable).collect(Collectors.toList());
        List<double[]> l2 = readTable(t2), l3 = readTable(t3);
        Map<Double, double[]> l1 = readAndGroupTable(t1);

        List<double[]> merged = l2.parallelStream()
                .flatMap(r1 -> l3.parallelStream().map(r2 -> {
                    double[] res = new double[2];
                    res[0] = r1[0] + r2[0]; // b + c
                    res[1] = r1[1] * r2[1]; // y * z
                    return res;
                })).sorted(Comparator.comparingDouble(a -> a[0])).collect(Collectors.toList());

        double [] prefixSum = new double[merged.size()];
        if (!merged.isEmpty())
            prefixSum[0] = merged.get(0)[1];
        for(int i = 1; i < merged.size(); i++) {
            prefixSum[i] = prefixSum[i - 1] + merged.get(i)[1];
        }

        List<Map.Entry<Double, double[]>> resultSet = l1.entrySet().parallelStream()
                .peek(entry -> entry.getValue()[1] *= calcSum(entry.getKey(), merged, prefixSum))
                .sorted((a, b) -> {
                    // ORDER BY s DESC
                    int cmp = Double.compare(b.getValue()[1], a.getValue()[1]);
                    // make sorting stable
                    if (cmp == 0) {
                        return Double.compare(a.getValue()[0], b.getValue()[0]);
                    }
                    return cmp;
                }).limit(10).collect(Collectors.toList());

        writeResultToTable(output, resultSet);
    }

    private double calcSum(double key, List<double[]> merged, double [] prefixSum) {
        int n = prefixSum.length;
        int high = n - 1, low = 0, index = -1;
        // do binary search to find strictly greater element
        while(low <= high) {
            int mid = low + (high - low) / 2;
            if (merged.get(mid)[0] <= key) {
                low = mid + 1;
            } else {
                index = mid;
                high = mid - 1;
            }
        }
        if (index == -1) {
            return 0.0;
        } else {
            // calculate sum using prefixSum array
            return index == 0 ? prefixSum[n - 1] : prefixSum[n - 1] - prefixSum[index - 1];
        }
    }

    private void writeResultToTable(Path pathToTable, List<Map.Entry<Double, double[]>> resultSet) throws IOException {
        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(pathToTable.toFile())))) {
            bw.write(Long.toString(resultSet.size()));
            if (resultSet.isEmpty()) return;
            bw.newLine();
            for (int i = 0; i < resultSet.size(); i++) {
                Map.Entry<Double, double[]> entry = resultSet.get(i);
                bw.write(entry.getKey().toString());
                bw.write(" ");
                bw.write(Double.toString(entry.getValue()[1]));
                if (i != resultSet.size() - 1)
                    bw.newLine();
            }
        }
    }

    private Map<Double, double[]> readAndGroupTable(Path pathToTable) throws IOException {
        Map<Double, double[]> content;
        try (BufferedReader br
                     = new BufferedReader(new InputStreamReader(new FileInputStream(pathToTable.toFile())))) {
            int count = Integer.parseInt(br.readLine());
            content = new HashMap<>();
            int rowNumber = 1;
            while (rowNumber <= count) {
                String[] parts = br.readLine().split(" ");
                double a = Double.parseDouble(parts[0]);
                double x = Double.parseDouble(parts[1]);
                double [] e = content.get(a);
                if (e == null) {
                    e = new double[]{rowNumber, x};
                    content.put(a, e);
                } else
                    e[1] += x;
                content.put(a, e);
                rowNumber++;
            }
        }
        return content;
    }

    private List<double[]> readTable(Path pathToTable) throws IOException {
        List<double[]> content;
        try (BufferedReader br
                     = new BufferedReader(new InputStreamReader(new FileInputStream(pathToTable.toFile())))) {
            int count = Integer.parseInt(br.readLine());
            content = new ArrayList<>(count);
            int rowNumber = 1;
            while (rowNumber <= count) {
                String[] parts = br.readLine().split(" ");
                content.add(new double[]{Double.parseDouble(parts[0]), Double.parseDouble(parts[1])});
                rowNumber++;
            }
        }
        return content;
    }

}
