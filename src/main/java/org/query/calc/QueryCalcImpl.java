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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class QueryCalcImpl implements QueryCalc {
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

        List<double[]> l1 = readTable(t1, true), l2 = readTable(t2, false), l3 = readTable(t3, false);

        List<Map.Entry<Key, Double>> resultSet = l1.stream() // from t1
                // join t2
                .flatMap(r1 -> l2.stream().map(r2 -> {
                    double[] res = new double[5];
                    System.arraycopy(r1, 0, res, 0, 3);
                    System.arraycopy(r2, 0, res, 3, 2);
                    return res;
                    // join t3
                })).flatMap(r12 -> l3.stream().map(r3 -> {
                    double[] res = new double[7];
                    System.arraycopy(r12, 0, res, 0, 5);
                    System.arraycopy(r3, 0, res, 5, 2);
                    return res;
                    // WHERE a < b + c
                })).filter(r -> r[1] < r[3] + r[5])
                .collect(Collectors.groupingBy(r -> new Key(r[1], r[0]), // GROUP BY a
                        Collectors.summingDouble(r -> r[2] * r[4] * r[6]))) // SUM(x * y * z)
                .entrySet().stream()
                .sorted((a,b) -> {
                    // ORDER BY s DESC
                    int cmp = b.getValue().compareTo(a.getValue());
                    // make sorting stable
                    if (cmp == 0) {
                        return Double.compare(a.getKey().rowNumber, b.getKey().rowNumber);
                    }
                    return cmp;
                }).limit(10).collect(Collectors.toList());

        writeResultToTable(output, resultSet);
    }

    private static class Key {
        private final double value;
        private final double rowNumber;

        public Key(double value, double rowNumber) {
            this.value = value;
            this.rowNumber = rowNumber;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Key key = (Key) o;
            return Double.compare(key.value, value) == 0;
        }

        @Override
        public int hashCode() {
            return Objects.hash(value);
        }
    }

    private void writeResultToTable(Path pathToTable, List<Map.Entry<Key, Double>> resultSet) throws IOException {
        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(pathToTable.toFile())))) {
            bw.write(Long.toString(resultSet.size()));
            if (resultSet.isEmpty()) return;
            bw.newLine();
            for (int i = 0; i < resultSet.size(); i++) {
                Map.Entry<Key, Double> entry = resultSet.get(i);
                bw.write(Double.toString(entry.getKey().value));
                bw.write(" ");
                bw.write(entry.getValue().toString());
                if (i != resultSet.size() - 1)
                    bw.newLine();
            }
        }
    }

    private List<double[]> readTable(Path pathToTable, boolean returnRowNumber) throws IOException {
        List<double[]> content;
        try (BufferedReader br
                     = new BufferedReader(new InputStreamReader(new FileInputStream(pathToTable.toFile())))) {
            int count = Integer.parseInt(br.readLine());
            content = new ArrayList<>(count);
            int rowNumber = 1;
            while (rowNumber <= count) {
                String[] parts = br.readLine().split(" ");
                if (returnRowNumber)
                    content.add(new double[]{rowNumber, Double.parseDouble(parts[0]), Double.parseDouble(parts[1])});
                else
                    content.add(new double[]{Double.parseDouble(parts[0]), Double.parseDouble(parts[1])});
                rowNumber++;
            }
        }
        return content;
    }

}