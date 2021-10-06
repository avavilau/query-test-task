package org.query.calc;

import lombok.SneakyThrows;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.Collections.reverseOrder;
import static java.util.Map.Entry.comparingByValue;

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
        var t2Rows = readAllRows(t2);
        var t3Rows = readAllRows(t3);
        var result = groupedTable(t1, t2Rows, t3Rows)
                .entrySet()
                .stream()
                .sorted(reverseOrder(comparingByValue()))
                .limit(10)
                .collect(Collectors.toList());
        writeResult(result, output);
    }

    private Map<Double, Double> groupedTable(Path t1, Collection<double[]> t2Rows, Collection<double[]> t3Rows) {
        var groupedTable = new LinkedHashMap<Double, Double>();
        forEachRow(t1, t1Row -> {
            for (var t2Row : t2Rows) {
                for (var t3Row : t3Rows) {
                    if (t1Row[0] < t2Row[0] + t3Row[0]) {
                        groupedTable.merge(t1Row[0], t1Row[1] * t2Row[1] * t3Row[1], Double::sum);
                    } else {
                        groupedTable.putIfAbsent(t1Row[0], 0D);
                    }
                }
            }
        });
        return groupedTable;
    }

    private Collection<double[]> readAllRows(Path file) {
        var rows = new ArrayList<double[]>();
        forEachRow(file, rows::add);
        return rows;
    }

    @SneakyThrows
    private void forEachRow(Path file, Consumer<double[]> rowConsumer) {
        try (var reader = Files.newBufferedReader(file)) {
            int rowsNumber = Integer.parseInt(reader.readLine());
            int iteration = 0;
            while (iteration < rowsNumber) {
                String[] values = reader.readLine().split(" ");
                rowConsumer.accept(new double[]{Double.parseDouble(values[0]), Double.parseDouble(values[1])});
                iteration++;
            }
        }
    }

    @SneakyThrows
    private void writeResult(Collection<Map.Entry<Double, Double>> result, Path output) {
        try (var writer = Files.newBufferedWriter(output)) {
            writer.append(String.valueOf(result.size()));
            for (Map.Entry<Double, Double> resultRow : result) {
                writer.newLine();
                writer.append(String.valueOf(resultRow.getKey()));
                writer.append(" ");
                writer.append(String.valueOf(resultRow.getValue()));
            }
        }
    }
}
