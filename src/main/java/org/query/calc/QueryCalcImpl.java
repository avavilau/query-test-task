package org.query.calc;

import lombok.Data;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * // - t1 is a file contains table "t1" with two columns "a" and "x". First line is a number of rows, then each
 * //  line contains exactly one row, that contains two numbers parsable by Double.parse(): value for column a and
 * //  x respectively.See test resources for examples.
 * // - t2 is a file contains table "t2" with columns "b" and "y". Same format.
 * // - t3 is a file contains table "t3" with columns "c" and "z". Same format.
 * // - output is table stored in the same format: first line is a number of rows, then each line is one row that
 * //  contains two numbers: value for column a and s.
 * //
 * // Number of rows of all three tables lays in range [0, 1_000_000].
 * // It's guaranteed that full content of all three tables fits into RAM.
 * // It's guaranteed that full outer join of at least one pair (t1xt2 or t2xt3 or t1xt3) of tables can fit into RAM.
 * //
 * // TODO: Implement following query, put a reasonable effort into making it efficient from perspective of
 * //  computation time, memory usage and resource utilization (in that exact order). You are free to use any lib
 * //  from a maven central.
 * //
 * // SELECT a, SUM(X * y * z) AS s FROM
 * // t1 LEFT JOIN (SELECT * FROM t2 JOIN t3) AS t
 * // ON a < b + c
 * // GROUP BY a
 * // STABLE ORDER BY s DESC
 * // LIMIT 10;
 * //
 * // Note: STABLE is not a standard SQL command. It means that you should preserve the original order.
 * // In this context it means, that in case of tie on s-value you should prefer value of a, with a lower row number.
 * // In case multiple occurrences, you may assume that group has a row number of the first occurrence.
 */

public class QueryCalcImpl implements QueryCalc {
    @Override
    public void select(Path t1, Path t2, Path t3, Path output) throws IOException {
        try (
                Stream<double[]> t1_stream = Files.lines(t1).skip(1).map(this::stringToDataRow); //on current realization we don't need cont of rows
                Stream<double[]> t2_stream = Files.lines(t2).skip(1).map(this::stringToDataRow); //on current realization we don't need cont of rows
                Stream<double[]> t3_stream = Files.lines(t3).skip(1).map(this::stringToDataRow); //on current realization we don't need cont of rows
        ) {
            //reactive execution
            Stream<GroupedResult> resultStream = prepareQueryStream(t1_stream, t2_stream, t3_stream);
            // cause we have not over 10 items we can operate resultStream to list, then we have count of records = data lines
            List<String> dataLines = resultStream.map(r -> r.group + " " + r.aggregation).collect(Collectors.toList());
            Files.write(output, List.of(dataLines.size() + " "), StandardOpenOption.CREATE);
            Files.write(output, dataLines, StandardOpenOption.APPEND);
            //TODO: put lines directly from stream going over it.
            //TODO: then we need to insert calculated count before 1st file line
        }
    }

    private double[] stringToDataRow(String s) {
        double[] row = new double[2];
        int spaceIndex = s.indexOf(' ');
        row[0] = Double.parseDouble(s.substring(0, spaceIndex));
        row[1] = Double.parseDouble(s.substring(spaceIndex));
        return row;
    }

    private Stream<GroupedResult> prepareQueryStream(Stream<double[]> t1_stream, Stream<double[]> t2_stream, Stream<double[]> t3_stream) {

        double[][] t2 = t2_stream.toArray(double[][]::new); //row data of t2 table -> to RAM
        double[][] t3 = t3_stream.toArray(double[][]::new); //row data of t3 table -> to RAM

        double[][] t2xt3 = joinTables(t2, t3); // t2xt3 -> to RAM

        //go throw t1 with stream pinter for line in file // not use all RAM to load file into RAM - only single file line -> RAM
        return t1_stream
                .map(t1_row -> {
                    //pointed of next row of t1
                    double a = t1_row[0];
                    // pick out memory of new double[2] just for one pre grouping result
                    // a =>  row_group[0] => group by
                    // s =>  row_group[1] => agregation SUM(X * y * z)
                    //for each row from t2_t3_joined
                    //skip rows where we are not applied for condition a < b + c
                    //counting total sum of x * y * z while going all rows = onAir -> memory optimization
                    double group_aggregation = IntStream.range(0, t2xt3.length)
                            .filter(index -> {
                                // ON a < b + c
                                double b = t2xt3[index][0];
                                double c = t2xt3[index][2];
                                return a < b + c;
                            }).mapToDouble(index -> {
                                // x * y * z
                                double x = t1_row[1];
                                double y = t2xt3[index][1];
                                double z = t2xt3[index][3];
                                return x * y * z;
                            }).sum(); //sum (x * y * z)
                    return new GroupedResult(a, group_aggregation);
                })
                //for same a value just sum again calculated aggregations
                .collect(Collectors.toMap(
                        rg -> rg.getGroup(), // key = a
                        rg -> rg.getAggregation(),//  value = SUM(X * y * z)
                        (v1, v2) -> v1 + v2))
                .entrySet().stream()// in case same groups just sum aggregations)
                .map(entry -> new GroupedResult(entry.getKey(), entry.getValue()))
                .sorted(Comparator.comparing(GroupedResult::getAggregation).reversed()) //ORDER BY s DESC
                .limit(10);  // LIMIT 10;
    }

    private double[][] joinTables(double[][] m1, double[][] m2) {
        double[][] result = new double[m1.length * m2.length][4];
        int rowIndex = 0;
        for (int i = 0; i < m1.length; i++) {//3
            for (int j = 0; j < m2.length; j++) { //2
                result[rowIndex][0] = m1[i][0];
                result[rowIndex][1] = m1[i][1];
                result[rowIndex][2] = m2[j][0];
                result[rowIndex][3] = m2[j][1];
                rowIndex++;
            }
        }
        return result;
    }

    @Data
    private class GroupedResult {
        private final double group;
        private final double aggregation;
    }
}
