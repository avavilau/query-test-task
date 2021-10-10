package org.query.calc;

import it.unimi.dsi.fastutil.doubles.AbstractDouble2DoubleSortedMap;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Comparator;

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

        // ************************************************************************************************
        // the worst computation time: t3 + t2*t3*log2(t2*t3)  +    t2*t3 +          t1*log2(t2*t3)*10
        //                         read t3 / RB tree for t2*t3 / calc total for b+c/ search 10 result rows
        // max memory usage: t3 + t2*t3 (at the stage of building RB tree)
        var table3 = (new FileReader(t3)).readFile();
        var joinedData = (new DataJoiner(table3)).readFileAndJoinData(t2);
        (new Aggregator(10, joinedData)).calcResult(t1, output);
    }
}
