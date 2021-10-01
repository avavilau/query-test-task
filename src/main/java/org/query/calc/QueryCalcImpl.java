package org.query.calc;

import java.io.IOException;
import java.nio.file.Path;

public class QueryCalcImpl implements QueryCalc {
    @Override
    public void select(Path t1, Path t2, Path t3, Path output) throws IOException {
        // t1 is a file contains table "t1" with two columns "a" and "x". First line is a number of rows, then each line
        // contains exactly one row.  See test resources for examples.
        // t2 is a file contains table "t2" with columns "b" and "y". Same format.
        // t3 is a file contains table "t3" with columns "c" and "z". Same format.
        // Number of rows of all three tables lays in range [0, 1_000_000].
        // Still it's guaranteed that full outer join of at least one pair of tables can fit into RAM.
        //
        // TODO: Implement following query, put a reasonable effort into making it efficiently from perspective of
        //  computation time, memory usage and resource utilization (in that exact order).
        // SELECT a, SUM(x * y * z) as s
        // FROM t1
        // JOIN t2
        // JOIN t3
        // WHERE a < b + c
        // GROUP BY a
        // STABLE ORDER BY s DESC
        // Limit 10
    }
}
