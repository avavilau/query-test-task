package org.query.calc;

import it.unimi.dsi.fastutil.BidirectionalIterator;
import it.unimi.dsi.fastutil.doubles.Double2DoubleSortedMap;
import org.query.calc.model.InputFile;
import org.query.calc.model.Record;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;

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

        // Prepare file readers
        try(BufferedReader br1 = new BufferedReader(new InputStreamReader(Files.newInputStream(t1)));
            BufferedReader br2 = new BufferedReader(new InputStreamReader(Files.newInputStream(t2)));
            BufferedReader br3 = new BufferedReader(new InputStreamReader(Files.newInputStream(t3)))) {

            InputFile t1RawData = new InputFile(Integer.parseInt(br1.readLine()), br1.lines());
            InputFile t2RawData = new InputFile(Integer.parseInt(br2.readLine()), br2.lines());
            InputFile t3RawData = new InputFile(Integer.parseInt(br3.readLine()), br3.lines());

            // Add parallelism if needed (don't use parallelism for t1 because the order is important)
            int magicNumber = 1000;
            t2RawData.setParallelProcessing(t2RawData.getSize() > magicNumber);
            t3RawData.setParallelProcessing(t3RawData.getSize() > magicNumber);

            // Join t2 and t3 to RB or AVL tree map where keys are b+c and values y*z
            Double2DoubleSortedMap joinedData;
            if (t1RawData.getSize() < t2RawData.getSize() * t3RawData.getSize()) {
                // Use RB tree because there are more insertions than searching
                if (t2RawData.getSize() < t3RawData.getSize()) { // select the smallest file between t2 and t3 to reduce memory usage
                    joinedData = DataJoiner.joinFilesToRBTree(t2RawData.getContent().map(this::parseFileLine),
                            t3RawData.getContent().map(this::parseFileLine));
                } else {
                    joinedData = DataJoiner.joinFilesToRBTree(t3RawData.getContent().map(this::parseFileLine),
                            t2RawData.getContent().map(this::parseFileLine));
                }
            } else {
                // Use AVL tree because there is more searching than insertions
                if (t2RawData.getSize() < t3RawData.getSize()) { // select the smallest file between t2 and t3 to reduce memory usage
                    joinedData = DataJoiner.joinFilesToAVLTree(t2RawData.getContent().map(this::parseFileLine),
                            t3RawData.getContent().map(this::parseFileLine));
                } else {
                    joinedData = DataJoiner.joinFilesToAVLTree(t3RawData.getContent().map(this::parseFileLine),
                            t2RawData.getContent().map(this::parseFileLine));
                }
            }

            // Calculate SUM(y*z) for b+c.
            Double2DoubleSortedMap preparedJoinedData = setSumFromBiggestToSmallest(joinedData);

            // Stream t1 records and collect 10 best records in STABLE DESC order
            var resultCalculator = new Aggregator(t1RawData.getContent().map(this::parseFileLine),
                    preparedJoinedData, 10);
            resultCalculator.calcAndSaveResult(output);
        }
    }

    private Record parseFileLine(String s) {
        String[] arr = s.split(" ");
        return new Record(Double.parseDouble(arr[0]), Double.parseDouble(arr[1]));
    }

    private Double2DoubleSortedMap setSumFromBiggestToSmallest(Double2DoubleSortedMap map) {
        BidirectionalIterator<Double> iterator = map.keySet().iterator(map.lastDoubleKey());
        double total = 0;
        while (iterator.hasPrevious()) {
            double indx = iterator.previous();
            total = total + map.get(indx);
            map.replace(indx, total);
        }
        return map;
    }
}
