package org.query.calc;

import it.unimi.dsi.fastutil.doubles.Double2IntOpenHashMap;
import it.unimi.dsi.fastutil.doubles.DoubleArrays;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.function.DoubleBinaryOperator;

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
        // TODO: Implement following query, put a reasonable effort into making it efficient from perspective of
        //  computation time, memory usage and resource utilization (in that exact order). You are free to use any lib
        //  from a maven central.
        //
        // SELECT a, SUM(X * y * z) AS s FROM 
        // t1 LEFT JOIN (SELECT * FROM t2 JOIN t3) AS t
        // ON a < b + c
        // GROUP BY a
        // STABLE ORDER BY s DESC
        // LIMIT 10;
        // 
        // Note: STABLE is not a standard SQL command. It means that you should preserve the original order. 
        // In this context it means, that in case of tie on s-value you should prefer value of a, with a lower row number.
        // In case multiple occurrences, you may assume that group has a row number of the first occurrence.
        DoublePairTable t2table = readTable(t2);
        t2table = compactTable(t2table, QueryCalcImpl::sortingCompact);

        DoublePairTable t3table = readTable(t3);
        t3table = compactTable(t3table, QueryCalcImpl::sortingCompact);

        // SELECT * FROM t2 JOIN t3 => (b + c, y * z)
        DoublePairTable t2t3crossJoin = t2table.size > t3table.size // Loop through bigger table inside
                ? t2t3crossJoin(t3table, t2table)                   // to make use of any possible caching
                : t2t3crossJoin(t2table, t3table);
        t2t3crossJoin = compactTable(t2t3crossJoin, QueryCalcImpl::sortingCompact);

        // Prefix sums
        for (int i = t2t3crossJoin.size - 2; i >= 0; --i) {
            t2t3crossJoin.values[i] += t2t3crossJoin.values[i + 1];
        }

        DoublePairTable t1table = readTable(t1);
        t1table = compactTable(t1table, QueryCalcImpl::stableCompact);

        for (int i = 0; i < t1table.size; ++i) {
            int index = Math.abs(Arrays.binarySearch(t2t3crossJoin.keys, 0, t2t3crossJoin.size, t1table.keys[i]) + 1);
            if (index == t2t3crossJoin.size) {
                // Left join with missing entries
                t1table.values[i] = 0;
            } else {
                // x * precalculated sum(y * z)
                t1table.values[i] *= t2t3crossJoin.values[index];
            }
        }

        // ORDER BY s DESC on a compacted table
        DoublePairTable topN = topN(t1table, LIMIT);

        writeTable(topN, output);
    }

    private static DoublePairTable readTable(Path path) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(path)) {
            int size = Integer.parseInt(reader.readLine());
            var keys = new double[size];
            var values = new double[size];
            for (int i = 0; i < size; ++i) {
                String[] numbers = reader.readLine().split(" ");
                keys[i] = Double.parseDouble(numbers[0]);
                values[i] = Double.parseDouble(numbers[1]);
            }

            return new DoublePairTable(size, keys, values);
        }
    }

    private static void writeTable(DoublePairTable table, Path path) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(path)) {
            writer.write(table.size + System.lineSeparator());
            for (int i = 0; i < table.size; ++i) {
                writer.write(table.keys[i] + " " + table.values[i] + System.lineSeparator());
            }
        }
    }

    private static DoublePairTable topN(DoublePairTable table, int windowSize) {
        var maxQueue = new SlidingWindowPriorityQueue(windowSize, (a, b) -> Double.compare(table.values[a], table.values[b]));
        for (int i = 0; i < table.size; ++i) {
            maxQueue.enqueue(i);
        }

        int resultSize = maxQueue.size();
        var keys = new double[resultSize];
        var values = new double[resultSize];
        for (int i = resultSize - 1; i >= 0; --i) {
            var index = maxQueue.dequeue();
            keys[i] = table.keys[index];
            values[i] = table.values[index];
        }

        return new DoublePairTable(resultSize, keys, values);
    }

    private static DoublePairTable t2t3crossJoin(DoublePairTable left, DoublePairTable right) {
        int size = Math.multiplyExact(left.size, right.size);
        var keys = new double[size];
        var values = new double[size];

        int newPosition = 0;
        for (int leftIndex = 0; leftIndex < left.size; ++leftIndex) {
            for (int i = 0; i < right.size; ++i) {
                keys[newPosition] = left.keys[leftIndex] + right.keys[i];
                values[newPosition] = left.values[leftIndex] * right.values[i];
                ++newPosition;
            }
        }

        return new DoublePairTable(size, keys, values);
    }

    /***
     * Applies compact function on the table and resizes it if necessary.
     * @param table           The table to compact
     * @param compactFunction The compact function
     * @return The compacted table
     */
    private static DoublePairTable compactTable(DoublePairTable table, CompactFunction compactFunction) {
        int size = compactFunction.apply(table.keys, table.values, table.size);

        return size == table.size
                ? table
                : size > table.size >> 1 // if less than half empty it's probably to allocate smaller arrays
                ? new DoublePairTable(size, table.keys, table.values)
                : new DoublePairTable(size, resizeArray(table.keys, size), resizeArray(table.values, size));
    }

    /***
     * Sorts and compacts key-value pairs by removing pairs with duplicated keys. Values from duplicated pairs are added
     * to corresponding compacted pairs. Arrays' length remains unchanged.
     * @param keys   Keys to compact
     * @param values Values to compact
     * @param size   Size of work area
     * @return The total number of remaining pairs
     */
    private static int sortingCompact(double[] keys, double[] values, int size) {
        if (size > 1) {
            DoubleArrays.parallelQuickSort(keys, values);
            int shift = 0;
            for (int i = 1; i < size; ++i) {
                if (Double.compare(keys[i], keys[i - 1 - shift]) == 0) {
                    transfer(values, i, i - 1 - shift++, Double::sum);
                } else if (shift > 0) {
                    movePair(keys, values, i, i - shift);
                }
            }

            return size - shift;
        }

        return size;
    }

    /***
     * Compacts key-value pairs by removing pairs with duplicated keys without changing order. Values from duplicated
     * pairs are added to corresponding compacted pairs. Arrays' length remains unchanged.
     * @param keys   Keys to compact
     * @param values Values to compact
     * @param size   Size of work area
     * @return The total number of remaining pairs
     */
    private static int stableCompact(double[] keys, double[] values, int size) {
        if (size > 1) {
            var map = new Double2IntOpenHashMap(size);
            for (int i = 0; i < size; ++i) {
                map.mergeInt(keys[i], i, (old, current) -> old);
            }

            for (int i = 0; i < size; ++i) {
                if (map.get(keys[i]) != i) {
                    transfer(values, i, map.get(keys[i]), Double::sum);
                }
            }

            int shift = 0;
            for (int i = 0; i < size; ++i) {
                if (map.get(keys[i]) != i) {
                    ++shift;
                } else if (shift > 0) {
                    movePair(keys, values, i, i - shift);
                }
            }

            return size - shift;
        }

        return size;
    }

    /**
     * Moves a key-value pair from source index to destination index. Key and value at source index are zeroed.
     * @param keys        Input keys
     * @param values      Input values
     * @param source      Source index
     * @param destination Destination index
     */
    private static void movePair(double[] keys, double[] values, int source, int destination) {
        transferPair(keys, values, source, destination, (a, b) -> b /* overwrite existing */);
    }

    /**
     * Moves a key-value pair from source index to destination index and applies merge function. Key and value at source index are zeroed.
     * @param keys          Input keys
     * @param values        Input values
     * @param source        Source index
     * @param destination   Destination index
     * @param mergeFunction Merge function.
     */
    private static void transferPair(double[] keys, double[] values, int source, int destination, DoubleBinaryOperator mergeFunction) {
        transfer(keys, source, destination, mergeFunction);
        transfer(values, source, destination, mergeFunction);
    }

    /**
     * Moves a value within the array from source index to destination index and applies merge function. Value at source index is zeroed.
     * @param array         Input array
     * @param source        Source index
     * @param destination   Destination index
     * @param mergeFunction Merge function.
     */
    private static void transfer(double[] array, int source, int destination, DoubleBinaryOperator mergeFunction) {
        array[destination] = mergeFunction.applyAsDouble(array[destination], array[source]);
        array[source] = 0;
    }

    private static double[] resizeArray(double[] array, int newSize) {
        var result = new double[newSize];
        System.arraycopy(array, 0, result, 0, newSize);

        return result;
    }

    @FunctionalInterface
    private interface CompactFunction {
        int apply(double[] keys, double[] values, int size);
    }

    private static final class DoublePairTable {
        public final int size;
        public final double[] keys;
        public final double[] values;

        public DoublePairTable(int size, double[] keys, double[] values) {
            this.size = size;
            this.keys = keys;
            this.values = values;
        }
    }
}
