package org.query.calc;

import it.unimi.dsi.fastutil.Pair;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleArrayPriorityQueue;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import it.unimi.dsi.fastutil.doubles.DoubleRBTreeSet;

import java.io.IOException;
import java.nio.file.Path;

import static it.unimi.dsi.fastutil.doubles.DoubleArrays.binarySearch;
import static java.lang.Math.max;
import static org.query.calc.FileUtil.readFile;
import static org.query.calc.FileUtil.writeFile;
import static org.query.calc.Sort.topNAgg;

public class QueryCalcImpl implements QueryCalc {
    public static final int LIMIT = 10;

    @Override
    public void select(Path t1, Path t2, Path t3, Path output) throws IOException {
        double[] a, x, b, y, c, z;
        double[] aInOriginalOrder; // We can re-read file t1 and don't keep this in memory whenever it's required.

        // 1. Read and sort a,x.
        {
            double[][] pair = readFile(t1);
            a = pair[0];
            x = pair[1];
            aInOriginalOrder = a.clone(); // see comments above
            Sort.quickSort(a, x);

            Pair<double[], double[]> p = mergeSameValues(a, x); // Merge same values in "a" in case they are present
            a = p.first();
            x = p.second();
        }

        // 2. Read and sort b, y.
        {
            double[][] pair = readFile(t2);
            b = pair[0];
            y = pair[1];
            Sort.quickSort(b, y);
        }

        // 3. Read and sort c, z.
        {
            double[][] pair = readFile(t3);
            c = pair[0];
            z = pair[1];
            Sort.quickSort(c, z);
        }

        // 4. When all values in x,y,z are non-negative it's possible to significantly reduce the size of a
        if (a.length > LIMIT && x[0] >= 0 && y[0] >= 0 && z[0] >= 0) {
            Pair<double[], double[]> p = reduceDueLimit(a, x);
            a = p.first();
            x = p.second();
        }

        // 5. Select top sums
        Pair<DoubleRBTreeSet[], double[]> res = select(a, x, b, y, c, z);
        DoubleRBTreeSet[] topAs = res.first();
        double[] topS = res.second();

        // 6. Iterate `a` in the original order to prepare stable results
        DoubleRBTreeSet all = new DoubleRBTreeSet();
        for (DoubleRBTreeSet topA : topAs) {
            all.addAll(topA);
        }
        final int sizeOfFirst = topAs[0].size() - (max(all.size() - LIMIT, 0));
        final Pair<DoubleList, Double>[] stableResults = new Pair[topS.length];
        for (int i = 0; i < topS.length; i++) {
            stableResults[i] = Pair.of(new DoubleArrayList(i == 0 ? sizeOfFirst : topAs[i].size()), topS[i]);
        }
        for (double ai : aInOriginalOrder) {   // See comments for aInOriginalOrder about memory usage optimization
            if (all.contains(ai)) {
                for (int i = 0; i <= topAs.length; i++) {
                    if (topAs[i].contains(ai)) {
                        if (i > 0 || stableResults[0].first().size() < sizeOfFirst) {
                            stableResults[i].first().add(ai);
                            all.remove(ai);
                        }
                        break;
                    }
                }
            }
        }

        // 7. Write results
        writeFile(output, stableResults);
    }

    private static Pair<double[], double[]> mergeSameValues(final double[] a, final double[] x) {
        int numberOfSameValues = 0;
        double prev = a[0];
        for (int i = 1; i < a.length; i++) {
            if (a[i] == prev) numberOfSameValues++;
            prev = a[i];
        }
        if (numberOfSameValues == 0) {
            return Pair.of(a, x);
        }
        double[] a1 = new double[a.length - numberOfSameValues];
        double[] x1 = new double[x.length - numberOfSameValues];
        prev = a[0];
        a1[0] = a[0];
        x1[0] = x[0];
        for (int i = 1, j = 0; i < a.length; i++) {
            if (a[i] == prev) {
                x1[j] += x[i];
            } else {
                j++;
                a1[j] = prev = a[i];
                x1[j] = x[i];
            }
        }
        return Pair.of(a1, x1);
    }

    private static Pair<double[], double[]> reduceDueLimit(final double[] a, final double[] x) {
        DoubleArrayPriorityQueue heap = new DoubleArrayPriorityQueue(LIMIT);
        for (int i = 0; i < LIMIT; i++) {
            heap.enqueue(x[i]);
        }
        int newSize = LIMIT;
        for (int i = LIMIT; i < x.length; i++) {
            if (x[i] > heap.firstDouble()) {
                newSize++;
                heap.dequeueDouble();
                heap.enqueue(x[i]);
            }
        }
        if (newSize == a.length) {
            return Pair.of(a, x);
        }
        double[] a1 = new double[newSize];
        double[] x1 = new double[newSize];
        heap.clear();
        for (int i = 0; i < LIMIT; i++) {
            heap.enqueue(x[i]);
            a1[i] = a[i];
            x1[i] = x[i];
        }
        for (int i = LIMIT, j = LIMIT; i < x.length; i++) {
            if (x[i] > heap.firstDouble()) {
                heap.dequeueDouble();
                heap.enqueue(x[i]);
                a1[j] = a[i];
                x1[j] = x[i];
                j++;
            }
        }
        return Pair.of(a1, x1);
    }

    private Pair<DoubleRBTreeSet[], double[]> select(final double[] a, final double[] x,
                                                     final double[] b, final double[] y,
                                                     final double[] c, final double[] z) {
        // 1. Calculate sums mapped on a values
        double[] s = new double[a.length];
        {
            int[] from = new int[c.length];
            for (int j = 0; j < b.length; j++) {   // i iterates a, j iterates b, k iterates c
                int i = from[0], k;
                // Distribute c values with fixed b[j]
                // Skip small c values
                //
                for (k = 0; k < c.length && a[i = max(i, from[k])] >= b[j] + c[k]; k++) ;
                // Distribute other c values
                for (; k < c.length; k++) {
                    i = max(i, from[k]);
                    while (i + 1 < a.length && a[i + 1] < b[j] + c[k]) i++;
                    from[k] = i;
                    s[i] += y[j] * z[k];
                }
            }
        }
        // 2. Aggregate sums, distributing an accumulated sum back and multiply it on x
        {
            double acc = s[a.length - 1];
            s[a.length - 1] *= x[a.length - 1];
            for (int i = s.length - 2; i >= 0; i--) {
                acc += s[i];
                s[i] = acc * x[i];
            }
        }
        // 3. Find top N sums and its counts (in the case of same s values)
        Pair<double[], int[]> top = topNAgg(s, LIMIT);
        double[] topS = top.first();
        // 4. Find sets of "a" values for appropriate values of top N sums
        DoubleRBTreeSet[] topAs = new DoubleRBTreeSet[topS.length];
        for (int k = 0; k < topAs.length; k++)
            topAs[k] = new DoubleRBTreeSet();
        double topMin = topS[0];
        for (int i = 0; i < a.length; i++) {
            if (s[i] >= topMin) {
                int j = binarySearch(topS, s[i]);
                topAs[j].add(a[i]);
            }
        }
        return Pair.of(topAs, topS);
    }

}
