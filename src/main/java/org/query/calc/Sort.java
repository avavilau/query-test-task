package org.query.calc;

import it.unimi.dsi.fastutil.Pair;
import it.unimi.dsi.fastutil.doubles.DoubleArrayPriorityQueue;
import it.unimi.dsi.fastutil.doubles.DoubleArrays;
import it.unimi.dsi.fastutil.ints.IntArrays;

import static java.lang.Math.min;

public class Sort {

    public static void quickSort(double[] a, double[] b, int low, int high) {
        if (low < high) {
            double pivot = a[(low + high)/2];
            int i = low;
            int j = high;
            while (true) {
                while (a[i] < pivot) i++;
                while (a[j] > pivot) j--;
                if (i >= j) break;
                double tmp = a[i]; a[i] = a[j]; a[j] = tmp;
                tmp = b[i]; b[i] = b[j]; b[j] = tmp;
                i++; j--;
            }
            quickSort(a, b, low, j);
            quickSort(a, b,j+1, high);
        }
    }

    public static void quickSort(double[] a, double[] b) {
        quickSort(a, b, 0, a.length - 1);
    }

    public static double[] topN(final double[] v, final int n) {
        if (v == null || v.length == 0 || n <= 0) {
            return DoubleArrays.EMPTY_ARRAY;
        }
        final int limit = min(v.length, n);
        DoubleArrayPriorityQueue heap = new DoubleArrayPriorityQueue(limit);
        for (int i = 0; i < limit; i++) {
            heap.enqueue(v[i]);
        }
        for (int i = limit; i < v.length; i++) {
            if (heap.firstDouble() < v[i]) {
                heap.dequeueDouble();
                heap.enqueue(v[i]);
            }
        }
        double[] top = new double[limit];
        for (int i = 0; i < limit; i++) {
            top[i] = heap.dequeueDouble();
        }
        return top;
    }

    static Pair<double[], int[]> topNAgg(final double[] v, final int n) {
        if (v == null || v.length == 0 || n <= 0) {
            return Pair.of(DoubleArrays.EMPTY_ARRAY, IntArrays.EMPTY_ARRAY);
        }
        final double[] top = topN(v, n);
        int unique = 1;
        double prev = top[0];
        for (int i = 1; i < top.length; i++) {
            if (top[i] != prev) {
                prev = top[i];
                unique++;
            }
        }
        final double[] u = new double[unique];
        final int[] c = new int[unique];
        u[0] = prev = top[0];
        c[0] = 1;
        for (int i = 1, j = 0; i < top.length; i++) {
            if (top[i] != prev) {
                u[++j] = prev = top[i];
                c[j] = 1;
            } else {
                c[j]++;
            }
        }
        return Pair.of(u, c);
    }

}
