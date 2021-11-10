package org.query.calc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

public class QueryCalcImpl implements QueryCalc {

    private static final int COLUMNS_NUMBER = 2;
    private static final String DELIMITER = " ";
    private static final int LIMIT = 10;
    private static final double ZERO_VALUE = 0;

    @Override
    public void select(Path t1, Path t2, Path t3, Path output) throws IOException {
        double[][] tableT2 = getTableT2(t2);
        double[][] table3AndJoinT2 = getTable3AndJoinT2(t3, tableT2);
        double[][] tablesJoinedAndGrouped = getTable1JoinAndGroup(t1, table3AndJoinT2);
        double[][] sortedResult = sortWithLimit(tablesJoinedAndGrouped);
        writeResult(output, sortedResult);
    }

    private void writeResult(Path output, double[][] order) throws IOException {
        try (FileWriter fileWriter = new FileWriter(output.toFile());
             BufferedWriter writer = new BufferedWriter(fileWriter)) {
            writer.write(String.format("%d", order.length));
            writer.newLine();
            for (int i = 0; i < order.length; i++) {
                writer.write(String.format("%f %f", order[i][0], order[i][1]));
                writer.newLine();
            }
        }
    }

    private double[][] sortWithLimit(double[][] source) {
        double[][] result = new double[Math.min(LIMIT, source.length)][COLUMNS_NUMBER];
        boolean isNotContainsEmpty = calcResultWithoutEmpty(source, result);
        if (!isNotContainsEmpty) {
            populateEmptyElements(source, result);
        }
        return result;
    }

    private boolean calcResultWithoutEmpty(double[][] res, double[][] order) {
        boolean isMax = false;
        for (int i = 0; i < order.length; i++) {
            isMax = false;
            double max = Double.MIN_VALUE;
            for (int j = 0; j < res.length; j++) {
                //could be simplify 'if (res[j][1] > max)' if use NEGATIVE_INFINITY
                if (!Double.isNaN(res[j][1]) && !Double.isInfinite(res[j][1]) && res[j][1] > max) {
                    order[i][0] = j;
                    order[i][1] = res[j][1];
                    max = res[j][1];
                    isMax = true;
                }
            }
            if (!isMax) {
                break;
            }
            res[(int) order[i][0]][1] = Double.POSITIVE_INFINITY;
            order[i][0] = res[(int) order[i][0]][0];
        }
        return isMax;
    }

    private void populateEmptyElements(double[][] source, double[][] result) {
        for (int i = result.length - 1; i > 0; i--) {
            if (result[i][0] != ZERO_VALUE || result[i][1] != ZERO_VALUE) {
                break;
            }
            for (int j = 0; j < source.length; j++) {
                if (Double.isNaN(source[j][1])) {
                    result[i][0] = source[j][0];
                    source[j][1] = 0;
                    break;
                }
            }
        }
    }

    private double[][] getTable1JoinAndGroup(Path t1, double[][] arr2JoinArr3) throws IOException {
        final Map<Double, Double> map;
        try (FileReader fileInputStream = new FileReader(t1.toFile());
             BufferedReader reader = new BufferedReader(fileInputStream)) {
            map = readTable1JoinAndGroup(reader, arr2JoinArr3);
        }
        double[][] res = new double[map.size()][COLUMNS_NUMBER];
        int i = 0;
        for (var entry : map.entrySet()) {
            res[i][0] = entry.getKey();
            res[i++][1] = entry.getValue();
        }
        return res;
    }

    private Map<Double, Double> readTable1JoinAndGroup(BufferedReader reader, double[][] table2And3) throws IOException {
        final Map<Double, Double> map;
        String line = reader.readLine();
        int arr1Length = Integer.parseInt(line);
        map = new LinkedHashMap<>(arr1Length, 1);
        for (int i = 0; i < arr1Length; i++) {
            line = reader.readLine();
            final int index = line.indexOf(DELIMITER);
            double a = Double.parseDouble(line.substring(0, index));
            double x = Double.parseDouble(line.substring(index));
            map.putIfAbsent(a, Double.NaN);
            for (int j = 0; j < table2And3.length; j++) {
                if (a < table2And3[j][0]) {
                    final double newVal = x * table2And3[j][1];
                    final Double oldVal = map.get(a);
                    if (Double.isNaN(oldVal)) {
                        map.put(a, newVal);
                    } else {
                        map.put(a, oldVal + newVal);
                    }
                }
            }
        }
        return map;
    }

    private double[][] getTable3AndJoinT2(Path t3, double[][] arr2) throws IOException {
        try (FileReader fileInputStream = new FileReader(t3.toFile());
             BufferedReader reader = new BufferedReader(fileInputStream)) {
            String line = reader.readLine();
            int rows = Integer.parseInt(line);
            final int arr2Length = arr2.length;
            double[][] arr3 = new double[rows * arr2Length][COLUMNS_NUMBER];
            for (int i = 0; i < rows; i++) {
                line = reader.readLine();
                final int index = line.indexOf(DELIMITER);
                double c = Double.parseDouble(line.substring(0, index));
                double z = Double.parseDouble(line.substring(index));
                for (int j = 0; j < arr2Length; j++) {
                    arr3[i * arr2Length + j][0] = arr2[j][0] + c;
                    arr3[i * arr2Length + j][1] = arr2[j][1] * z;
                }
            }
            return arr3;
        }
    }

    private double[][] getTableT2(Path t2) throws IOException {
        try (FileReader fileInputStream = new FileReader(t2.toFile());
             BufferedReader reader = new BufferedReader(fileInputStream)) {
            String line = reader.readLine();
            int rows = Integer.parseInt(line);
            double[][] arr2 = new double[rows][COLUMNS_NUMBER];
            for (int i = 0; i < rows; i++) {
                line = reader.readLine();
                final int index = line.indexOf(DELIMITER);
                arr2[i][0] = Double.parseDouble(line.substring(0, index));
                arr2[i][1] = Double.parseDouble(line.substring(index));
            }
            return arr2;
        }
    }
}
