package org.query.calc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;

public class QueryCalcImpl implements QueryCalc {

    final static int NUMBER_OF_COLUMNS = 2;
    final static int LIMIT = 10;


    @Override
    public void select(Path t1, Path t2, Path t3, Path output) throws IOException {

        if (t1 == null || t2 == null || t3 == null || output == null) {
            System.out.println("Null parameters are mot expected.");
        }

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
        // SELECT a, SUM(x * y * z) AS s FROM
        // t1 LEFT JOIN (SELECT * FROM t2 JOIN t3) AS t
        // ON a < b + c
        // GROUP BY a
        // STABLE ORDER BY s DESC
        // LIMIT 10;
        //
        // Note: STABLE is not a standard SQL command. It means that you should preserve the original order.
        // In this context it means, that in case of tie on s-value you should prefer value of a, with a lower row number.
        // In case multiple occurrences, you may assume that group has a row number of the first occurrence.


        selectWithFakeJoin(t1, t2, t3, output);

        //5000 5000 5000 - time 2.0127s memory 67Mb
        //10000 5000 5000 - time 1.9902s memory 61Mb
        //5000 10000 5000 - time 4.0253s memory 74Mb
        //5000 5000 10000 - time 4.2568s memory 67Mb
        //10000 10000 10000 - time 8.5983s memory 93Mb
        //20000 20000 20000 - time 34.1624s memory 300Mb
        //40000 40000 40000 - time 170.9143s memory 230Mb

    }


    /***
     * Implements logic with three provided tables in files. Produces result in output file.
     * SELECT a, SUM(x * y * z) AS s FROM
     * t1 LEFT JOIN (SELECT * FROM t2 JOIN t3) AS t
     * ON a < b + c
     * GROUP BY a
     * STABLE ORDER BY s DESC
     * LIMIT 10;
     * @param t1 - Table with two columns (a, x) as 2d array of double.
     * @param t2 - Table with two columns (b, y) as 2d array of double.
     * @param t3 - Table with two columns (c, z) as 2d array of double.
     * @param output - file for writing table with two columns (a, s) as 2d array of double.
     * @throws IOException
     */
    private static void selectWithFakeJoin(Path t1, Path t2, Path t3, Path output) throws IOException {

        //Read table data from file.
        double[][] table1 = readTable(t1);
        if (table1 == null) {
            System.out.println("No data in the table1");
            return;
        }
        double[][] table2 = readTable(t2);
        if (table2 == null) {
            System.out.println("No data in the table2");
            return;
        }
        double[][] table3 = readTable(t3);
        if (table3 == null) {
            System.out.println("No data in the table3");
            return;
        }

        //Group elements and keep order.
        //For a1=a2, the result calculates as s = x1y1z1 + x1y1z2 + ... + x1ynzm + x2y1z1 + x2y1z2 + ... + x2ynzm.
        //We can move x1+x2  take out of brackets.
        //So we can group on the first step.
        table1 = group(table1);
        //Now in the table1 we have rows with unique values in 'a' column.
        //So we will calculate 's' for every column only once.

        //Calculate result table with fake cross join table.
        //We will not calculate and store in memory (SELECT * FROM t2 JOIN t3)
        //Every row of this table will be calculated on demand and used in calculation without storing it.
        double[][] result = calc(table1, table2, table3);

        //Write result to file.
        writeTable(output, result);
    }

    /***
     * Group table rows by the first columns.
     * Row grouping is an operation when two rows {{a1, x1}, {a2, x2}} merged into a single row {{a1, x1+x2}}.
     * a = a1 - because a1 equal a2
     * x = x1+x2 - according to required logic in the task SUM(x * y * z)
     * @param table - Table with two columns as 2d array of double.
     * @return new table with two columns as 2d array of double.
     */
    private static double[][] group(double[][] table) {
        double[][] result = new double[table.length][NUMBER_OF_COLUMNS];
        int pointer = 0; //use as pointer to available row in the result table
        for (int i=0; i < table.length; i++) {
            boolean hit = false;
            for (int j = pointer-1; j>=0; j--) {
                //group to rows
                if (Double.compare(result[j][0], table[i][0]) == 0) {
                    result[j][1] += table[i][1];
                    hit = true;
                    break;
                }
            }
            if (hit) continue;
            //we are here if the second row for grouping was not found
            result[pointer][0]=table[i][0];
            result[pointer][1]=table[i][1];
            pointer++;
        }
        if (pointer == table.length) return result; //if no grouping the result size is equal to the input table, so not partial copy required.
        //if grouping was performed the result size is less than the input table, so partial copy required.
        return Arrays.copyOf(result, pointer);
    }


    /***
     * Calculate SUM(x * y * z) for every row from the table1.
     * @param table1 - Table with two columns (a, x) as 2d array of double.
     * @param table2 - Table with two columns (b, y) as 2d array of double.
     * @param table3 - Table with two columns (c, z) as 2d array of double.
     * @return Table with two columns (a, s) as 2d array of double.
     */
    private static double[][] calc(double[][] table1, double[][] table2, double[][] table3) {

        double [][] result = new double[LIMIT][NUMBER_OF_COLUMNS];
        int pointer = 0; //use as pointer to available row in the result table

        for (int i = 0; i < table1.length; i++) {

            if (table1[i][1] == 0) continue; //if x = 0 then sum = 0;
            double sum = 0;

            //calculate values for join of table2 and table3
            for (int j1 = 0; j1 < table2.length; j1++) {
                double b = table2[j1][0];
                double y = table2[j1][1];
                for (int j2 = 0; j2 < table3.length; j2++) {

                    double bc = b + table3[j2][0];
                    double z = table3[j2][1];

                    if (table1[i][0] < bc) {
                        sum += y*z; //s = x1y1z1 + x1y1z2 + ... + x1ynzm = x1*(y1z1 + y1z2 + ... + ynzm)
                    }

                }
            }
            //now sum = (y1z1 + y1z2 + ... + ynzm)
            //we can multiply with x = table1[i][1]

            table1[i][1] *= sum;

            // we should insert calculated row in the result table
            if (pointer < LIMIT) {
                //add to the tail and resort if limit not reached
                insert(result, table1[i], pointer++);
            } else if (table1[i][1] > result[LIMIT-1][1]) {
                //replace the last row if value in a new row is higher
                insert(result, table1[i], LIMIT-1);
            }
        }

        //cut table if limit is not reached
        if (table1.length < LIMIT) {
            result = Arrays.copyOf(result, table1.length);
        }

        return result;
    }


    /***
     * Insert row to the end of table and resort the table on the second column.
     * @param table - with two columns (a, s) as 2d array of double.
     * @param row - row for insertion
     * @param to - position in the table
     */
    private static void insert(double[][] table, double[] row, int to) {
        table[to] = row;
        for (int j = to; j > 0; j--) {
            if (!(table[j][1] > table[j - 1][1])) break;
            double[] t = table[j];
            table[j] = table[j - 1];
            table[j - 1] = t;
        }
    }


    /***
     * Read table from a file.
     * @param path - Path to file.
     * @return - table as 2d array of double.
     */
    private static double[][] readTable(Path path) throws IOException {

        try (BufferedReader reader = new BufferedReader(new FileReader(path.toFile()))) {
            String line = reader.readLine();
            if (line == null) {
                System.out.println("File does not have table data.");
                return null;

            }
            int size = Integer.parseInt(line);

            double[][] table = new double[size][NUMBER_OF_COLUMNS];

            for (int i = 0; (line = reader.readLine()) != null && i < size; i++) {
                String[] items = line.split(" ");
                for (int j = 0; j < NUMBER_OF_COLUMNS; j++) {
                    table[i][j] = Double.parseDouble(items[j]);
                }
            }

            return table;

        }
    }


    /***
     * Write table to a file.
     * @param path - path to file
     * @param table - table as 2d array of double.
     */
    private static void writeTable(Path path, double[][] table) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(path.toFile()))) {
            writer.append(String.valueOf(table.length));

            for (int i = 0; i < table.length; i++) {
                writer.newLine();
                writer.append(String.valueOf(table[i][0])).append(" ").append(String.valueOf(table[i][1]));
            }

        }
    }
}
