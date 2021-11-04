package org.query.calc;

import it.unimi.dsi.fastutil.Pair;
import it.unimi.dsi.fastutil.doubles.DoubleList;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FileUtil {
    private static final Pattern LINE_PATTERN_EMPTY_LINE = Pattern.compile("^\\s*(?:#.*)?$");
    private static final Pattern LINE_PATTERN_PAIR_OF_DOUBLES =
            Pattern.compile("\\s*([-+]?[0-9]*\\.?[0-9]+(?:[eE][-+]?[0-9]+)?)\\s+" +
                    "([-+]?[0-9]*\\.?[0-9]+(?:[eE][-+]?[0-9]+)?)\\s*");

    public static double[][] readFile(Path filepath) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(filepath)) {
            int lineNum = 0;
            String line;
            while ((line = reader.readLine()) != null) {
                lineNum++;
                if (!LINE_PATTERN_EMPTY_LINE.matcher(line).matches()) break;
            }
            if (line == null) {
                throw new IllegalArgumentException("File " + filepath.toAbsolutePath() +
                        " doesn't have enough data.");
            }
            int n = Integer.parseInt(line);
            double[] p = new double[n];
            double[] q = new double[n];
            int i = 0;
            while (i < n && (line = reader.readLine()) != null) {
                lineNum++;
                Matcher m = LINE_PATTERN_PAIR_OF_DOUBLES.matcher(line);
                if (m.matches()) {
                    p[i] = Double.parseDouble(m.group(1));
                    q[i] = Double.parseDouble(m.group(2));
                    i++;
                } else if (!LINE_PATTERN_EMPTY_LINE.matcher(line).matches()) {
                    throw new IllegalArgumentException("File" + filepath.toAbsolutePath() +
                            ", Line:" + lineNum + " has unexpected format.");
                }
            }
            return new double[][]{p, q};
        } catch (IOException ex) {
            throw new IOException("Failed to read input file " + filepath.toAbsolutePath());
        }
    }

    public static void writeFile(Path filepath, Pair<DoubleList, Double>[] result) throws IOException {
        int size = 0;
        for (Pair<DoubleList, Double> pair : result) {
            size += pair.first().size();
        }
        try (PrintWriter out = new PrintWriter(Files.newBufferedWriter(filepath))) {
            out.println(size);
            for (int i = result.length - 1; i >= 0; i--) {
                for (double a : result[i].first()) {
                    out.print(a);
                    out.print(' ');
                    out.println(result[i].second());
                }
            }
        } catch (IOException ex) {
            throw new IOException("Failed to write results to file " + filepath.toAbsolutePath(), ex);
        }
    }

}
