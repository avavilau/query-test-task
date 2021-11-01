package org.query.executor;
import java.io.*;

import java.nio.file.Path;
import java.util.concurrent.Callable;

/**
 * Step 1: Input Parser: Parallelly read the all the files t1 , t2 and t3
 * This scope of this class is to perform Input File Parsing
 */
public class FileParser implements Callable<Double[][]> {

    public FileParser(Path path){
        input = path.toFile();
    }
    private File input = null;
    /**
     * Computes a result, or throws an exception if unable to do so.
     *
     * @return computed result
     * @throws Exception if unable to compute a result
     */
    @Override
    public Double[][] call() throws Exception {
        final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(input)));
        Integer size = Integer.parseInt(bufferedReader.readLine());
        Double[][] result = new Double[size][2];
        String line;
        int i =0;
        while ((line = bufferedReader.readLine()) != null) {

            String[] input = line.split(" ");
            if(input.length == 2){
                result[i][0] = Double.parseDouble(input[0]);
                result[i][1] = Double.parseDouble(input[1]);
                i++;
            }

        }
        return result;
    }
}
