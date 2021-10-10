package org.query.calc;

import it.unimi.dsi.fastutil.doubles.Double2DoubleRBTreeMap;
import org.query.calc.model.Record;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedList;

public class Aggregator {
    private final Double2DoubleRBTreeMap joinedData;
    private final LinkedList<Record> resultList;
    private final int size;

    public Aggregator(int size, Double2DoubleRBTreeMap joinedData) {
        this.joinedData = joinedData;
        this.size = size;
        this.resultList = new LinkedList<>();
    }

    public void calcResult(Path table, Path output) throws IOException {
        try(BufferedReader br = Files.newBufferedReader(table)) {

            String line = br.readLine();
            while ((line = br.readLine()) != null) {
                String[] arr = line.split(" ");
                var record = new Record(Double.parseDouble(arr[0]), Double.parseDouble(arr[1]));

                boolean notFound = true;
                for (Double d : joinedData.keySet()) {
                    if (record.getCol1() < d) {
                        addRecord(record, joinedData.get(d.doubleValue()));
                        notFound = false;
                        break;
                    }
                }
                if (notFound) {
                    addRecord(record, 0.0);
                }
            }
            writeResult(output);

        }
    }

    private void addRecord(Record record, double total) {
        double result = total * record.getCol2();

        int i;
        for (i = 0; i < resultList.size() && resultList.get(i).getCol2() >= result; i++) ;
        if (i < resultList.size() || i < size) {
            resultList.add(i, new Record(record.getCol1(), result));
            if (resultList.size() > size) {
                resultList.removeLast();
            }
        }
    }

    private void writeResult(Path output) {
        try(BufferedWriter bwr = Files.newBufferedWriter(output)) {
            bwr.write(String.valueOf(resultList.size()));
            for (Record r : resultList) {
                bwr.newLine();
                bwr.write(r.toString());
            }

        } catch (IOException e) {
            throw new RuntimeException("Can't read file " + output, e);
        }
    }
}
