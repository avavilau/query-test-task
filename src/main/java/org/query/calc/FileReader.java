package org.query.calc;

import lombok.AllArgsConstructor;
import org.query.calc.model.Record;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

@AllArgsConstructor
public class FileReader {
    private final Path path;

    List<Record> readFile() throws IOException {
        try(BufferedReader br = new BufferedReader(
                new InputStreamReader(Files.newInputStream(path)))) {
            String size = br.readLine();
            var output = new ArrayList<Record>(Integer.parseInt(size));
            br.lines()
                    .map(line -> line.split(" "))
                    .map(arr -> new Record(Double.parseDouble(arr[0]), Double.parseDouble(arr[1])))
                    .forEach(output::add);
            return output;

        }
    }
}
