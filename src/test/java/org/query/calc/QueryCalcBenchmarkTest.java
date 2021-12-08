package org.query.calc;

import org.junit.jupiter.api.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Random;

public class QueryCalcBenchmarkTest {
    Random rd = new Random();

    private void emulateData(File f, int count) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(f.toPath())) {
            writer.write(String.format("%d\n", count));
            for (int i=0; i < count; i++) {
                writer.write(String.format("%f %f\n", rd.nextDouble(), rd.nextDouble()));
            }
        }
    }

    @Test
    public void doBenchmark() throws IOException {
        TemporaryFolder temporaryFolder = new TemporaryFolder();
        temporaryFolder.create();
        File[] files = new File[3];
        for (int i=0; i < files.length; i++) {
            files[i] = temporaryFolder.newFile(String.format("benchmark-t%d", i));
            emulateData(files[i], 5000);
        }
        File outFile = temporaryFolder.newFile("benchmark-out");

        QueryCalc underTest = new QueryCalcImpl();

        long startTime = System.currentTimeMillis();
        underTest.select(files[0].toPath(), files[1].toPath(), files[2].toPath(), outFile.toPath());
        System.out.println(String.format("The join is done in %d milliseconds",
                System.currentTimeMillis() - startTime));
    }
}
