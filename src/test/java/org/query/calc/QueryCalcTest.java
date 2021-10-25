package org.query.calc;

import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;

import static org.query.calc.ResultComparator.assertFilesEqual;

public class QueryCalcTest {

    public void doTest(String testName, QueryCalc queryCalc, String caseName) throws IOException, URISyntaxException {
        TemporaryFolder temporaryFolder = new TemporaryFolder();
        ClassLoader classLoader = getClass().getClassLoader();

        temporaryFolder.create();
        File actualResultFile = temporaryFolder.newFile("actual-result");
        Path actualResult = actualResultFile.toPath();
        try {
            Path t1 = Path.of(classLoader.getResource(caseName + "/t1").toURI());
            Path t2 = Path.of(classLoader.getResource(caseName + "/t2").toURI());
            Path t3 = Path.of(classLoader.getResource(caseName + "/t3").toURI());
            Path expectedResult = Path.of(classLoader.getResource(caseName + "/expected-result").toURI());

            queryCalc.select(t1, t2, t3, actualResult);

            assertFilesEqual(() -> testName, expectedResult, actualResult);

        } finally {
            actualResultFile.delete();
            temporaryFolder.delete();
        }
    }

    @Test
    public void testCase0() throws IOException, URISyntaxException {
        doTest("case-0", new QueryCalcImpl(), "case-0");
    }

    @Test
    public void testCase1() throws IOException, URISyntaxException {
        doTest("case-1", new QueryCalcImpl(), "case-1");
    }

    @Test
    public void testCase2() throws IOException, URISyntaxException {
        doTest("case-2", new QueryCalcImpl(), "case-2");
    }

    @Test
    public void testCase3() throws IOException, URISyntaxException {
        doTest("case-3", new QueryCalcImpl(), "case-3");
    }
}
