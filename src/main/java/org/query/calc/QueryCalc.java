package org.query.calc;

import java.io.IOException;
import java.nio.file.Path;

public interface QueryCalc {
    void select(Path t1, Path t2, Path t3, Path output) throws IOException;
}
