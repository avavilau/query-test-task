package org.query.calc;

import it.unimi.dsi.fastutil.doubles.Double2DoubleAVLTreeMap;
import it.unimi.dsi.fastutil.doubles.Double2DoubleRBTreeMap;
import it.unimi.dsi.fastutil.doubles.Double2DoubleSortedMap;
import org.query.calc.model.Record;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DataJoiner {
    private DataJoiner() {
    }

    public static Double2DoubleSortedMap joinFilesToRBTree(Stream<Record> file1, Stream<Record> file2) {
        List<Record> file1Data = file1.collect(Collectors.toList());
        return file2
                .flatMap(r -> file1Data.stream()
                        .map(d -> new Record(r.getCol1() + d.getCol1(), r.getCol2() * d.getCol2())))
                .collect(() -> new Double2DoubleRBTreeMap(),
                        (m, r) -> m.addTo(r.getCol1(), r.getCol2()),
                        (m1, m2) -> m1.putAll(m2));
    }
    public static Double2DoubleSortedMap joinFilesToAVLTree(Stream<Record> file1, Stream<Record> file2) {
        List<Record> file1Data = file1.collect(Collectors.toList());
        return file2
                .flatMap(r -> file1Data.stream()
                        .map(d -> new Record(r.getCol1() + d.getCol1(), r.getCol2() * d.getCol2())))
                .collect(() -> new Double2DoubleAVLTreeMap(),
                        (m, r) -> m.addTo(r.getCol1(), r.getCol2()),
                        (m1, m2) -> m1.putAll(m2));
    }
}
