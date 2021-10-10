package org.query.calc;

import it.unimi.dsi.fastutil.BidirectionalIterator;
import it.unimi.dsi.fastutil.doubles.Double2DoubleRBTreeMap;
import it.unimi.dsi.fastutil.doubles.DoubleBigArrayBigList;
import it.unimi.dsi.fastutil.objects.AbstractObjectSortedSet;
import it.unimi.dsi.fastutil.objects.ObjectBigArrayBigList;
import it.unimi.dsi.fastutil.objects.ObjectBigList;
import lombok.AllArgsConstructor;
import org.query.calc.model.Record;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

@AllArgsConstructor
public class DataJoiner {
    private final List<Record> dataForJoying;

    Double2DoubleRBTreeMap readFileAndJoinData(Path file) throws IOException {
        try(BufferedReader br = new BufferedReader(
                new InputStreamReader(Files.newInputStream(file)))) {

            String size = br.readLine();
            Double2DoubleRBTreeMap joinedData = br.lines()
                    .map(line -> line.split(" "))
                    .map(arr -> new Record(Double.parseDouble(arr[0]), Double.parseDouble(arr[1])))
                    .flatMap(r -> dataForJoying.stream()
                            .map(d -> new Record(r.getCol1() + d.getCol1(), r.getCol2()*d.getCol2())))
                    .collect(() -> new Double2DoubleRBTreeMap(),
                            (m, r) -> m.addTo(r.getCol1(), r.getCol2()),
                            (m1, m2) -> m1.putAll(m2));

            BidirectionalIterator<Double> iterator = joinedData.keySet().iterator(joinedData.lastDoubleKey());
            double total = 0;
            while (iterator.hasPrevious()) {
                double indx = iterator.previous();
                total = total + joinedData.get(indx);
                joinedData.put(indx, total);
            }
            return joinedData;

        }
    }
}
