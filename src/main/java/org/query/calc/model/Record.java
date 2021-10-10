package org.query.calc.model;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class Record implements Comparable<Record>{

    private final double col1;
    private final double col2;

    @Override
    public int compareTo(Record o) {
        return Double.compare(col1, o.getCol1());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Record)) return false;

        Record record = (Record) o;

        return Double.compare(record.col1, col1) == 0;
    }

    @Override
    public int hashCode() {
        long temp = Double.doubleToLongBits(col1);
        return (int) (temp ^ (temp >>> 32));
    }

    @Override
    public String toString() {
        return col1 + " " + col2;
    }
}
