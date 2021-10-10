package org.query.calc.model;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import java.util.stream.Stream;

@RequiredArgsConstructor
public class InputFile {

    @Getter
    private final int size;
    private final Stream<String> content;

    @Setter
    private boolean parallelProcessing;

    public Stream<String> getContent() {
        if (parallelProcessing) {
            return content.parallel();
        }
        return content;
    }
}
