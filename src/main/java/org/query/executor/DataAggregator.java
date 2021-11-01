package org.query.executor;

import it.unimi.dsi.fastutil.Pair;
import org.query.model.FileContent;

import java.util.List;

/**
 * Step 3: Data Aggregator: Parallelly perform Data aggregation to perform SUM(X * y * z)
 * This scope of this class is to perform Data Aggregation
 *
 */
public class DataAggregator implements Runnable{

    private int index;
    private List<FileContent> outputList;
    private List<Double[]> data;


    public DataAggregator(int index,List<Double[]> data, List<FileContent> outputList){
        this.index = index;
        this.data = data;
        this.outputList = outputList;
    }
    /**
     * When an object implementing interface <code>Runnable</code> is used
     * to create a thread, starting the thread causes the object's
     * <code>run</code> method to be called in that separately executing
     * thread.
     * <p>
     * The general contract of the method <code>run</code> is that it may
     * take any action whatsoever.
     *
     * @see Thread#run()
     */
    @Override
    public void run() {
        Double sum = 0.0;
        Double a = 0.0;
        for(Double[] data : data){
            Double mul =data[1] * data[3] * data[5];
            sum =sum+mul;
            a = data[0];
        }
        outputList.add(new FileContent(index, Pair.of(a,sum)));
    }
}
