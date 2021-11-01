package org.query.executor;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Step 2: Data Joiner: Parallelly perform Data joiner operation to perform T1 LEFT JOIN (SELECT * FROM t2 JOIN t3)
 * This scope of this class is to perform Data Joiner
 *
 */

public class DataJoiner implements Runnable {

    private Double[][] left = null;
    private Double[][] right = null;
    Map<Double,List<Double[]>> aggregatedData = null;
    Map<Double,Integer> indexes = null;
    AtomicInteger index = null;
    private Double a;
    private Double x;
    ReentrantLock lock = null;
    public DataJoiner(Double[][] left, Double[][] right,Map<Double,List<Double[]>> aggregatedData,Map<Double,Integer> indexes,AtomicInteger index, Double a, Double x,ReentrantLock lock){
        this.left = left;
        this.right = right;
        this.aggregatedData = aggregatedData;
        this.indexes = indexes;
        this.index = index;
        this.a = a;
        this.x = x;
        this.lock = lock;
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
    public void run() {List<Double[]> outputData = new LinkedList<>();
        boolean isAdded = false;
        for(int t2= 0;  t2 < left.length; t2++ ){
            Double b = left[t2][0];
            Double y = left[t2][1];

            for(int t3= 0;  t3 < right.length; t3++ ){
                Double c = right[t3][0];
                Double z = right[t3][1];
                if( a < b+c){
                    outputData.add(new Double[]{a,x,b,y,c,z});
                    isAdded = true;
                }
            }
        }
        if(!isAdded){
            outputData.add(new Double[]{a,x,0.0,0.0,0.0,0.0});
        }
        lock.lock();
        List<Double[]> tempOutput = aggregatedData.get(a);
        if(tempOutput != null && !tempOutput.isEmpty()){
            tempOutput.addAll(outputData);
        }else {
            tempOutput = outputData;
            index.incrementAndGet();
        }
        if(!indexes.containsKey(a)){
            indexes.put(a, index.get());
        }
        aggregatedData.put(a,tempOutput);
        lock.unlock();
    }
}
