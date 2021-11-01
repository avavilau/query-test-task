

import it.unimi.dsi.fastutil.io.BinIO;
import org.query.executor.DataAggregator;
import org.query.executor.DataJoiner;
import org.query.executor.FileParser;

import org.query.model.FileContent;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;


/**
 * IGNORE THIS FILE, THIS IS LOCAL TESTING
 */
public class LocalTester {

    public static void main(String args[]){

        Date timenow = new Date();
        Path t1 = Paths.get("/Users/amprabak/Downloads/case-3/t1");
        Path t2 = Paths.get("/Users/amprabak/Downloads/case-3/t2");
        Path t3 = Paths.get("/Users/amprabak/Downloads/case-3/t3");
        Path output = Paths.get("/Users/amprabak/Downloads/case-3/expected");
        List<FileParser> processorList = new ArrayList<>();

        processorList.add(new FileParser(t2));
        processorList.add(new FileParser(t3));
        ExecutorService fileParsingExecutor = Executors.newFixedThreadPool(3);
        try {

            //Step 1: Input parsing
            List<Future<Double[][]>> outputs = fileParsingExecutor.invokeAll(processorList);
            Future<Double[][]> t1Result = fileParsingExecutor.submit(new FileParser(t1));
            fileParsingExecutor.shutdown();
            fileParsingExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

            //Step 2: Data Joiner
            Double[][] t1Data = t1Result.get();
            Double[][] t2Data = outputs.get(0).get();
            Double[][] t3Data = outputs.get(1).get();
            Map<Double,List<Double[]>> aggregatedData = new ConcurrentHashMap<>();
            Map<Double,Integer> indexes = new ConcurrentHashMap<>();
            ExecutorService joinerExecutor = Executors.newFixedThreadPool(5);
            AtomicInteger index = new AtomicInteger(-1);
            ReentrantLock lock = new ReentrantLock();
            for(int t1Index= 0;  t1Index < t1Data.length; t1Index++ )
                joinerExecutor.submit(new DataJoiner(t2Data,t3Data,aggregatedData,indexes,index,t1Data[t1Index][0],t1Data[t1Index][1],lock));
            t1Data = null;
            t2Data = null;
            t3Data = null;

            joinerExecutor.shutdown();
            joinerExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

            //Step 3: Data Aggregator
            List<FileContent> contents = new CopyOnWriteArrayList<>();
            ExecutorService aggregationExecutor = Executors.newFixedThreadPool(4);
            for(Map.Entry<Double, List<Double[]>> dataIndex : aggregatedData.entrySet())
                aggregationExecutor.submit(new DataAggregator(indexes.get(dataIndex.getKey()), dataIndex.getValue(),contents));

            aggregationExecutor.shutdown();
            aggregationExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            aggregatedData = null;
            //Step 4: Data Sorter/Limiter
            Collections.sort(contents);
            contents = contents.stream().limit(10).collect(Collectors.toList());

            //Step 5: Output writer
            StringBuffer expectedOutput  = new StringBuffer();
            expectedOutput.append(contents.size());
            expectedOutput.append("\n");
            for(FileContent f : contents){
                System.out.println(f.getData().left() + " "+f.getData().right());
                expectedOutput.append(String.format("%.6f", Math.round(f.getData().left() * 1000000.0) / 1000000.0));
                expectedOutput.append(" ");
                expectedOutput.append(String.format("%.6f", Math.round(f.getData().right() * 1000000.0) / 1000000.0));
                expectedOutput.append("\n");
            }
            contents = null;
            BinIO.storeBytes(expectedOutput.toString().getBytes(),output.toFile());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }catch(ExecutionException exceptionExp){
            exceptionExp.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }catch(Exception e){
            e.printStackTrace();
        }

        System.out.print(new Date().getTime()- timenow.getTime());

    }


}
