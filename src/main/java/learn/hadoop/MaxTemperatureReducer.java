package learn.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author: tunzao
 * @date: 13-12-25 下午6:06
 */
public class MaxTemperatureReducer extends MapReduceBase
        implements Reducer<Text, IntWritable, Text, IntWritable>{
    public void reduce(Text text, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> textIntWritableOutputCollector, Reporter reporter) throws IOException {
        int maxTemperature = Integer.MIN_VALUE;
        while (values.hasNext()) {
            maxTemperature = Math.max(maxTemperature, values.next().get());
        }
        textIntWritableOutputCollector.collect(text, new IntWritable(maxTemperature));
    }
}
