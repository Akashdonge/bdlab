package employee;

import java.io.IOException;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;

public class mapper extends MapReduceBase
implements Mapper<LongWritable, Text, Text, DoubleWritable>
{
    public void map(LongWritable key, Text value,
                    OutputCollector<Text, DoubleWritable> output,
                    Reporter r) throws IOException
    {
        String[] line = value.toString().split("\\t");
        if (line.length < 9) return;

        String gender = line[3];
        Double salary = Double.parseDouble(line[8]);

        output.collect(new Text(gender), new DoubleWritable(salary));
    }
}
package employee;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;

public class reducer extends MapReduceBase
implements Reducer<Text, DoubleWritable, Text, DoubleWritable>
{
    public void reduce(Text key, Iterator<DoubleWritable> values,
                       OutputCollector<Text, DoubleWritable> output,
                       Reporter r) throws IOException
    {
        double sum = 0;
        int count = 0;

        while (values.hasNext()) {
            sum += values.next().get();
            count++;
        }

        output.collect(new Text(key + " Average"),
                       new DoubleWritable(sum / count));

        output.collect(new Text(key + " Count"),
                       new DoubleWritable(count));
    }
}
package employee;

import java.io.IOException;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.Path;

public class driver
{
    public static void main(String[] args) throws IOException
    {
        JobConf conf = new JobConf(driver.class);

        conf.setMapperClass(mapper.class);
        conf.setReducerClass(reducer.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}
