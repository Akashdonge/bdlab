package oddeven;

import java.io.IOException;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;

public class mapper extends MapReduceBase
implements Mapper<LongWritable, Text, Text, IntWritable>
{
    public void map(LongWritable key, Text value,
                    OutputCollector<Text, IntWritable> output,
                    Reporter r) throws IOException
    {
        String[] nums = value.toString().split(" ");

        for (String n : nums) {
            int number = Integer.parseInt(n);

            if (number % 2 == 0)
                output.collect(new Text("even"), new IntWritable(number));
            else
                output.collect(new Text("odd"), new IntWritable(number));
        }
    }
}
package oddeven;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;

public class reducer extends MapReduceBase
implements Reducer<Text, IntWritable, Text, IntWritable>
{
    public void reduce(Text key, Iterator<IntWritable> values,
                       OutputCollector<Text, IntWritable> output,
                       Reporter r) throws IOException
    {
        int sum = 0, count = 0;

        while (values.hasNext()) {
            sum += values.next().get();
            count++;
        }

        output.collect(new Text("Sum of " + key + " Numbers"),
                       new IntWritable(sum));
        output.collect(new Text(key + " Number count"),
                       new IntWritable(count));
    }
}
package oddeven;

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
        conf.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}
javac -classpath $(hadoop classpath) -d . *.java
echo Main-Class: wordcount.driver > Manifest.txt
jar cfm wordcount.jar Manifest.txt wordcount/*.class
hadoop jar wordcount.jar input output
cat output/*
