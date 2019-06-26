import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;


public class USpopulation extends Configured implements Tool {
    public static class USMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.get() == 0) {
                return;
            } else {
                String[] line = value.toString().split(";");
                String[] number = line[3].split("\\.");

                int value_data = Integer.parseInt(number[0]);

                Text key_data = new Text();
                key_data.set(line[0]);
                //key_data.set(line[0] + ";" + line[2]);

                context.write(new Text(key_data), new IntWritable(value_data));
            }
        }
    }

    public static class USReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int total = 0;
            for (IntWritable val : values) {
                total = total + val.get();
            }
            context.write(new Text(key), new IntWritable(total));  // Write reduce result {word,count}
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        args = new GenericOptionsParser(conf, args).getRemainingArgs();
        FileSystem.get(conf).delete(new Path("/user/kgm1/out/"), true);

        Job job = Job.getInstance(conf);
        job.setJarByClass(USpopulation.class);
        job.setMapperClass(USMapper.class);
        job.setReducerClass(USReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new USpopulation(), args);
        System.exit(exitCode);
    }
}

