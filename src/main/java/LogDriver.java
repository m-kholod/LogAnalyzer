import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class LogDriver  extends Configured implements Tool {

    enum ipAddress {
        MALFORMED
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.printf("Usage: %s [generic options] <input> <intermediate> <sorted>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Log analyzer");
        job1.setJarByClass(getClass());
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.setMapperClass(LogMapper.class);
        job1.setCombinerClass(LogReducer.class);
        job1.setReducerClass(LogReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        job1.waitForCompletion(true);

        Counters counters = job1.getCounters();
        long missing = counters.findCounter(
                ipAddress.MALFORMED).getValue();
        long total = counters.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
        System.out.printf("Records with malformed IP: %d out of %d (%.2f%%)\n", missing, total,
                100.0 * missing / total);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Sorting");
        job2.setJarByClass(getClass());
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        job2.setMapperClass(InverseMapper.class);
        job2.setReducerClass(GroupReducer.class);
        job2.setSortComparatorClass(DescendingSortComparator.class);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);

        return job2.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new LogDriver(), args);
        System.exit(exitCode);
    }
}
