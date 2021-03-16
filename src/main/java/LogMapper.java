import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LogMapper  extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final LogParser parser = new LogParser();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        parser.process(value);
        if (parser.isValidIpAddress()) {
            context.write(new Text(parser.getIpAddress()),
                    new IntWritable(1));
        } else {
            context.getCounter(LogDriver.ipAddress.MALFORMED).increment(1);
        }

    }
}
