package mapreduce.friend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Basic Friends Discover
 * Both Followed and Following is considered as a relation
 * A-B B-C => recommend C to A and A to C even if A-C is already friend
 */
public class FriendsDiscover {
    public static class Deg2FriendMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] ss = line.split(" ");
            context.write(new Text(ss[0]), new Text(ss[1]));
            context.write(new Text(ss[1]), new Text(ss[0]));
        }
    }

    public static class Deg2Reducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> value, Context context)
                throws IOException, InterruptedException {

            Set<String> set = new HashSet<String>();

            for (Text t : value) {
                set.add(t.toString());
            }
            if (set.size() >= 2) {
                Iterator<String> iter = set.iterator();
                while (iter.hasNext()) {
                    String name = iter.next();
                    for (Iterator<String> iter2 = set.iterator(); iter2.hasNext(); ) {
                        String name2 = iter2.next();
                        if (!name2.equals(name)) {
                            context.write(new Text(name), new Text(name2));
                        }
                    }
                }

            }
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "mapreduce.friend.FriendsDiscover");
        job.setJarByClass(FriendsDiscover.class);

        job.setMapperClass(Deg2FriendMapper.class);
        job.setReducerClass(Deg2Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
