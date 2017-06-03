/**
 * Created by zhufangze on 2017/6/1.
 * @param: user.input.path1
 * @param: user.input.path2
 * @param: user.param.sitemap.type
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GetFetchedUrl extends Configured implements Tool {

    public int run(String[] arg0) throws Exception {

        Configuration conf = getConf();
        Job job = new Job(conf, conf.get("mapred.job.name"));

        job.setJarByClass(GetFetchedUrl.class);
        job.setReducerClass(GetFetchedUrlReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        String input_path1 = conf.get("user.input.path1"); // fetch result's path, separated by ",", do not contain space between them
        String input_path2 = conf.get("user.input.path2");
        String input_fetchresults[] = input_path1.split(",");
        for (String p : input_fetchresults) {
            MultipleInputs.addInputPath(job, new Path(p.trim()), TextInputFormat.class, GetFetchedUrlMapper1.class);
        }
        MultipleInputs.addInputPath(job, new Path(input_path2), TextInputFormat.class, GetFetchedUrlMapper2.class);

        if (job.waitForCompletion(true) && job.isSuccessful()) {
            return 0;
        }
        return -1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new GetFetchedUrl(), args);
        System.exit(res);
    }
}