/**
 * Created by linfeng on 2020/01/01.
 */

import com.hadoop.compression.lzo.LzopCodec;
import com.hadoop.mapreduce.LzoTextInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Main extends Configured implements Tool {

    private static String datetime;
    private static String host;
    private static String inputFilePath = "/hdfs/log/";

    private static Logger logger = LoggerFactory.getLogger(Main.class);


    public static class CustomMapper extends Mapper<LongWritable, Text, MapKeyOutputWritable, MapValueOutputWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws InterruptedException {
//            Configuration conf = context.getConfiguration();
            String line = value.toString();
            String[] lineArr = line.split("\\t");

            try {
                context.write(new MapKeyOutputWritable(lineArr[0], lineArr[1]), new MapValueOutputWritable(lineArr[2],
                        lineArr[3]));
            } catch (IOException e) {
                logger.error("CustomMapper write line" + line + " failed IOException error " + e.getMessage());
            }
        }
    }


    public static class CustomReducer extends Reducer<MapKeyOutputWritable, MapValueOutputWritable, DBOutputWritable,
            NullWritable> {
        @Override
        public void reduce(MapKeyOutputWritable key, Iterable<MapValueOutputWritable> values, Context context) {
            Configuration conf = context.getConfiguration();
            StringBuilder sumVal1 = new StringBuilder();
            StringBuilder sumVal2 = new StringBuilder();
            for (MapValueOutputWritable value : values) {
                sumVal1.append(value.getVal1());
                sumVal2.append(value.getVal2());
            }
            try {
                context.write(new DBOutputWritable(conf.get("datetime"), key.getKey1(), key.getKey2(),
                        sumVal1.toString(), sumVal2.toString()), NullWritable.get());
            } catch (IOException e) {
                logger.error("CustomReducer write key " + key.toString() + " failed IOException error " +
                        e.getMessage());
            } catch (InterruptedException e) {
                logger.error("CustomReducer write key " + key.toString() + " failed InterruptedException error " +
                        e.getMessage());
            }
        }
    }


    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // 判断日志是否存在，不存在则不进行相应计算
        FileSystem hdfs = FileSystem.get(conf);
        Path inputFilePath = new Path(Main.inputFilePath);
        boolean pathExist = hdfs.exists(inputFilePath);
        if (!pathExist) {
            logger.error("hdfs path not exist file: " + inputFilePath.toString());
            return 0;
        }
        conf.set("datetime", args[0]);  // 将datetime传入reduce
        conf.set("mapreduce.job.queuename", "root.common_queue");

        // 配置可用内存
        conf.set("mapreduce.map.memory.mb", "16384");
        conf.set("mapreduce.map.java.opts", "-Xmx13108m");
        conf.set("mapreduce.reduce.memory.mb", "16384");
        conf.set("mapreduce.reduce.java.opts", "-Xmx13108m");

        // 配置reduce个数
        conf.set("mapreduce.reduce.tasks", "250");

        DBConfiguration.configureDB(conf,
                "com.mysql.cj.jdbc.Driver",
                "jdbc:mysql://127.0.0.1:3306/db_name?useUnicode=true&characterEncoding=UTF-8&useSSL=false&&serverTimezone=UTC",
                "db_user",
                "db_password");

        Job job = Job.getInstance(conf, "job_name");
        job.setJarByClass(Main.class);

        FileInputFormat.addInputPath(job, inputFilePath);
        FileInputFormat.setInputDirRecursive(job, true);

        // 配置输入类型为lzo
        job.setInputFormatClass(LzoTextInputFormat.class);

        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, LzopCodec.class);

        job.setMapperClass(CustomMapper.class);
        job.setReducerClass(CustomReducer.class);

        job.setMapOutputKeyClass(MapKeyOutputWritable.class);
        job.setMapOutputValueClass(MapValueOutputWritable.class);

        job.setOutputKeyClass(DBOutputWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(DBOutputFormat.class);

        String[] fields = new String[]{"datetime", "key1", "key2", "val1", "val2"};
        DBOutputFormat.setOutput(job, "tab_name", fields);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        Main.datetime = formatter.format(new Date().getTime() - 86400 * 1000);
        if ((args.length > 0) && (args[0].length() > 0)) {
            Main.host = args[0];
            Main.inputFilePath += Main.host + "/";
        } else {
            logger.error("main get host: " + Main.host + " at " + Main.datetime + " lose necessary param");
            return;
        }

        Main.inputFilePath += Main.datetime.replace("-", "");

        logger.info("main get host: " + Main.host + " at " + Main.datetime + " start");
        int res = ToolRunner.run(new Configuration(), new Main(), new String[]{Main.host, Main.datetime});
        logger.info("main get host: " + Main.host + " at " + Main.datetime + " end status is " + res);
        System.exit(res);
    }
}
