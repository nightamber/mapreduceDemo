package xin.mrbear.demo;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by mrbear
 *
 * 这个类就是mr程序运行时候的主类，本类中组装了一些程序运行时候所需要的信息
 * 比如：使用的是那个Mapper类  那个Reducer类  输入数据在那 输出数据在什么地方
 */
public class WordCount {

    public static void main(String[] args)
        throws IOException, ClassNotFoundException, InterruptedException {
        //通过Job来封装本次mr的相关信息
        Configuration conf = new Configuration();
        //conf.set("mapreduce.framework.name","local");
        Job wcjob = Job.getInstance(conf);
        //指定本次mr job jar包运行主类
        wcjob.setJarByClass(WordCount.class);
        //指定本次mr 所用的mapper reducer类分别是什么
        wcjob.setMapperClass(WordCountMapper.class);
        wcjob.setReducerClass(WordCountReducer.class);
        //指定本次mr 阶段的输出  k  v类型
        wcjob.setOutputKeyClass(Text.class);
        wcjob.setOutputValueClass(IntWritable.class);
        //指定本次mr 输入的数据路径 和最终输出结果存放在什么位置
        FileInputFormat.setInputPaths(wcjob,args[0]);
        FileOutputFormat.setOutputPath(wcjob,new Path(args[1]));

        boolean res = wcjob.waitForCompletion(true);
        System.exit(res?0:1);

    }
}
