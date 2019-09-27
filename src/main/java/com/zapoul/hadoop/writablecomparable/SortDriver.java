package com.zapoul.hadoop.writablecomparable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * bean对象做为key传输，需要实现WritableComparable接口重写compareTo方法，就可以实现排序。
 * Created with IDEA
 * author:zapoul
 * Date:2019/9/27
 * Time:11:21
 */
public class SortDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.获取一个job实例
        Job job = Job.getInstance(new Configuration());

        //2.设置我们的类路径
        job.setJarByClass(SortDriver.class);

        //3.设置Mapper和Reducer
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        //4.设置Mapper和Reducer输出类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //5.设置输入输出数据
        FileInputFormat.setInputPaths(job, new Path("D:\\output2"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\output\\out2"));

        //6. 提交我们的job
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
