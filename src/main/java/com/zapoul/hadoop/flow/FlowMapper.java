package com.zapoul.hadoop.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created with IDEA
 * author:zapoul
 * Date:2019/9/24
 * Time:9:06
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    private Text phone = new Text();
    private FlowBean flowBean = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        phone.set(split[1]);
        flowBean.set(Long.parseLong(split[split.length - 3]), Long.parseLong(split[split.length - 2]));
        context.write(phone, flowBean);
    }

}
