package com.zapoul.hadoop.groupingcomparator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created with IDEA
 * author:zapoul
 * Date:2019/9/27
 * Time:15:55
 */
public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {

    private OrderBean orderBean = new OrderBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splits = value.toString().split("\t");
        orderBean.setOrderId(splits[0]);
        orderBean.setProductId(splits[1]);
        orderBean.setPrice(Double.parseDouble(splits[2]));
        context.write(orderBean, NullWritable.get());
    }
}
