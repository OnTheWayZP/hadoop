package com.zapoul.hadoop.groupingcomparator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created with IDEA
 * author:zapoul
 * Date:2019/9/27
 * Time:15:56
 */
public class OrderReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {

    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //context.write(key, NullWritable.get());
        //订单最高的前两名
        Iterator<NullWritable> iterator = values.iterator();
        for (int i = 0; i < 2; i++) {
            if (iterator.hasNext()) {
                context.write(key, iterator.next());
            }
        }
    }
}
