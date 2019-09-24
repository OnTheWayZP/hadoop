package com.zapoul.hadoop.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created with IDEA
 * author:zapoul
 * Date:2019/9/24
 * Time:9:06
 */
public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    private FlowBean flowBean = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long sumUpFlow = 0;
        long sumDownFlow = 0;
        for (FlowBean flowBean : values) {
            sumDownFlow += flowBean.getDownFlow();
            sumUpFlow += flowBean.getUpFlow();
        }
        flowBean.set(sumUpFlow, sumDownFlow);
        context.write(key, flowBean);
    }
}
