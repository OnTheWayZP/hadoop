package com.zapoul.hadoop.partitioner;

import com.zapoul.hadoop.flow.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created with IDEA
 * author:zapoul
 * Date:2019/9/26
 * Time:20:51
 */
public class MyPartitioner extends Partitioner<Text, FlowBean> {

    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        String phone = text.toString();

        switch (phone.substring(0, 3)) {
            case "136":
                return 0;
            case "137":
                return 1;
            case "138":
                return 2;
            case "139":
                return 3;
            default:
                return 4;
        }
    }
}
