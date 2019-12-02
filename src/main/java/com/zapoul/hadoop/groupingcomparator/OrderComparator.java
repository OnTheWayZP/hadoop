package com.zapoul.hadoop.groupingcomparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created with IDEA
 * author:zapoul
 * Date:2019/9/27
 * Time:15:56
 */
public class OrderComparator extends WritableComparator {
    
    protected OrderComparator() {
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean a1 = (OrderBean) a;
        OrderBean b1 = (OrderBean) b;

        return a1.getOrderId().compareTo(b1.getOrderId());
    }
}
