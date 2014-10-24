package com.dsinpractice.samples.hadoop.mapred.customformat;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



/**
 * Created by admin on 10/23/14.
 */
public  class MyMapper extends Mapper<MyKey, MyValue, Text, Text> {


    public void map(MyKey key, MyValue value, Context context) throws java.io.IOException ,InterruptedException {
        String imei = key.getImei().toString();
        String model = key.getDeviceModel().toString();
        if(imei.startsWith("9765")){
            String status = value.getStatus().toString();
            String country = value.getCountry().toString();
            String basePhone = value.getBasePhone().toString();
            String advPhone = value.getAdvPhone().toString();
            context.write(new Text(imei),new Text(country));
        }

    }
}
