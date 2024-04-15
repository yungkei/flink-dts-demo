package com.alibaba.blink.datastreaming.datastream;

import java.util.Arrays;
import java.util.stream.Collectors;

public class Test {
    public static final String HDFS_PREFIX = "hdfs://";
    public static final String SLASH = "/";
    public static final String COLON = ":";
    public static void main(String[] args) {
        String location = "hdfs://dataphin-cdh6-cluster-00001:8020/tmp/savepoint";
        location = location.substring(HDFS_PREFIX.length());
        String address = location.split(COLON)[0];
        String port = location.split(COLON)[1].split(SLASH)[0];
        location = location.split(COLON)[1].substring(port.length());
    }
}
