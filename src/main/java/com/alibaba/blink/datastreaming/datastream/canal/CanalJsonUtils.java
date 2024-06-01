package com.alibaba.blink.datastreaming.datastream.canal;

import com.alibaba.blink.datastreaming.datastream.action.AbstractDtsToKafkaFlinkAction;
import com.alibaba.blink.datastreaming.datastream.action.RouteDef;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by hzy
 *
 * @author hzy
 */
public class CanalJsonUtils {
    private final long startTime = System.currentTimeMillis();
    private long recordCount = 0;
    private static final Map<Integer, String> typeMap = new HashMap<>();

    private static final String SYSTEM_PHYSICAL_DB_KEY = "SYSTEM_PHYSICAL_DB";
    private static final String SYSTEM_PHYSICAL_TABLE_KEY = "SYSTEM_PHYSICAL_TABLE";
    private static final String SYSTEM_LOGICAL_DB_KEY = "SYSTEM_LOGICAL_DB";
    private static final String SYSTEM_LOGICAL_TABLE_KEY = "SYSTEM_LOGICAL_TABLE";
    private static final String SYSTEM_OP_TS_KEY = "SYSTEM_OP_TS";
    private static final String SYSTEM_OP_TRACEID_KEY = "SYSTEM_OP_TRACEID";
    private static final String PROV_KEY = "prov";
    private static final String PROV_KEY_UPPER = "PROV";

    private static final String DTS_FIELDS_NAME_KEY = "name";
    private static final String DTS_FIELDS_DATA_TYPE_NUMBER_KEY = "dataTypeNumber";

    static {
        typeMap.put(0, "DECIMAL");
        typeMap.put(1, "TINYINT");
        typeMap.put(2, "SMALLINT");
        typeMap.put(3, "INT");
        typeMap.put(4, "FLOAT");
        typeMap.put(5, "DOUBLE");
        typeMap.put(6, "NULL");
        typeMap.put(7, "TIMESTAMP");
        typeMap.put(8, "BIGINT");
        typeMap.put(9, "MEDIUMINT");
        typeMap.put(10, "DATE");
        typeMap.put(11, "TIME");
        typeMap.put(12, "DATETIME");
        typeMap.put(13, "YEAR");
        typeMap.put(14, "DATE");
        typeMap.put(15, "VARCHAR");
        typeMap.put(16, "BIT");
        typeMap.put(17, "TIMESTAMP");
        typeMap.put(18, "DATETIME");
        typeMap.put(19, "TIME");
        typeMap.put(245, "JSON");
        // decimal we cannot get scale so use string
        typeMap.put(246, "STRING");
        typeMap.put(247, "ENUM");
        typeMap.put(248, "SET");
        typeMap.put(249, "TINYBLOB");
        typeMap.put(250, "MEDIUMBLOB");
        typeMap.put(251, "LONGBLOB");
        typeMap.put(252, "BLOB");
        typeMap.put(253, "STRING");
        typeMap.put(254, "STRING");
        typeMap.put(255, "GEOMETRY");
        // 添加其他类型映射...
    }

    public static void main(String[] args) {
        JSONObject jsonObject = JSON.parseObject("{\"version\": 0, \"id\": 1858644, \"sourceTimestamp\": 1712905965, \"sourcePosition\": \"2924526@143\", \"safeSourcePosition\": \"2924372@143\", \"sourceTxid\": \"0\", \"source\": {\"sourceType\": \"MySQL\", \"version\": \"8.0.34\"}, \"operation\": \"INSERT\", \"objectName\": \"test_drds_hzy_pyii_0005.sample_order_real_aeje_11\", \"processTimestamps\": null, \"tags\": {\"thread_id\": \"2533976\", \"readerThroughoutTime\": \"1712905965384\", \"l_tb_name\": \"sample_order_real\", \"pk_uk_info\": \"{\\\"PRIMARY\\\":[\\\"id\\\"]}\", \"metaVersion\": \"827452312\", \"server_id\": \"2814094928\", \"l_db_name\": \"test_drds_hzy\"}, \"fields\": [{\"name\": \"id\", \"dataTypeNumber\": 3}, {\"name\": \"seller_id\", \"dataTypeNumber\": 3}, {\"name\": \"trade_id\", \"dataTypeNumber\": 3}, {\"name\": \"buyer_id\", \"dataTypeNumber\": 3}, {\"name\": \"buyer_nick\", \"dataTypeNumber\": 253}], \"beforeImages\": null, \"afterImages\": [{\"precision\": 4, \"value\": \"10000\"}, {\"precision\": 4, \"value\": \"11\"}, {\"precision\": 4, \"value\": \"122\"}, {\"precision\": 4, \"value\": \"33\"}, {\"charset\": \"utf8mb3\", \"value\": \"aaas\"}]}");
        CanalJson convert = convert(jsonObject);
        System.out.println(JSON.toJSONString(convert));
    }

    public static CanalJson convert(JSONObject dtsJsonObject) {
        return convert(dtsJsonObject, null, null);
    }

    public static CanalJson convert(JSONObject dtsJsonObject, List<RouteDef> routeDefs, HashMap<String, String> extraColumns) {
        CanalJson canalJson = new CanalJson();
        canalJson.setType(dtsJsonObject.getString("operation")); // 假定 operation 字段即代表了 DTS 操作类型 UPDATE, INSERT 等
        canalJson.setId(dtsJsonObject.getLong("id"));


        // 将数据库和表名分割
        String dtsObjectName = dtsJsonObject.getString("objectName");
        String dtsObjectNamePerformed = AbstractDtsToKafkaFlinkAction.convertTableNameIfMatched(routeDefs, dtsObjectName);
        String[] dbTableArray = dtsObjectNamePerformed.split("\\.");
        if (dbTableArray.length == 1) {
            canalJson.setDatabase(dbTableArray[0]);
        } else {
            canalJson.setDatabase(dbTableArray[0]);
            canalJson.setTable(dbTableArray[1]);
        }

        if ("DDL".equals(canalJson.getType())) {
            JSONObject afterImages = dtsJsonObject.getJSONObject("afterImages");
            if (afterImages != null && afterImages.containsKey("ddl")) {
                canalJson.setSql(afterImages.getString("ddl"));
                canalJson.setIsDdl(true);
            }
        } else {
            HashMap<String, String> extraFieldMap = new HashMap<>();
            String systemOpTs = dtsJsonObject.getString("sourceTimestamp");
            if (systemOpTs != null) {
                extraFieldMap.put(SYSTEM_OP_TS_KEY, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Timestamp(Long.valueOf(systemOpTs) * 1000L)));
            }
            extraFieldMap.put(SYSTEM_OP_TRACEID_KEY, dtsJsonObject.getString("id"));

            String[] sourceTableArray = dtsObjectName.split("\\.");
            if (sourceTableArray.length == 1) {
                extraFieldMap.put(SYSTEM_PHYSICAL_DB_KEY, sourceTableArray[0]);
                extraFieldMap.put(SYSTEM_PHYSICAL_TABLE_KEY, null);
            } else {
                extraFieldMap.put(SYSTEM_PHYSICAL_DB_KEY, sourceTableArray[0]);
                extraFieldMap.put(SYSTEM_PHYSICAL_TABLE_KEY, sourceTableArray[1]);
            }

            if (dbTableArray.length == 1) {
                extraFieldMap.put(SYSTEM_LOGICAL_DB_KEY, dbTableArray[0]);
                extraFieldMap.put(SYSTEM_LOGICAL_TABLE_KEY, null);
            } else {
                extraFieldMap.put(SYSTEM_LOGICAL_DB_KEY, dbTableArray[0]);
                extraFieldMap.put(SYSTEM_LOGICAL_TABLE_KEY, dbTableArray[1]);
            }

            if (extraColumns != null && !extraColumns.isEmpty()) {
                extraFieldMap.put(PROV_KEY_UPPER, extraColumns.get(PROV_KEY));
            }

            // 需要一个键值对列表来表示之前的图像和之后的图像。
            List<Map<String, String>> oldList = new ArrayList<>();
            oldList.add(convertFieldMap(dtsJsonObject.getJSONObject("beforeImages"), extraFieldMap));
            if (!"DELETE".equals(canalJson.getType())) {
                canalJson.setOld(oldList);
            }

            List<Map<String, String>> dataList = new ArrayList<>();
            dataList.add(convertFieldMap(dtsJsonObject.getJSONObject("afterImages"), extraFieldMap));
            canalJson.setData(dataList);
            if ("DELETE".equals(canalJson.getType())) {
                canalJson.setData(oldList);
            }

            JSONArray dtsJsonFields = dtsJsonObject.getJSONArray("fields");
            dtsJsonFields.add(new JSONObject() {{
                put(DTS_FIELDS_NAME_KEY, SYSTEM_PHYSICAL_DB_KEY);
                put(DTS_FIELDS_DATA_TYPE_NUMBER_KEY, 254);
            }});
            dtsJsonFields.add(new JSONObject() {{
                put(DTS_FIELDS_NAME_KEY, SYSTEM_PHYSICAL_TABLE_KEY);
                put(DTS_FIELDS_DATA_TYPE_NUMBER_KEY, 254);
            }});
            dtsJsonFields.add(new JSONObject() {{
                put(DTS_FIELDS_NAME_KEY, SYSTEM_LOGICAL_DB_KEY);
                put(DTS_FIELDS_DATA_TYPE_NUMBER_KEY, 254);
            }});
            dtsJsonFields.add(new JSONObject() {{
                put(DTS_FIELDS_NAME_KEY, SYSTEM_LOGICAL_TABLE_KEY);
                put(DTS_FIELDS_DATA_TYPE_NUMBER_KEY, 254);
            }});
            dtsJsonFields.add(new JSONObject() {{
                put(DTS_FIELDS_NAME_KEY, SYSTEM_OP_TS_KEY);
                put(DTS_FIELDS_DATA_TYPE_NUMBER_KEY, 7);
            }});
            dtsJsonFields.add(new JSONObject() {{
                put(DTS_FIELDS_NAME_KEY, SYSTEM_OP_TRACEID_KEY);
                put(DTS_FIELDS_DATA_TYPE_NUMBER_KEY, 254);
            }});
            dtsJsonFields.add(new JSONObject() {{
                put(DTS_FIELDS_NAME_KEY, PROV_KEY_UPPER);
                put(DTS_FIELDS_DATA_TYPE_NUMBER_KEY, 254);
            }});


            // 字段类型和字段 SQL 类型映射
            Map<String, Integer> sqlTypeMap = dtsJsonFields.stream().collect(Collectors.toMap(fieldObj -> ((JSONObject) fieldObj).getString("name"), fieldObj -> ((JSONObject) fieldObj).getInteger("dataTypeNumber")));
            canalJson.setSqlType(sqlTypeMap);
            Map<String, String> mysqlType = convertToTypeNameMap(sqlTypeMap);
            canalJson.setMysqlType(mysqlType);

            // 其他字段根据实际情况进行填充
            canalJson.setEs(dtsJsonObject.getLong("sourceTimestamp"));
            // TimeStamp in DTS JSON seems to be in seconds, while in Canal it's expected in milliseconds.
            canalJson.setTs(dtsJsonObject.getLong("tags.readerThroughoutTime"));
            // Assuming that the "tags.pk_uk_info" includes primary key info in JSON format.
            JSONObject tags = dtsJsonObject.getJSONObject("tags");
            if (tags != null && tags.containsKey("pk_uk_info")) {
                JSONObject pkInfo = JSON.parseObject(tags.getString("pk_uk_info"));
                if (pkInfo != null && pkInfo.containsKey("PRIMARY")) {
                    canalJson.setPkNames(pkInfo.getJSONArray("PRIMARY").toJavaList(String.class));
                }
            }
        }


        return canalJson;
    }

    public static Map<String, String> convertToTypeNameMap(Map<String, Integer> sqlTypeMap) {
        Map<String, String> sqlTypeNameMap = new HashMap<>();
        for (Map.Entry<String, Integer> entry : sqlTypeMap.entrySet()) {
            String fieldName = entry.getKey();
            Integer typeNumber = entry.getValue();
            String typeName = typeMap.getOrDefault(typeNumber, "STRING");
            sqlTypeNameMap.put(fieldName, typeName);
        }
        return sqlTypeNameMap;
    }

    private static Map<String, String> convertFieldMap(JSONObject fieldJson, HashMap<String, String> extraFieldMap) {
        if (fieldJson == null) {
            return null;
        }
        Map<String, String> fieldMap = new HashMap<>();
        for (String key : fieldJson.keySet()) {
            fieldMap.put(key, fieldJson.getString(key));
        }

        if (extraFieldMap != null && !extraFieldMap.isEmpty()) {
            extraFieldMap.forEach((key, value) -> {
                fieldMap.put(key, value);
            });
        }

        return fieldMap;
    }

    private static Map<String, String> convertFieldMap(JSONObject fieldJson) {
        return convertFieldMap(fieldJson, null);
    }
}
