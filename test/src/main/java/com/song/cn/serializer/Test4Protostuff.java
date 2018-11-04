package com.song.cn.serializer;

import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * 参考：
 * java序列化/反序列化之xstream、protobuf、protostuff 的比较与使用例子
 * https://www.cnblogs.com/xiaoMzjm/p/4555209.html
 */
public class Test4Protostuff {

    public static void main(String[] args) throws IOException {
        List<IdnoAndName> ians = new ArrayList<>();
        String path = "D:\\Desktop\\redis";
        Collection<File> files = FileUtils.listFiles(new File(path), new String[]{"bjson"}, false);

        List<IdnoAndName> ianList = new ArrayList<>();
        for (File file : files) {
            //读取文件中的内容，并构造成对象
            LineIterator iterator = FileUtils.lineIterator(file);
            while (iterator.hasNext()) {
                String line = iterator.nextLine();
                String[] splits = line.split("\t");
                IdnoAndName ian = new IdnoAndName();
                ian.setIdno(splits[0]);
                ian.setName(splits[1]);
                ianList.add(ian);

            }
            //使用protostuff序列化数据
            List<byte[]> bytes = Test4Protostuff.serializeProtoStuffProductsList(ianList);

            //将序列化的数据写到文件中
            for (int i = 0; i < bytes.size(); i++) {
                //FileUtils.writeByteArrayToFile(new File(file.getParent()+File.separator+"convert.nb"),bytes.get(i),true);
                FileUtils.writeStringToFile(new File(file.getParent() + File.separator + "convert.nb"), new String(bytes.get(i)), "utf-8", true);
                //FileUtils.write(new File(file.getParent()+File.separator+"convert.nb"),new String(bytes.get(i)),"utf-8",true);
                //FileUtils.write(new File(file.getParent()+File.separator+"convert.nb"),Arrays.toString(bytes.get(i)),"utf-8",true);
            }
//            FileUtils.writeLines(new File(file.getParent()+File.separator+"convert.nb"),bytes,"\n");

            //反序列化输出
            LineIterator convertInterator = FileUtils.lineIterator(new File(file.getParent() + File.separator + "convert.nb"));
            List<byte[]> result = new ArrayList<>();
            while (convertInterator.hasNext()) {
                String tmp = convertInterator.nextLine();
                if ("".equals(tmp)) {
                    continue;
                }
                tmp = "\n" + tmp;
                byte[] bytes1 = tmp.getBytes(StandardCharsets.UTF_8);
                result.add(bytes1);
            }
            List<IdnoAndName> idnoAndNames = Test4Protostuff.deserializeProtoStuffDataListToProductsList(result);
            for (IdnoAndName idnoAndName : idnoAndNames) {
                System.out.println(idnoAndName);
            }
        }

    }


    public static List<byte[]> serializeProtoStuffProductsList(List<IdnoAndName> ianList) {
        if (ianList == null || ianList.size() <= 0) {
            return null;
        }
        long start = System.currentTimeMillis();
        List<byte[]> bytes = new ArrayList<byte[]>();
        //Schema<IdnoAndName> schema = RuntimeSchema.getSchema(IdnoAndName.class);
        Schema<IdnoAndName> schema = RuntimeSchema.createFrom(IdnoAndName.class);
        LinkedBuffer buffer = LinkedBuffer.allocate(4096);
        byte[] protostuff = null;
        for (IdnoAndName p : ianList) {
            try {
                protostuff = ProtostuffIOUtil.toByteArray(p, schema, buffer);
                bytes.add(protostuff);
            } finally {
                buffer.clear();
            }
        }
        long end = System.currentTimeMillis();
        long userTime = end - start;
        return bytes;
    }

    public static List<IdnoAndName> deserializeProtoStuffDataListToProductsList(List<byte[]> bytesList) {
        if (bytesList == null || bytesList.size() <= 0) {
            return null;
        }
        long start = System.currentTimeMillis();
        Schema<IdnoAndName> schema = RuntimeSchema.getSchema(IdnoAndName.class);
        List<IdnoAndName> list = new ArrayList<IdnoAndName>();
        for (byte[] bs : bytesList) {
            IdnoAndName idnoAndName = new IdnoAndName();
            ProtostuffIOUtil.mergeFrom(bs, idnoAndName, schema);
            list.add(idnoAndName);
        }
        long end = System.currentTimeMillis();
        long userTime = end - start;
        return list;
    }
}
