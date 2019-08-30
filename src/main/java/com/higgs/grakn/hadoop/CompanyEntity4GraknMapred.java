package com.higgs.grakn.hadoop;


import com.higgs.grakn.client.HgraknClient;
import com.higgs.grakn.client.schema.Schema;
import com.higgs.grakn.variable.Variable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import grakn.client.GraknClient;
import graql.lang.Graql;
import graql.lang.query.GraqlInsert;
import graql.lang.query.GraqlQuery;
import graql.lang.statement.Statement;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import static graql.lang.Graql.var;


/**
 * User: JerryYou
 *
 * Date: 2019-08-22
 *
 * Copyright (c) 2018 devops
 *
 * <<licensetext>>
 */
public class CompanyEntity4GraknMapred extends Configured implements Tool {
  /**
   * Created by Jerry on 2018/8/22. 输入文件格式：（docId \t json)
   * 输出满足grakn格式的gql文件数据
   * 通过console导入文件数据到grakn中
   */
    private static Logger logger = LoggerFactory.getLogger(CompanyEntity4GraknMapred.class);

    public static class MyMap extends Mapper<LongWritable, Text, Text, Text> {
      private Counter skipperCounter;
      private Counter noFiledCounter;
      private Counter originSuccessCounter;
      private Counter jsonSuccessCounter;
      private Counter errorCounter;
      private int setBatch = 0;
      private int checkUid = 0;
      private long source = 0;
      private Counter timeOutErrorCounter;
      private MultipleOutputs<Text, Text> mos;

      @Override
      protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        mos = new MultipleOutputs<>(context);
        skipperCounter = context.getCounter("runner", "skipperCounter");
        timeOutErrorCounter = context.getCounter("runner", "timeOutErrorCounter");
        errorCounter = context.getCounter("runner", "errorCounter");
        noFiledCounter = context.getCounter("runner", "noFiledCounter");
        originSuccessCounter = context.getCounter("runner", "originSuccessCounter");
        jsonSuccessCounter = context.getCounter("runner", "jsonSuccessCounter");
        source = context.getConfiguration().getLong("source", 0);
        logger.info("source:" + source);
      }

      @Override
      protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
      }

      @Override
      public void run(Context context) throws IOException, InterruptedException {
        setup(context);
        int batch = 0;
        while (context.nextKeyValue()) {
          JsonObject infoObject = new JsonObject();
          String info = context.getCurrentValue().toString().trim();
          int index = info.indexOf("\t");
          String json = info.substring(0, index + "\t".length());
          try {
            infoObject = new JsonObject(json);
          } catch (Exception e) {
            errorCounter.increment(1);
            e.printStackTrace();
          }
          if (source == 1) {
            context.write(new Text(infoObject.getString("normedName", "NoDef")), new Text("1"));
          } else if (source == 2) {
            context.write(new Text(infoObject.getString("deptName", "NoDef")), new Text("1"));
          } else  if (source == 3) {
            context.write(new Text(infoObject.encode()), new Text("1"));
          }
          jsonSuccessCounter.increment(1L);
        }
        cleanup(context);
      }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

      private Counter counterReducer;
      private MultipleOutputs<Text, Text> mos;
      private Counter errorCounter;
      private int setBatch = 0;
      private GraknClient.Session session;
      private HgraknClient hgraknClient;
      private int checkUid = 0;
      private long source = 0;
      private long reduceNum = 20;
      private Counter originSuccessCounter;
      private Counter jsonSuccessCounter;
      private Counter timeOutErrorCounter;
      private Counter skipperCounter;

      @Override
      protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        mos = new MultipleOutputs<>(context);
        counterReducer = context.getCounter("reduce", "counterReducer");
        errorCounter = context.getCounter("reduce", "errorCounter");
        originSuccessCounter = context.getCounter("reduce", "originSuccessCounter");
        jsonSuccessCounter = context.getCounter("reduce", "jsonSuccessCounter");
        timeOutErrorCounter = context.getCounter("reduce", "timeOutErrorCounter");
        skipperCounter = context.getCounter("reduce", "skipperCounter");
        source = context.getConfiguration().getLong("source", 0);
        String graknServer = context.getConfiguration().get("GraknServer", "");
        String keySpace = context.getConfiguration().get("key_space", "");
        hgraknClient = new HgraknClient(graknServer, keySpace);
        session = hgraknClient.getClient().session(keySpace);
        reduceNum = context.getConfiguration().getLong("reduce_num", 20);
      }

      @Override
      protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        session.close();
        hgraknClient.getClient().close();
      }

      private List<Statement> getName(String type,String key) {
        String var = Variable.getVarValue(type, key);
        List<Statement> statements = Graql.insert(
            var(var).isa(Schema.Entity.ENTITY.getName()),
            var(var).has(Schema.Attribute.NAME.getName(), key)
        ).statements();
        return statements;
      }

      private List<Statement> getDeptRel(String type, String... key) {
        List<String> keys = Arrays.asList(key);
        if (keys.size() < 2) {
          return null;
        }
        String companyName = keys.get(0);
        String deptName = keys.get(1);
        String companyVar = Variable.getVarValue(Schema.Entity.COMPANY.getName(), companyName);
        String departVar = Variable.getVarValue(Schema.Entity.DEPARTMENT.getName(), deptName);
        String companyDeptVar = Variable.getRelVarValue(Schema.Entity.DEPARTMENT.getName(),
            companyName, deptName);
        GraqlQuery graqlQuery = Graql.match(
            var(companyVar).isa(Schema.Entity.ENTITY.getName())
                .has(Schema.Attribute.NAME.getName(),companyName),
            var(departVar).isa(Schema.Entity.ENTITY.getName()).has(Schema.Attribute.NAME.getName
                (), deptName)
        ).insert(
            var(companyDeptVar).isa(type).rel(Schema.Relations
                .HAS_DEPARTMENT.getName(), var(departVar)).rel(Schema.Relations.DEPARTMENT_IN
                .getName(), var(companyVar))
        );
        return ((GraqlInsert) graqlQuery).statements();
      }

      @Override
      protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
          InterruptedException {
        super.reduce(key, values, context);
      }

      @Override
      public void run(Context context) throws IOException, InterruptedException {
        setup(context);
        int batch = 0;
        List<Statement>  querys = new ArrayList<>();
        while (context.nextKey()) {
          String key = context.getCurrentKey().toString();
          // don't care about value
          if ("NoDef".equals(key)) {
            errorCounter.increment(1L);
            continue;
          }
          jsonSuccessCounter.increment(1L);
          batch++;
          if (batch % 10000 == 0) {
             GraknClient.Transaction transaction =  session.transaction().write();
             transaction.execute(Graql.insert(querys));
             transaction.commit();
             querys.clear();
             batch = 0;
          } else {
            // 公司或者部门的名字
            if (source == 1 ) {
              querys.addAll(getName(Schema.Entity.COMPANY.getName(),key));
            } else  if (source == 2) {
              querys.addAll(getName(Schema.Entity.DEPARTMENT.getName(),key));
            } else if (source == 3) {
              // 公司部门关系
              JsonObject infoObject = new JsonObject(key);
              String companyName = infoObject.getString("normedName");
              String deptName = infoObject.getString("deptName");
              querys.addAll(getDeptRel(Schema.RelType.ENTITY_REL.getName(), companyName, deptName));
            }
          }
        }
        if (batch > 0) {
          GraknClient.Transaction transaction =  session.transaction().write();
          transaction.execute(Graql.insert(querys));
          transaction.commit();
        }
        cleanup(context);
      }
    }


    public void configJob(Job job, String input,String output, int reduceNum) throws Exception {
      job.setJarByClass(CompanyEntity4GraknMapred.class);
      job.setJobName("CompanyEntity4GraknMapred -" + input.substring(input.lastIndexOf("/") +
          1));
      job.setMapperClass(MyMap.class);
      FileInputFormat.setInputPaths(job, input);
      FileSystem fs = FileSystem.get(job.getConfiguration());
      Path outPath = new Path(output);
      fs.delete(outPath, true);
      FileOutputFormat.setOutputPath(job, outPath);
      job.setInputFormatClass(CombineTextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Text.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      job.setReducerClass(Reduce.class);
      job.setNumReduceTasks(reduceNum);
    }

    @SuppressWarnings("RegexpSinglelineJava")
    public int run(String[] args) throws Exception {
      if (args.length < 7) {
        System.err.println("Usage: CompanyEntity4GraknMapred <Input> <ConfDir> <KeySpace> " +
            "<GraknServer> <output> <Source> <reduce_num>");
        System.exit(1);
      }
      String input = args[0];
      String confDir = args[1];
      Configuration conf = new Configuration();
      conf.addResource(confDir + "/core-site.xml");
      conf.addResource(confDir + "/hdfs-site.xml");
      conf.addResource(confDir + "/hbase-site.xml");
      conf.addResource(confDir + "/yarn-site.xml");
      conf.set("key_space", args[2]);
      conf.set("GraknServer", args[3]);
      String  output = args[4];
      int reduceNum = Integer.parseInt(args[6]);
      conf.setLong("source", Long.parseLong(args[5]));
      conf.setInt("reduce_num", reduceNum);
      // java.lang.NoSuchMethodError: com.google.protobuf
      conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);
      conf.setLong("mapreduce.input.fileinputformat.split.maxsize", 256L * 1024 * 1024);
      conf.set("mapreduce.reduce.shuffle.memory.limit.percent", "0.25");
      System.setProperty("java.security.krb5.conf", confDir + "/krb5.conf");
      try {
        UserGroupInformation.loginUserFromKeytab("mindcube@WGQ.HIGGS.COM",
            confDir + "/krb5_mindcube.keytab");
      } catch (IOException e) {
        logger.info("key tab error:" + e.getMessage());
      }
      Job job = Job.getInstance(conf);
      configJob(job, input, output, reduceNum);
      job.waitForCompletion(true);
      return 0;
    }

    @SuppressWarnings("RegexpSinglelineJava")
    public static void main(String[] args) throws Exception {
      int exitCode = new CompanyEntity4GraknMapred().run(args);
      System.exit(exitCode);
    }
}
