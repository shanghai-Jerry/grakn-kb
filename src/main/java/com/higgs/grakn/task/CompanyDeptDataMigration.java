package com.higgs.grakn.task;

import com.higgs.grakn.client.migration.DataMigration;
import com.higgs.grakn.client.HgraknClient;
import com.higgs.grakn.client.migration.Input;
import com.higgs.grakn.client.schema.Schema;
import com.higgs.grakn.util.TimeUtil;
import com.higgs.grakn.variable.Variable;
import com.higgs.grakn.callable.DataHandlerCallable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import graql.lang.Graql;
import graql.lang.query.GraqlQuery;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;

import static com.higgs.grakn.util.FileUtils.readFiles;
import static graql.lang.Graql.var;

/**
 * User: JerryYou
 *
 * Date: 2019-08-23
 *
 * Copyright (c) 2018 devops
 *
 * <<licensetext>>
 */
public class CompanyDeptDataMigration extends DataMigration  {

  static Logger logger = LoggerFactory.getLogger(CompanyDeptDataMigration.class);
  // numThread should only be 1, cause GraknClient.Session with transaction is not thread safe
  public int numThread = 1;
  ExecutorService executor;
  ExecutorCompletionService executorCompletionService;

  public CompanyDeptDataMigration(int numThread) {
    if (numThread > 1 || numThread == 0) {
      System.err.println("numThread should be 1, not thread safe operation");
      System.exit(-1);
    }
    this.numThread = numThread;
    executor = Executors.newFixedThreadPool(this.numThread);
    executorCompletionService = new ExecutorCompletionService(executor);
  }

  public CompanyDeptDataMigration() {
    executor = Executors.newFixedThreadPool(this.numThread);
    executorCompletionService = new ExecutorCompletionService(executor);
  }

  // 多线程任务submit
  void loadDataIntoGraknThread(Input input) {
    List<JsonObject> items = input.parseDataToJson();
    int totalLength = items.size();
    logger.info("Loading from [" + input.getDataPath() + "] into Grakn, totalSize:" + totalLength);
    int batchSize = totalLength / this.numThread;
    for (int i = 0; i < this.numThread; i++) {
      if (i == this.numThread - 1) {
        executorCompletionService.submit(new DataHandlerCallable(input, getHgraknClient(),
            items.subList(i * batchSize, totalLength)));
      } else {
        executorCompletionService.submit(new DataHandlerCallable(input, getHgraknClient(),
            items.subList(i * batchSize, (i+1) *batchSize)));
      }
    }
  }

  @Override
  public void connectAndMigrate(Collection<Input> inputs) {
    super.connectAndMigrate(inputs);
  }

  public void connectAndMigrateWithMultiThread(Collection<Input> inputs) {
    for (Input input : inputs) {
      long startTime = System.currentTimeMillis();
      logger.info("Loading from [" + input.getDataPath() + "] into Grakn, started_at " +
          new Date().toString());

      loadDataIntoGraknThread(input);

      for (int i = 0; i < this.numThread; i++) {
        try {
          executorCompletionService.take();
        } catch (Exception e) {
          logger.info("[ExecutorCompletionService Future Get error] -> " + e.getMessage());
        }
      }
      long endTime = System.currentTimeMillis();
      long cost = (endTime - startTime) / 1000;
      logger.info("Inserted items from [ "
          + input.getDataPath() + "]" + " into Grakn, ended_at "+ new Date().toString() +"\n" +
          ",cost:" + TimeUtil.costTime(cost));

    }
  }
  private JsonObject content2Json(String info) {
    JsonObject infoObject = new JsonObject();
    int index = info.indexOf("\t");
    String json = info.substring(0, index);
    try {
      infoObject = new JsonObject(json);
    } catch (Exception e) {
      logger.info("content to json error");
    }
    return infoObject;
  }

  public static void main(String[] args) {
    if (args.length < 5) {
      System.err.println("Usage: CompanyDeptDataMigration " +
          "<CompanyInput>" +
          " <DeptInput> " +
          "<Compaany-Dept Input> " +
          "<KeySpace>" +
          "<numThread>"
      );
      System.exit(-1);
    }
    String companyInput = Variable.dirFormat(args[0]);
    String deptInput = Variable.dirFormat(args[1]);
    String companyDeptInput = Variable.dirFormat(args[2]);
    String KEY_SPACE = args[3];
    int nums = Integer.parseInt(args[4]);

    // init variable
    CompanyDeptDataMigration companyDeptMigration = new CompanyDeptDataMigration(nums);
    HgraknClient hgraknClient = new HgraknClient(Variable.GRAKN_ADDRESS, KEY_SPACE);
    companyDeptMigration.setHgraknClient(hgraknClient);
    companyDeptMigration.setBatchSize(10000);

    Collection<Input> inputs = new ArrayList<>();
    // 公司名字
    if (companyInput.startsWith("/")) {
      inputs.add(new Input(companyInput) {
        @Override
        public GraqlQuery template(JsonObject data) {
          String name = data.getString("normedName");
          GraqlQuery graqlQuery = Graql.insert(
              var(Variable.getVarValue(Schema.Entity.COMPANY.getName(), name))
                  .isa(Schema.Entity.ENTITY_TYPE.getName())
                  .has(Schema.Attribute.NAME.getName(), name)
          );
          return graqlQuery;
        }
        @Override
        public List<JsonObject> parseDataToJson() {
          List<JsonObject> items = new ArrayList<>();
          List<String> contents = new ArrayList<>();
          readFiles(this.getDataPath(), contents);
          Map<String, Boolean> distinct = new HashMap<>();
          for(String content : contents) {
            JsonObject jsonObject = companyDeptMigration.content2Json(content);
            String company = jsonObject.getString("normedName", "nodef");
            if (distinct.containsKey(company)) {
              continue;
            }
            distinct.put(company, true);
            items.add(jsonObject);
          }
          contents.clear();
          return items;
        }
      });
    }
    if (deptInput.startsWith("/")) {
      // 公司部门
      inputs.add(new Input(deptInput) {
        @Override
        public GraqlQuery template(JsonObject data) {
          String name = data.getString("deptName");
          GraqlQuery graqlQuery = Graql.insert(
              var(Variable.getVarValue(Schema.Entity.DEPARTMENT.getName(),Variable.getMD5(name))).isa
                  (Schema.Entity.ENTITY_TYPE.getName())
                  .has(Schema.Attribute.NAME.getName(), name)
          );
          return graqlQuery;
        }
        @Override
        public List<JsonObject> parseDataToJson() {
          List<JsonObject> items = new ArrayList<>();
          List<String> contents = new ArrayList<>();
          readFiles(this.getDataPath(), contents);
          Map<String, Boolean> distinct = new HashMap<>();
          for(String content : contents) {
            JsonObject jsonObject = companyDeptMigration.content2Json(content);
            String dept = jsonObject.getString("deptName", "nodef");
            if (distinct.containsKey(dept)) {
              continue;
            }
            distinct.put(dept, true);
            items.add(jsonObject);
          }
          contents.clear();
          return items;
        }
      });
    }
    if (companyDeptInput.startsWith("/")) {
      // 公司与部门之间的关系
      inputs.add(new Input(companyDeptInput) {
        @Override
        public GraqlQuery template(JsonObject data) {
          String companyName =  data.getString("normedName");
          String deptName =data.getString("deptName");
          String companyVar = Variable.getVarValue(Schema.Entity.COMPANY.getName(), companyName);
          String departVar = Variable.getVarValue(Schema.Entity.DEPARTMENT.getName(), deptName);
          String companyDeptVar = Variable.getRelVarValue(Schema.Entity.DEPARTMENT.getName(),
              companyName, deptName);
          GraqlQuery graqlQuery = Graql.match(
              var(companyVar).isa(Schema.Entity.ENTITY_TYPE.getName())
                  .has(Schema.Attribute.NAME.getName(),companyName),
              var(departVar).isa(Schema.Entity.ENTITY_TYPE.getName()).has(Schema.Attribute.NAME.getName
                  (), deptName)
          ).insert(
              var(companyDeptVar).rel(Schema.Relations
                  .HAS_DEPARTMENT.getName(), var(departVar)).rel(Schema.Relations.DEPARTMENT_IN
                  .getName(), var(companyVar)).isa(Schema.RelType.COMPANY_DEPARTMENT.toString())
          );
          return graqlQuery;
        }
        @Override
        public List<JsonObject> parseDataToJson() {
          List<JsonObject> items = new ArrayList<>();
          List<String> contents = new ArrayList<>();
          readFiles(this.getDataPath(), contents);
          for(String content : contents) {
            JsonObject jsonObject = companyDeptMigration.content2Json(content);
            items.add(jsonObject);
          }
          contents.clear();
          return items;
        }
      });
    }

    companyDeptMigration.connectAndMigrate(inputs);
    hgraknClient.close();
    logger.info("main func finished!!!");
    System.exit(0);
  }
}
