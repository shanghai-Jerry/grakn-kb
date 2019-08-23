package com.higgs.grakn;

import com.google.gson.stream.JsonReader;

import com.higgs.grakn.data.DataHandler;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import grakn.client.GraknClient;
import graql.lang.query.GraqlQuery;
import io.vertx.core.json.JsonObject;

import static graql.lang.Graql.parse;

/**
 * User: JerryYou
 *
 * Date: 2019-08-22
 *
 * Copyright (c) 2018 devops
 *
 * <<licensetext>>
 */
public class DataMigration {

  static Logger logger = LoggerFactory.getLogger(DataMigration.class);

  private int numThread = 20;

  private AtomicLong counter = new AtomicLong(0);
  ExecutorService executor = Executors.newFixedThreadPool(numThread);
  ExecutorCompletionService executorCompletionService = new ExecutorCompletionService(executor);

  public String stringFormatSql(String var) {
    return "\"" + var + "\"";
  }

  public String dirFormat(String dataDir) {
    if (!dataDir.endsWith("/")) {
      dataDir = dataDir + "/";
    }
    return dataDir;
  }

  public void defaultConnectAndMigrate(Collection<Input> inputs) {
    HgraknClient hgraknClient = new HgraknClient(Variable.GRAKN_ADDRESS);
    GraknClient.Session session = hgraknClient.getClient().session(Variable.PHONE_CALL_KEY_SPACE);
    for (Input input : inputs) {
      logger.info("Loading from [" + input.getDataPath() + "] into Grakn, started_at " +
          new Date().toString());
      loadDataIntoGrakn(input, session);
    }

    session.close();
    hgraknClient.close();
  }

  public void connectAndMigrate(Collection<Input> inputs, GraknClient.Session session) {
    for (Input input : inputs) {
      logger.info("Loading from [" + input.getDataPath() + "] into Grakn, started_at " +
          new Date().toString());
      loadDataIntoGraknThread(input, session);
      long ret = 0;
      for (int i = 0; i < this.numThread; i++) {
        try {
          executorCompletionService.take();
          logger.info("Loading from [" + input.getDataPath() + "] into Grakn, finished: " + ret);
        } catch (Exception e) {
          logger.info("[ExecutorCompletionService Future Get error] -> " + e.getMessage());
        }
      }
      logger.info("Inserted "+ ret + " items from [ "
          + input.getDataPath() + "]" + " into Grakn, ended_at "+ new Date().toString() +"\n");


    }

    session.close();
  }

  void loadDataIntoGraknThread(Input input, GraknClient.Session session) {
    List<JsonObject> items = input.parseDataToJson();
    int totalLength = items.size();
    logger.info("Loading from [" + input.getDataPath() + "] into Grakn, totalSize:" + totalLength);
    int batchSize = totalLength / this.numThread;
    for (int i = 0; i < this.numThread; i++) {
      if (i == this.numThread - 1) {
        executorCompletionService.submit(new DataHandler(input, session, items.subList(i * batchSize,totalLength)));
      } else {
        executorCompletionService.submit(new DataHandler(input, session, items.subList(i * batchSize, (i+1)
            *batchSize)));
      }
    }
  }

  void loadDataIntoGrakn(Input input, GraknClient.Session session) {
    List<JsonObject> items = input.parseDataToJson();
    int count = 0;
    for (JsonObject item : items) {
      count++;
      GraknClient.Transaction transaction = session.transaction().write();
      GraqlQuery graqlInsertQuery = input.template(item);
      //
      transaction.execute(graqlInsertQuery);
      transaction.commit();
      if (count % 1000 == 0) {
        logger.info("Executing Graql Insert : " + count + "/" + items.size());
      }
    }
    logger.info("\nInserted " + items.size() + " items from [ " + input.getDataPath() + "]" +
        " into Grakn, ended_at "+ new Date().toString() +"\n");
    items.clear();
  }

  public  Reader getReader(String relativePath) throws FileNotFoundException {
    return new InputStreamReader(new FileInputStream(relativePath));
  }

  // 自定义的数据格式
  public Collection<Input> customDataFormatInput(String dir){
      Collection<Input> inputs = new ArrayList<>();
      dir = dirFormat(dir);
      inputs.add(new Input("/path/file") {
        @Override
        public GraqlQuery template(JsonObject call) {
          // match caller
          return null;
        }

        @Override
        public List<JsonObject> parseDataToJson() {
          List<JsonObject> items = new ArrayList<>();
          List<String> contents = new ArrayList<>();
          FileUtils.readFiles(this.getDataPath(), contents);
          for(String content : contents) {
            items.add(new JsonObject(content));
          }
          return items;
        }
      });
      return inputs;
  }

  Collection<Input> initialiseInputs(String dataDir) {
    Collection<Input> inputs = new ArrayList<>();
    int index = 0;
    dataDir = dirFormat(dataDir);
    // define template for constructing a company Graql insert query
    inputs.add(new Input(dataDir + "companies") {
      @Override
      public GraqlQuery template(JsonObject company) {
        return parse("insert $company isa company, has name " + stringFormatSql(company
            .getString("name")) + ";");
      }

      @Override
      public List<JsonObject> parseDataToJson() {
        List<JsonObject> items = new ArrayList<>();
        JsonReader jsonReader = null; // 1
        try {
          jsonReader = new JsonReader(getReader(this.getDataPath() + ".json"));
        } catch (FileNotFoundException e) {
          e.printStackTrace();
        }
        try {
          jsonReader.beginArray();
          while (jsonReader.hasNext()) {
            jsonReader.beginObject();
            JsonObject item = new JsonObject();
            while (jsonReader.hasNext()) {
              String key = jsonReader.nextName();
              switch (jsonReader.peek()) {
                case STRING:
                  item.put(key, jsonReader.nextString()); // 2
                  break;
                case NUMBER:
                  item.put(key, jsonReader.nextInt()); // 2
                  break;
              }
            }
            jsonReader.endObject();
            items.add(item); // 3
          }
          jsonReader.endArray();
        } catch (IOException e) {
          e.printStackTrace();
        }
        return items;
      }
    });
    // define template for constructing a person Graql insert query
    inputs.add(new Input(dataDir + "people") {
      @Override
      public GraqlQuery template(JsonObject person) {
        // insert person
        String graqlInsertQuery = "insert $person isa person, has phone-number " +
            stringFormatSql(person.getString("phone_number"));

        if (! person.containsKey("first_name")) {
          // person is not a customer
          graqlInsertQuery += ", has is-customer false";
        } else {
          // person is a customer
          graqlInsertQuery += ", has is-customer true";
          graqlInsertQuery += ", has first-name " + stringFormatSql(person.getString("first_name"));
          graqlInsertQuery += ", has last-name " + stringFormatSql(person.getString("last_name"));
          graqlInsertQuery += ", has city " + stringFormatSql(person.getString("city"));
          graqlInsertQuery += ", has age " + person.getInteger("age");
        }

        graqlInsertQuery += ";";
        return parse(graqlInsertQuery);
      }

      @Override
      public List<JsonObject> parseDataToJson() {
        List<JsonObject> items = new ArrayList<>();
        JsonReader jsonReader = null; // 1
        try {
          jsonReader = new JsonReader(getReader(this.getDataPath() + ".json"));
          jsonReader.beginArray();
          while (jsonReader.hasNext()) {
            jsonReader.beginObject();
            JsonObject item = new JsonObject();
            while (jsonReader.hasNext()) {
              String key = jsonReader.nextName();
              switch (jsonReader.peek()) {
                case STRING:
                  item.put(key, jsonReader.nextString()); // 2
                  break;
                case NUMBER:
                  item.put(key, jsonReader.nextInt()); // 2
                  break;
              }
            }
            jsonReader.endObject();
            items.add(item); // 3
          }
          jsonReader.endArray();
        } catch (IOException e) {
          e.printStackTrace();
        }

        return items;
      }
    });
    // define template for constructing a contract Graql insert query
    inputs.add(new Input(dataDir +"contracts") {
      @Override
      public GraqlQuery template(JsonObject contract) {
        List<String> sqls = new ArrayList<>();
        sqls.add("match $company isa company, has name " + stringFormatSql
            (contract.getString("company_name")));
        sqls.add(" $customer isa person, has phone-number " + stringFormatSql(contract
            .getString("person_id")));
        sqls.add(" insert (provider: $company, customer: $customer) isa contract");
        return parse(StringUtils.join(sqls, ";"));
      }

      @Override
      public List<JsonObject> parseDataToJson() {
        List<JsonObject> items = new ArrayList<>();
        try {
          JsonReader jsonReader = new JsonReader(getReader(this.getDataPath() + ".json")); // 1
          jsonReader.beginArray();
          while (jsonReader.hasNext()) {
            jsonReader.beginObject();
            JsonObject item = new JsonObject();
            while (jsonReader.hasNext()) {
              String key = jsonReader.nextName();
              switch (jsonReader.peek()) {
                case STRING:
                  item.put(key, jsonReader.nextString()); // 2
                  break;
                case NUMBER:
                  item.put(key, jsonReader.nextInt()); // 2
                  break;
              }
            }
            jsonReader.endObject();
            items.add(item); // 3
          }
          jsonReader.endArray();
        } catch (IOException e) {
          e.printStackTrace();
        }
        return items;
      }
    });
    // define template for constructing a call Graql insert query
    inputs.add(new Input(dataDir +"calls") {
      @Override
      public GraqlQuery template(JsonObject call) {
        // match caller
        String graqlInsertQuery = "match $caller isa person, has phone-number " + stringFormatSql
            (call.getString("caller_id")) + ";";
        // match callee
        graqlInsertQuery += " $callee isa person, has phone-number " + stringFormatSql(call
            .getString("callee_id")) + ";";
        // insert call
        graqlInsertQuery += " insert $call(caller: $caller, callee: $callee) isa call;" +
            " $call has started-at " + call.getString("started_at") + ";" +
            " $call has duration " + call.getInteger("duration") + ";";
        return parse(graqlInsertQuery);
      }

      @Override
      public List<JsonObject> parseDataToJson() {
        List<JsonObject> items = new ArrayList<>();
        try {
          JsonReader jsonReader = new JsonReader(getReader(this.getDataPath() + ".json")); // 1
          jsonReader.beginArray();
          while (jsonReader.hasNext()) {
            jsonReader.beginObject();
            JsonObject item = new JsonObject();
            while (jsonReader.hasNext()) {
              String key = jsonReader.nextName();
              switch (jsonReader.peek()) {
                case STRING:
                  item.put(key, jsonReader.nextString()); // 2
                  break;
                case NUMBER:
                  item.put(key, jsonReader.nextInt()); // 2
                  break;
              }
            }
            jsonReader.endObject();
            items.add(item); // 3
          }
          jsonReader.endArray();
        } catch (IOException e) {
          e.printStackTrace();
        }
        return items;
      }
    });
    return inputs;
  }
}
