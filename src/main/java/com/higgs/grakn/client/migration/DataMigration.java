package com.higgs.grakn.client.migration;

import com.google.gson.stream.JsonReader;

import com.higgs.grakn.variable.Variable;
import com.higgs.grakn.client.HgraknClient;
import com.higgs.grakn.util.TimeUtil;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

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

  private HgraknClient hgraknClient = new HgraknClient(Variable.GRAKN_ADDRESS, Variable.PHONE_CALL_KEY_SPACE);

  private int batchSize = 10000;

  public int getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  public HgraknClient getHgraknClient() {
    return hgraknClient;
  }

  public void setHgraknClient(HgraknClient hgraknClient) {
    this.hgraknClient = hgraknClient;
  }
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
    for (Input input : inputs) {
      logger.info("Loading from [" + input.getDataPath() + "] into Grakn, started_at " +
          new Date().toString());
      loadDataIntoGrakn(input, 10000);
    }
    hgraknClient.close();
  }

  // commit with batchSize
  public void connectAndMigrate(Collection<Input> inputs) {
    for (Input input : inputs) {
      long startTime = System.currentTimeMillis();
      logger.info("Loading from [" + input.getDataPath() + "] into Grakn, started_at " +
          new Date().toString());
      long total = loadDataIntoGrakn(input, this.batchSize);
      long endTime = System.currentTimeMillis();
      long cost = (endTime - startTime) / 1000;
      logger.info("Inserted [ " + total +" ] items from [ " + input.getDataPath() + "] into " +
          "Grakn, ended_at " + new Date().toString() + ",cost:" + TimeUtil.costTime(cost) + "\n");
    }
  }

  long loadDataIntoGrakn(Input input, int batchSize) {
    List<JsonObject> data = input.parseDataToJson();
    int count = 0;
    int finished = 0;
    GraknClient.Session session = hgraknClient.NewSession(hgraknClient.getKeySpace());
    GraknClient.Transaction transaction = session.transaction().write();
    try {
      for (JsonObject item : data) {
        count++;
        finished++;
        GraqlQuery graqlInsertQuery = input.template(item);
        transaction.execute(graqlInsertQuery);
        if (count % batchSize == 0) {
          logger.info(Thread.currentThread().getName()+" - Executing Graql Insert: " +
              finished + "/" + data.size());
          transaction.commit();
          transaction = session.transaction().write();
          count = 0;
        }
      }
      if (count > 0) {
        // commit when last item finished
        transaction.commit();
      }
    } catch (Exception e) {

      logger.info("[commit error] => " + e.getMessage());
    } finally {
      session.close();
    }
    return data.size();
  }

  public Reader getReader(String relativePath) throws FileNotFoundException {
    try {
      return new InputStreamReader(new FileInputStream(relativePath),  "utf-8");
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
    return null;
  }

  Collection<Input> initialiseInputs(String dataDir) {
    Collection<Input> inputs = new ArrayList<>();
    int index = 0;
    dataDir = dirFormat(dataDir);
    // define template for constructing a company Graql insert Query
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
    // define template for constructing a person Graql insert Query
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
    // define template for constructing a contract Graql insert Query
    inputs.add(new Input(dataDir +"contracts") {
      @Override
      public GraqlQuery template(JsonObject contract) {
        List<String> sqls = new ArrayList<>();
        sqls.add("match $company isa company, has name " + stringFormatSql
            (contract.getString("company_name")));
        sqls.add(" $customer isa person, has phone-number " + stringFormatSql(contract
            .getString("person_id")));
        sqls.add(" insert (provider: $company, customer: $customer) isa contract;");
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
    // define template for constructing a call Graql insert Query
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


  public static void main(String[] args) {
    DataMigration dataMigration = new DataMigration();
    dataMigration.defaultConnectAndMigrate(dataMigration.initialiseInputs("/Users/devops/workspace/kb/phone_calls"));
  }
}
