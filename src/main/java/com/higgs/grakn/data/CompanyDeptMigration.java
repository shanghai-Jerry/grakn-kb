package com.higgs.grakn.data;

import com.higgs.grakn.DataMigration;
import com.higgs.grakn.HgraknClient;
import com.higgs.grakn.Input;
import com.higgs.grakn.Schema;
import com.higgs.grakn.Variable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import grakn.client.GraknClient;
import graql.lang.Graql;
import graql.lang.query.GraqlQuery;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;

import static com.higgs.grakn.FileUtils.readFiles;
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
public class CompanyDeptMigration extends DataMigration {

  static Logger logger = LoggerFactory.getLogger(CompanyDeptMigration.class);

  public static void main(String[] args) {
    if (args.length < 2) {
      System.err.println("Usage: CompanyDeptMigration <Input> <KeySpace>");
      System.exit(-1);
    }
    CompanyDeptMigration companyDeptMigration = new CompanyDeptMigration();
    String kbTest = args[0];
    String KEY_SPACE = args[1];
    HgraknClient hgraknClient = new HgraknClient(Variable.GRAKN_ADDRESS);
    GraknClient.Session session = hgraknClient.getClient().session(KEY_SPACE);
    Collection<Input> inputs = new ArrayList<>();
    String dir = companyDeptMigration.dirFormat(kbTest);
    // 公司名字
    inputs.add(new Input(dir) {
      @Override
      public GraqlQuery template(JsonObject data) {
        GraqlQuery graqlQuery = Graql.insert(
          var("company").isa(Schema.Entity.ENTITY_TYPE.getName()).has(Schema.Attribute.NAME.getName
              (), data.getString("normedName"))
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
          JsonObject jsonObject = new JsonObject(content);
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
    // 公司部门
    inputs.add(new Input(dir) {
      @Override
      public GraqlQuery template(JsonObject data) {
        GraqlQuery graqlQuery = Graql.insert(
            var("dept").isa(Schema.Entity.ENTITY_TYPE.getName()).has(Schema.Attribute.NAME.getName
                (), data.getString("deptName"))
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
          JsonObject jsonObject = new JsonObject(content);
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

    // 公司与部门之间的关系
    inputs.add(new Input(dir) {
      @Override
      public GraqlQuery template(JsonObject data) {
        GraqlQuery graqlQuery = Graql.match(
            var("company").isa(Schema.Entity.ENTITY_TYPE.getName())
                .has(Schema.Attribute.NAME.getName(), data.getString("normedName")),
            var("dept").isa(Schema.Entity.ENTITY_TYPE.getName()).has(Schema.Attribute.NAME.getName
                (), data.getString("deptName"))
        ).insert(
            var("company-dept").isa(Schema.RelType.COMPANY_DEPARTMENT.toString()).rel(Schema.Relations
                .HAS_DEPARTMENT.getName(), var("dept")).rel(Schema.Relations.DEPARTMENT_IN
                .getName(), var("company"))
        );
        return graqlQuery;
      }
      @Override
      public List<JsonObject> parseDataToJson() {
        List<JsonObject> items = new ArrayList<>();
        List<String> contents = new ArrayList<>();
        readFiles(this.getDataPath(), contents);
        for(String content : contents) {
          JsonObject jsonObject = new JsonObject(content);
          items.add(jsonObject);
        }
        contents.clear();
        return items;
      }
    });
    companyDeptMigration.connectAndMigrate(inputs, session);
  }
}
