package com.higgs.grakn.task;

import com.csvreader.CsvReader;
import com.higgs.grakn.client.HgraknClient;
import com.higgs.grakn.client.migration.DataMigration;
import com.higgs.grakn.client.migration.Input;
import com.higgs.grakn.client.schema.Schema;
import com.higgs.grakn.variable.Variable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import graql.lang.Graql;
import graql.lang.query.GraqlQuery;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import static graql.lang.Graql.var;

/**
 * User: JerryYou
 *
 * Date: 2019-08-27
 *
 * Copyright (c) 2018 devops
 *
 * <<licensetext>>
 */
public class KbDataMigration extends DataMigration {
  static Logger logger = LoggerFactory.getLogger(KbDataMigration.class);

  public static void main(String[] args) {

    if (args.length < 5) {
      System.err.println("Usage: KbDataMigration " +
          "<EntityName Input>" +
          "<EntityAttribute Input> " +
          "<EntityRelation Input> " +
          "<KeySpace>" +
          "<GraknServer>"
      );
      System.exit(-1);
    }
    String name = Variable.dirFormat(args[0]);
    String attribute = Variable.dirFormat(args[1]);
    String relation = Variable.dirFormat(args[2]);
    String KEY_SPACE = args[3];
    String graknServer = args[4];
    // init variable
    KbDataMigration kbDataMigration = new KbDataMigration();
    HgraknClient hgraknClient = new HgraknClient(graknServer, KEY_SPACE);
    kbDataMigration.setHgraknClient(hgraknClient);
    kbDataMigration.setBatchSize(10000);
    Collection<Input> inputs = new ArrayList<>();
    inputs.add(new Input(name) {
      @Override
      public GraqlQuery template(JsonObject data) {
        String name = data.getString("name");
        return Graql.insert(
          var(Variable.getVarValue(Schema.Entity.ENTITY_TYPE.getName(), name))
              .isa(Schema.Entity.ENTITY_TYPE.getName())
              .has(Schema.Attribute.NAME.getName(), name)
        );
      }

      @Override
      public List<JsonObject> parseDataToJson() {
        List<JsonObject> items = new ArrayList<>();
        try {
          CsvReader csvReader = new CsvReader(kbDataMigration.getReader(this.getDataPath()));
          // csvReader.readHeaders();
          while(csvReader.readRecord()) {
            String [] values = csvReader.getValues();
            if (values.length != 2) {
              continue;
            }
            JsonObject json = new JsonObject();
            json.put("id", values[0]).put("name", values[1]);
            items.add(json);
          }
        } catch (FileNotFoundException e) {
          logger.info("[NoFile] =>" + e.getMessage());
        } catch (IOException e) {
          logger.info("[IOExp] =>" + e.getMessage());
        }
        return items;
      }
    });
    inputs.add(new Input(attribute) {
      @Override
      public GraqlQuery template(JsonObject data) {
        String name = data.getString("name");
        String type = data.getString("attribute_type");
        String value = data.getString("attribute_value");
        String var = Variable.getVarValue(Schema.Entity.ENTITY_TYPE.getName(), name);
        return Graql.match(
            var(var)
                .isa(Schema.Entity.ENTITY_TYPE.getName())
                .has(Schema.Attribute.NAME.getName(), name)
        ).insert(
            var(var).has(type, value)
        );
      }

      @Override
      public List<JsonObject> parseDataToJson() {
        List<JsonObject> items = new ArrayList<>();
        KbParseData.parseAttribute(items, this.getDataPath());
        return items;
      }
    });
    inputs.add(new Input(relation) {
      @Override
      public GraqlQuery template(JsonObject data) {
        return null;
      }

      @Override
      public List<JsonObject> parseDataToJson() {
        return new ArrayList<>();
      }
    });

    kbDataMigration.connectAndMigrate(inputs);

  }

}
