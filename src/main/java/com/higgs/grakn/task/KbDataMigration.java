package com.higgs.grakn.task;

import com.higgs.grakn.client.HgraknClient;
import com.higgs.grakn.client.migration.DataMigration;
import com.higgs.grakn.client.migration.Input;
import com.higgs.grakn.client.migration.RelationInput;
import com.higgs.grakn.client.schema.Schema;
import com.higgs.grakn.variable.Variable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

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

    if (args.length < 3) {
      System.err.println("Usage: KbDataMigration " +
          "<InputDir>" +
          "<KeySpace>" +
          "<GraknServer>"
      );
      System.exit(-1);
    }
    String dir = Variable.dirFormat(args[0], true);
    String KEY_SPACE = args[1];
    String graknServer = args[2];
    // init variable
    KbDataMigration kbDataMigration = new KbDataMigration();
    HgraknClient hgraknClient = new HgraknClient(graknServer, KEY_SPACE);
    kbDataMigration.setHgraknClient(hgraknClient);
    kbDataMigration.setBatchSize(10000);
    Collection<Input> inputs = new ArrayList<>();
    /*
    // kb_entity.csv
    inputs.add(new Input(dir + "kb_entity.csv") {
      @Override
      public GraqlQuery template(JsonObject data) {
        String name = data.getString("name");
        return Graql.insert(
          var(Variable.getVarValue(Schema.Entity.ENTITY.getName(), name))
              .isa(Schema.Entity.ENTITY.getName())
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

    List<Input> attributeInputs = Arrays.asList(
        new AttributeInput(dir + "corp_type.csv", Schema.Attribute.CORP_TYPE.getName()),
        new AttributeInput(dir + "cert_code.csv", Schema.Attribute.CERT_CODE.getName()),
        new AttributeInput(dir + "city_type.csv", Schema.Attribute.CITY_TYPE.getName()),
        new AttributeInput(dir + "corp_alias.csv", Schema.Attribute.CORP_ALIAS.getName()),
        new AttributeInput(dir + "corp_eng_name.csv", Schema.Attribute.CORP_ENG_NAME.getName()),
        new AttributeInput(dir + "ind_code.csv", Schema.Attribute.IND_CODE.getName()),
        new AttributeInput(dir + "loc_city_code.csv", Schema.Attribute.LOC_CITY_CODE.getName()),
        new AttributeInput(dir + "loc_code.csv", Schema.Attribute.LOC_CODE.getName()),
        new AttributeInput(dir + "major_code.csv", Schema.Attribute.MAJOR_CODE.getName()),
        new AttributeInput(dir + "school_code.csv", Schema.Attribute.SCHOOL_CODE.getName()),
        new AttributeInput(dir + "school_type.csv", Schema.Attribute.SCHOOL_TYPE.getName()),
        new AttributeInput(dir + "entity-type-format.csv", Schema.Attribute.ENTITY_TYPE.getName())
    );
    inputs.addAll(attributeInputs);

    // 公司 - 公司类型之间的关系
    inputs.add(new Input(dir + "corp_type.csv") {
      @Override
      public GraqlQuery template(JsonObject data) {
        String in_value = data.getString("in_value");
        String out_value = data.getString("out_value");
        String in = data.getString("in");
        String out = data.getString("out");
        String inVar = Variable.getVarValue(Schema.Entity.ENTITY.getName(), in_value);
        String outVar = Variable.getVarValue(Schema.Entity.ENTITY.getName(), out_value);
        String relVar = Variable.getRelVarValue(Schema.RelType.COMPANY_CORP_TYPE.getName(), in_value,
            out_value);
        return  Graql.match(
            var(inVar).isa(Schema.Entity.ENTITY.getName())
            .has(Schema.Attribute.NAME.getName(), in_value),
            var(outVar).isa(Schema.Entity.ENTITY.getName())
                .has(Schema.Attribute.NAME.getName(), out_value)
        ).insert(
          var(relVar).isa(Schema.RelType.COMPANY_CORP_TYPE.getName())
              .rel(in, var(inVar))
              .rel(out, var(outVar))
        );
      }

      @Override
      public List<JsonObject> parseDataToJson() {
        List<JsonObject> items = new ArrayList<>();
        KbParseData.parseRelationsInAttribute(items, Schema.Relations.CORPTYPE_COMPANY.getName(),
            Schema.Relations.COMPANY_CORPTYPE.getName(),this.getDataPath());
        return items;
      }
    });

    // 学校 - 学校类型之间的关系
    inputs.add(new Input(dir + "school_type.csv") {
      @Override
      public GraqlQuery template(JsonObject data) {
        String in_value = data.getString("in_value");
        String out_value = data.getString("out_value");
        String in = data.getString("in");
        String out = data.getString("out");
        String inVar = Variable.getVarValue(Schema.Entity.ENTITY.getName(), in_value);
        String outVar = Variable.getVarValue(Schema.Entity.SCHOOL_TYPE_ENTITY.getName(), out_value);
        String relVar = Variable.getRelVarValue(Schema.RelType.SCHOOL_SCHOOL_TYPE.getName(), in_value,
            out_value);
        return  Graql.match(
            var(inVar).isa(Schema.Entity.ENTITY.getName())
                .has(Schema.Attribute.NAME.getName(), in_value),
            var(outVar).isa(Schema.Entity.SCHOOL_TYPE_ENTITY.getName())
                .has(Schema.Attribute.NAME.getName(), out_value)
        ).insert(
            var(relVar).isa(Schema.RelType.SCHOOL_SCHOOL_TYPE.getName())
                .rel(in, var(inVar))
                .rel(out, var(outVar))
        );
      }

      @Override
      public List<JsonObject> parseDataToJson() {
        List<JsonObject> items = new ArrayList<>();
        KbParseData.parseRelationsInAttribute(items, Schema.Relations.SCHOOLTYPE_SCHOOL.getName(),
            Schema.Relations.SCHOOL_SCHOOLTYPE.getName(),this.getDataPath());
        return items;
      }
    });

    // 实体 - 实体类型型之间的关系
    inputs.add(new Input(dir + "entity-type-format.csv") {
      @Override
      public GraqlQuery template(JsonObject data) {
        String in_value = data.getString("in_value");
        String out_value = data.getString("out_value");
        String in = data.getString("in");
        String out = data.getString("out");
        String inVar = Variable.getVarValue(Schema.Entity.ENTITY.getName(), in_value);
        String outVar = Variable.getVarValue(Schema.Entity.ENTITY_TYPE_ENTITY.getName(), out_value);
        String relVar = Variable.getRelVarValue(Schema.RelType.ENTITY_ENTITY_TYPE.getName(), in_value,
            out_value);
        return  Graql.match(
            var(inVar).isa(Schema.Entity.ENTITY.getName())
                .has(Schema.Attribute.NAME.getName(), in_value),
            var(outVar).isa(Schema.Entity.ENTITY_TYPE_ENTITY.getName())
                .has(Schema.Attribute.NAME.getName(), out_value)
        ).insert(
            var(relVar).isa(Schema.RelType.ENTITY_ENTITY_TYPE.getName())
                .rel(in, var(inVar))
                .rel(out, var(outVar))
        );
      }

      @Override
      public List<JsonObject> parseDataToJson() {
        List<JsonObject> items = new ArrayList<>();
        KbParseData.parseRelationsInAttribute(items, Schema.Relations.ENTITY_TYPE_ENTITY.getName(),
            Schema.Relations.ENTITY_ENTITY_TYPE.getName(),this.getDataPath());
        return items;
      }
    });
    */
    List<RelationInput> relationInputs = new ArrayList<>();

    for (int i = 0; i <= 42; i++) {
      relationInputs.add(new RelationInput(dir + "relation_" + String.valueOf(i) + ".csv",
          Schema.Entity.ENTITY.getName(), Schema.Entity.ENTITY.getName(),
          Variable.relationPairs.get(i).getInRel(),
          Variable.relationPairs.get(i).getOutRel(),
          Schema.RelType.ENTITY_REL.getName()));
    }

    inputs.addAll(relationInputs);

    kbDataMigration.connectAndMigrate(inputs);

    logger.info("main Insert finished!!");

  }

}
