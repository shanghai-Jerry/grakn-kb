package com.higgs.grakn.client.migration;

import com.higgs.grakn.client.schema.Schema;
import com.higgs.grakn.task.KbParseData;
import com.higgs.grakn.variable.Variable;

import java.util.ArrayList;
import java.util.List;

import graql.lang.Graql;
import graql.lang.query.GraqlQuery;
import io.vertx.core.json.JsonObject;

import static graql.lang.Graql.var;

/**
 * User: JerryYou
 *
 * Date: 2019-08-30
 *
 * Copyright (c) 2018 devops
 *
 * <<licensetext>>
 */
public class EntityInput extends Input {

  public EntityInput(String path, String inEntity) {
    super(path, inEntity);
  }
  @Override
  public GraqlQuery template(JsonObject data) {
    String name = data.getString("name");
    String type = data.getString("type");
    long code = data.getLong("id");
    return Graql.insert(
        var(Variable.getVarValue(type, name))
            .isa(type)
            .has(Schema.Attribute.NAME.getName(), name)
            .has(Schema.Attribute.CODE.getName(), code)
    );
  }

  @Override
  public List<JsonObject> parseDataToJson() {
    List<JsonObject> items = new ArrayList<>();
    KbParseData.parseEntity(items, this.inEntity, this.getDataPath());
    return items;
  }
}
