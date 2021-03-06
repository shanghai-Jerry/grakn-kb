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
 * Date: 2019-08-29
 *
 * Copyright (c) 2018 devops
 *
 * <<licensetext>>
 */
public class AttributeInput extends Input {

  public AttributeInput(String path, String inEntity, String attributeType) {
    super(path, inEntity, attributeType);
  }

  public AttributeInput(String path, String attributeType) {
    super(path);
    this.attributeType = attributeType;
    this.inEntity = Schema.Entity.ENTITY.getName();
  }

  @Override
  public GraqlQuery template(JsonObject data) {
    String name = data.getString("name");
    String value = data.getString("attribute_value");
    String var = Variable.getVarValue(this.inEntity, name);
    return Graql.match(
        var(var)
            .isa(this.inEntity)
            .has(Schema.Attribute.NAME.getName(), name)
    ).insert(
        var(var).has(this.attributeType, value)
    );
  }

  @Override
  public List<JsonObject> parseDataToJson() {
    List<JsonObject> items = new ArrayList<>();
    KbParseData.parseAttribute(items, this.getDataPath());
    return items;
  }
}
