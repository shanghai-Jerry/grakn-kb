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
public class RelationInput extends Input {

  public RelationInput(String path, String inEntity, String outEntity, String inRel, String
      outRel, String relType) {
    super(path, inEntity, outEntity, inRel, outRel, relType);
  }

  public RelationInput(String path) {
    super(path);
  }

  @Override
  public GraqlQuery template(JsonObject data) {
    String in_value = data.getString("in_value");
    String out_value = data.getString("out_value");
    String in = data.getString("in");
    String out = data.getString("out");
    String relType = data.getString("rel_type");
    String inVar = Variable.getVarValue(this.inEntity, in_value);
    String outVar = Variable.getVarValue(this.outEntity, out_value);
    String relVar = Variable.getRelVarValue(this.relType, in_value,
        out_value);
    return  Graql.match(
        var(inVar).isa(this.inEntity)
            .has(Schema.Attribute.NAME.getName(), in_value),
        var(outVar).isa(this.outEntity)
            .has(Schema.Attribute.NAME.getName(), out_value)
    ).insert(
        var(relVar)
            .isa(relType)
            .rel(in, var(inVar))
            .rel(out, var(outVar))
    );
  }

  @Override
  public List<JsonObject> parseDataToJson() {
    List<JsonObject> items = new ArrayList<>();
    KbParseData.parseRelations(items, this.getDataPath());
    return items;
  }
}
