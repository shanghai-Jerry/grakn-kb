package com.higgs.grakn;

import java.util.List;

import graql.lang.query.GraqlQuery;
import io.vertx.core.json.JsonObject;

/**
 * User: JerryYou
 *
 * Date: 2019-08-23
 *
 * Copyright (c) 2018 devops
 *
 * <<licensetext>>
 */
public abstract class Input {
  String path;

  public Input(String path) {
    this.path = path;
  }

  public String getDataPath() {
    return path;
  }

  // transform data to Graql
  public abstract GraqlQuery template(JsonObject data);
  // according data format, transform data to standard json format
  public abstract List<JsonObject> parseDataToJson();
}
