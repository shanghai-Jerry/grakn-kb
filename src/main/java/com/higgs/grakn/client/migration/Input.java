package com.higgs.grakn.client.migration;

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

  String inEntity;

  String outEntity;

  String inRel;

  String outRel;

  String relType;

  String attributeType;

  // Relation
  public Input(String path, String inEntity, String outEntity, String inRel, String outRel, String relType) {
    this.path = path;
    this.inEntity = inEntity;
    this.outEntity = outEntity;
    this.inRel = inRel;
    this.outRel = outRel;
    this.relType = relType;
  }
  // Attribute
  public Input(String path, String inEntity, String attributeType) {
    this.path = path;
    this.inEntity = inEntity;
    this.attributeType = attributeType;
  }
  // entity
  public Input(String path, String inEntity) {
    this.path = path;
    this.inEntity = inEntity;
  }

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
