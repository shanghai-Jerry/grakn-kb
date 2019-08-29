package com.higgs.grakn.task;

import com.csvreader.CsvReader;
import com.higgs.grakn.client.schema.Schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.List;

import io.vertx.core.json.JsonObject;

/**
 * User: JerryYou
 *
 * Date: 2019-08-28
 *
 * Copyright (c) 2018 devops
 *
 * <<licensetext>>
 */
public class KbParseData {

  static Logger logger = LoggerFactory.getLogger(KbParseData.class);

  static public Reader getReader(String relativePath) throws FileNotFoundException {
    try {
      return new InputStreamReader(new FileInputStream(relativePath),  "utf-8");
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
    return null;
  }

  static  public void parseAttribute(List<JsonObject> items, String attributeType, String path) {
    try {
      CsvReader csvReader = new CsvReader(getReader(path));
      while(csvReader.readRecord()) {
        String [] values = csvReader.getValues();
        if (values.length != 3) {
          continue;
        }
        JsonObject json = new JsonObject();
        json.put("name", values[0])
            .put("attribute_type", attributeType)
            .put("attribute_value", values[2])
        ;
        items.add(json);
      }
    } catch (FileNotFoundException e) {
      logger.info("[NoFile] =>" + e.getMessage());
    } catch (IOException e) {
      logger.info("[IOExp] =>" + e.getMessage());
    }
  }

  static  public void parseRelations(List<JsonObject> items, String inRel, String outRel,
                                     String relType, String path) {
    try {
      CsvReader csvReader = new CsvReader(getReader(path));
      while(csvReader.readRecord()) {
        String [] values = csvReader.getValues();
        if (values.length != 4) {
          continue;
        }
        JsonObject json = new JsonObject();
        json.put("in_value", values[0])
            .put("out_value", values[1])
            .put("in", inRel)
            .put("out", outRel)
            .put("rel_type",relType)
        ;
        items.add(json);
      }
    } catch (FileNotFoundException e) {
      logger.info("[NoFile] =>" + e.getMessage());
    } catch (IOException e) {
      logger.info("[IOExp] =>" + e.getMessage());
    }
  }

  static  public void parseCorpTypeRelationsInAttribute(List<JsonObject> items, String path) {
    try {
      CsvReader csvReader = new CsvReader(getReader(path));
      while(csvReader.readRecord()) {
        String [] values = csvReader.getValues();
        if (values.length != 3) {
          continue;
        }
        String[] corps = values[1].split(",");
        for (String corp : corps) {
          JsonObject json = new JsonObject();
          json.put("in_value", values[0])
              .put("in", Schema.Relations.CORPTYPE_COMPANY.getName())
              .put("out", Schema.Relations.COMPANY_CORPTYPE.getName())
              .put("out_value", corp)
          ;
          items.add(json);
        }
      }
    } catch (FileNotFoundException e) {
      logger.info("[NoFile] =>" + e.getMessage());
    } catch (IOException e) {
      logger.info("[IOExp] =>" + e.getMessage());
    }
  }

}
