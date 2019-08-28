package com.higgs.grakn.task;

import com.csvreader.CsvReader;

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

  static  public void parseAttribute(List<JsonObject> items, String path) {
    try {
      CsvReader csvReader = new CsvReader(getReader(path));
      // csvReader.readHeaders();
      int count = csvReader.getHeaderCount();
      if (count != 4) {
        logger.info("csv header count error, check pls");
        return;
      }
      String attributeType = csvReader.getHeader(1);
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

}
