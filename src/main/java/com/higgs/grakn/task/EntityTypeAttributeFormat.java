package com.higgs.grakn.task;

import com.csvreader.CsvReader;
import com.higgs.grakn.util.FileUtils;
import com.higgs.grakn.variable.Variable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: JerryYou
 *
 * Date: 2019-08-30
 *
 * Copyright (c) 2018 devops
 *
 * <<licensetext>>
 */
public class EntityTypeAttributeFormat {

  static Logger logger = LoggerFactory.getLogger(EntityTypeAttributeFormat.class);

  public void convert(String path, String outPut) {
    Map<String, List<String>> typeMap = new HashMap<>();
    try {
      CsvReader csvReader = new CsvReader(KbParseData.getReader(path));
      while(csvReader.readRecord()) {
        String [] values = csvReader.getValues();
        if (values.length != 2) {
          continue;
        }
        String name = values[0];
        String type = values[1];
        List<String> types =  typeMap.getOrDefault(name, new ArrayList<>());
        types.add(Variable.entityTypeList.get(Integer.parseInt(type)));
        typeMap.put(name, types);
      }
    } catch (FileNotFoundException e) {
      logger.info("[NoFile] =>" + e.getMessage());
    } catch (IOException e) {
      logger.info("[IOExp] =>" + e.getMessage());
    }
    FileUtils.saveFileToCsv(outPut, typeMap);
  }

  public static void main(String[] args) {
    EntityTypeAttributeFormat entityTypeAttributeFormat = new EntityTypeAttributeFormat();
    entityTypeAttributeFormat.convert("/Users/devops/workspace/kb/kb_system/entity-type.csv",
        "/Users/devops/workspace/kb/kb_system/entity-type-format.csv");
  }
}
