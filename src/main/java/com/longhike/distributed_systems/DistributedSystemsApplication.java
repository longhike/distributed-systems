package com.longhike.distributed_systems;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import com.longhike.distributed_systems.kv_store.KVStore;
import com.longhike.distributed_systems.map_reduce.MapReduce;
import com.longhike.distributed_systems.web_crawler.WebCrawler;

@SpringBootApplication
public class DistributedSystemsApplication {

  public static void main(String[] args) {
    SpringApplication.run(DistributedSystemsApplication.class, args);
    
    // MAP REDUCE
    // doMapReduce();
    // WEB CRAWL
    // doWebCrawl();
    // KV STORE
    doKVStore();
  }

  private static void mapReduce() {
    try {
      Resource[] resources = new PathMatchingResourcePatternResolver().getResources("classpath:map_reduce/*.txt");
      List<File> filesList = new ArrayList<>();

      for (Resource resource : resources) {
        filesList.add(resource.getFile());
      }

      File[] files = filesList.toArray(new File[0]);
      var mapReduce = new MapReduce(8, files);
      mapReduce.execute();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private static void doWebCrawl() {
    new WebCrawler().crawl("https://en.wikipedia.org/wiki/England", "https://en.wikipedia.org/wiki/Jesus");
  }

  private static void doKVStore() {
    KVStore.doKVStore();
  }
}
