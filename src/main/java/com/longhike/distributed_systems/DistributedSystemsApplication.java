package com.longhike.distributed_systems;

import java.io.File;
import java.io.IOException;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import com.longhike.distributed_systems.map_reduce.MapReduce;

@SpringBootApplication
public class DistributedSystemsApplication {

  public static void main(String[] args) {
    SpringApplication.run(DistributedSystemsApplication.class, args);
    
    // MAP REDUCE
    doMapReduce();
  }

  private static void doMapReduce() {
    try {
      Resource[] resources = new PathMatchingResourcePatternResolver().getResources("classpath:map_reduce/*.txt");
      File[] files = new File[resources.length];

      for (int i = 0; i < resources.length; i++) {
        files[i] = resources[i].getFile();
      }
      new MapReduce(8, files).execute();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

}
