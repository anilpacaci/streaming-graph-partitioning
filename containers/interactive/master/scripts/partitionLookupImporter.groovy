/*
 * Copyright (C) 2015-2016 Stanford University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */


import org.apache.commons.configuration.Configuration
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.commons.io.FileUtils
import org.apache.commons.io.LineIterator



import com.whalin.MemCached.MemCachedClient;
import com.whalin.MemCached.SockIOPool

import java.nio.file.Paths


/**
 * This is a Groovy Script to run inside gremlin console, for loading LDBC SNB data into Tinkerpop Competible Graph.
 * original written by Jonathan Ellithorpe <jde@cs.stanford.edu> <a href="https://github.com/PlatformLab/ldbc-snb-impls/blob/master/snb-interactive-titan/src/main/java/net/ellitron/ldbcsnbimpls/interactive/titan/TitanGraphLoader.java">TitanGraphLoader </>
 *
 * @author Anil Pacaci <apacaci@uwaterloo.ca>
 */
class PartitionLookupImporter {

    static isIdMappingEnabled = false

    static PartitionMapping partitionMappingServer = null

    static void load(String configurationFile) throws IOException {

        Configuration configuration = new PropertiesConfiguration(configurationFile);

        String lookupFile = configuration.getString("partition.lookup")
	    String[] servers = configuration.getStringArray("memcached.address")
        partitionMappingServer = new PartitionMapping(servers)
        int batchSize = configuration.getInt("batch.size")

        try {
            LineIterator it = FileUtils.lineIterator(FileUtils.getFile(lookupFile), "UTF-8")
            long counter = 0
            while(it.hasNext()) {
                String[] parts = it.nextLine().split(",")
                String id = parts[0]
                Integer partition = Integer.valueOf(parts[1])

                partitionMappingServer.setPartition(id, partition)
                counter++

                if(counter % batchSize == 0) {
                    System.out.println("Imported: " + counter)
                }
            }
	    System.out.println("# of keys: " + counter)
        } catch (Exception e) {
            System.out.println("Exception: " + e);
            e.printStackTrace();
        }
    }

    static void addMissingVertices(String configurationFile) {
        Configuration configuration = new PropertiesConfiguration(configurationFile);

        String inputBaseDir = configuration.getString("input.base")
        String[] nodeFiles = configuration.getStringArray("nodes")
        String[] servers = configuration.getStringArray("memcached.address")
        partitionMappingServer = new PartitionMapping(servers)

        int batchSize = configuration.getInt("batch.size")

        try {
            
            for(String fileName : nodeFiles) {
                LineIterator it = FileUtils.lineIterator(FileUtils.getFile(Paths.get(inputBaseDir, fileName).toFile()), "UTF-8")
                long counter = 0
                while(it.hasNext()) {
                    String[] parts = it.nextLine().split("\\|")
                    String id = "person:" + parts[0]
                    Integer partition = 0

                    partitionMappingServer.addPartition(id, partition)
                    counter++

                    if(counter % batchSize == 0) {
                        System.out.println("Imported: " + counter)
                    }
                }
	    System.out.println(String.format("# of keys in %s : %d", fileName, counter))         
   	    }

        } catch (Exception e) {
            System.out.println("Exception: " + e);
            e.printStackTrace();
        }
    }

    static class PartitionMapping {

        private static String INSTANCE_NAME = "partition-lookup";

        private MemCachedClient client;

        public PartitionMapping(String... servers) {
            SockIOPool pool = SockIOPool.getInstance(INSTANCE_NAME);
            pool.setServers(servers);
            pool.setFailover(true);
            pool.setInitConn(10);
            pool.setMinConn(5);
            pool.setMaxConn(250);
            pool.setMaintSleep(30);
            pool.setNagle(false);
            pool.setSocketTO(3000);
            pool.setAliveCheck(true);
            pool.initialize();

            client = new MemCachedClient(INSTANCE_NAME);
            // client.flushAll();
        }

        public Integer getPartition(String identifier) {
            Object value = client.get(identifier);
            if (value == null)
                return null;
            return (Integer) value;
        }

        public void setPartition(String identifier, Integer id) {
            client.set(identifier, id)
        }

        public void addPartition(String identifier, Integer id) {
            client.add(identifier, id)
        }
    }

}
