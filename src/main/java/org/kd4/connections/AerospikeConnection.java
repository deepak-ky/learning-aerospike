package org.kd4.connections;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;

public class AerospikeConnection {

  private static final String address = "127.0.0.1"; // aerospike host
  private static final Integer port = 3000; // aerospike port
  private static final String namespace = "test"; // cluster namespace
  private static final String set = "testSet"; // set name within namespace


  public static void main(String[] args) {
    System.out.println("beginning from here...");

    // instantiate the aerospike client
    IAerospikeClient client = new AerospikeClient(address, port);

    // create a writepolicy
    WritePolicy writePolicy = new WritePolicy();
    writePolicy.totalTimeout = 5000;

    // create a record Key, a tuple consisting of namespace, set name and user defined key
    Key key = new Key(namespace, set, "U12250");

    // create a bin to store data within the new record

    // If pkBinOptional is not formed PK would be empty for the record formed (info required)
    Bin pkBinOptional = new Bin("PK", "U12250");
    Bin nameBin = new Bin("name", "Deepak");
    Bin companyBin = new Bin("company", "PIBS");

    // write the record to aerospike
    try {
      client.put(writePolicy, key, pkBinOptional, nameBin, companyBin);
      System.out.println("successfully wrote record");
    } catch (AerospikeException e) {
      e.printStackTrace();
    }

    System.out.println("code came here....");
    // create a policy to set the total timeout for reads
    Policy readPolicy = new Policy();
    readPolicy.totalTimeout = 5000;

    try {
      Record record = client.get(readPolicy, key);
      System.out.format("Record : %s", record.bins);
      System.out.println();
      System.out.format("Key : %s", key.userKey);
    } catch (AerospikeException e) {
      e.printStackTrace();
    } finally {
      client.close();
    }
  }
}
