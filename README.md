RollingScan
===========
#Problems
•	Compaction and split
–	Compaction and split use lots of resources.
–	Do we still need the compaction and split if the row keys are monotonically increased? If the compaction is not needed, how to guarantee the read performance?

#Solution
•	Requirements on the table

–	Disable all the compactions and splits.

–	The data are never modified.

–	The row keys are monotonically increased, the non-monotonicity of few row keys is allowed as well.

 
#•	RollingStoreFileScanner

–	Provide an optimized heap for StoreFileScanners, only few of the scanner are in this heap when scanning.

–	The large amount of StoreFiles don’t impact this scanner.

 
#•	RollingScanRegionObsever

–	postOpen method: if the rolling scan is enabled, after the region is opened, the FirstKeySortedStoreFiles will be created for each store.

–	preStoreScannerOpen method: if the rolling scan is enabled, the RollingStoreFileScanner will be used instead.

–	postClose methods.


#Enable the RollingStoreFileScanner

•	Disable the compaction and split for a table, drop a hint to use the RollingStoreFileScanner

```java
HTableDescriptor desc = new HTableDescriptor(tableName) ;

//Disable auto-split
desc.setValue(HConstants.HREGION_MAX_FILESIZE,String.valueOf(Long.MAX_VALUE));

//disable major compaction
desc.setValue(HConstants.MAJOR_COMPACTION_PERIOD, "0");

//disable all compactions
desc.setValue(“hbase.hstore.compaction.min”, Integer.Max);
```

•	Add the RollingScanRegionObserver into the coprocessors

<property>
    <name>hbase.coprocessor.region.classes</name>           
    <value>org.apache.hadoop.hbase.regionserver.RollingScanRegionObserver</value>
</property>

