RollingScan
===========
##Problems
In HBase, compaction (minor and major) and split is a major bottleneck for write performance as they introduce lots of disk operations and data movement. So, if disabling them, the write performance can be improved by several times (depending on the use case). Split can be disabled if we can pre-split table when creation. However, generally, compaction can't be disabled as the read (`get`/`scan`) performance will be greatly hurted if disabled. The reason is that read operation needs to read each HFile and merge the result before returning to user (based on the fact that: HFiles of a region may have overlapped rowkey range). So, if compaction is disabled, there will be hundreds or even thousands of HFiles per region. This leads to unacceptable disk operation count.  

However, we find that, if the rowkey range of each HFile in a region doesn't overlap with each other, we can break above assumption. As HFile doesn't overlap with each other, we need not access all HFiles at a time. We only need to access one HFile at a time. Thus, the number of HFiles per region doesn't impact the read performance. So, in this case, we can disable compaction and don't impact read performance at all. 

The question is: when can above condition (rowkey range of each HFile in a region doesn't overlap) be satisfied? The answer is: when rows are inserted in a rowkey monotonical way and data are never updated after insertion. In such condition, we know that the new generated HFile will have bigger or smaller row range than elder HFiles, but not overlapped. 

This condition can be loosen a little bit: the rowkey range of each HFile in a region can overlap slightly. This is used to handle case that rows are put monotonically but multiple puts are used to put one row into HBase. In this case, two puts of one row may be in two adjacent HFiles. 

Disabling compaction and split is easy in HBase. HBase `get` operation implementation is good enough to skip unnecessary HFiles. But, to support `scan` operation for such HFile pattern needs a specialized Scanner implementation. We name it as rolling scanner as it will only access a small batch of HFiles at one time and the batch window will roll when scanning. 

##Solution
###Assumptions
–	Disable all the compactions and splits.

–	The inserted data are never updated.

–	The row keys are monotonically increased.

 
###Implementation
####RollingStoreFileScanner

`RollingStoreFileScanner` is a scanner which contains a batch of `StoreFileScanner`s (one per opened HFile). This batch window will roll when scanning. When rolling, the old HFiles will be closed and new ones will be open. 

In implmentation, `RollingStoreFileScanner` contains an optimized heap for `StoreFileScanner`s, only few of the scanner are in this heap when scanning. Thus, the large amount of StoreFiles don’t impact this scanner.

 
####RollingScanRegionObserver
The next question is how to provide user the `RollingStoreFileScanner` when user performing a `scan` operation. The way we do so is to implement an `Observer`. And we overwrite following methods in this `Observer`:

–	postOpen method: if the rolling scan is enabled, after the region is opened, the FirstKeySortedStoreFiles will be created for each store.

–	preStoreScannerOpen method: if the rolling scan is enabled, the RollingStoreFileScanner will be used instead.

–	postClose methods.


##How to use

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
```xml
<property>
    <name>hbase.coprocessor.region.classes</name>           
    <value>org.apache.hadoop.hbase.regionserver.RollingScanRegionObserver</value>
</property>
```

