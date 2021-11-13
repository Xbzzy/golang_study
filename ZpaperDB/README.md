# ZpaperDB(A distributed B+tree StorageEngine)
The purpose of this project is to understand how a BTree StorageEngine work in distributed database system.And this whole database system is divide into several module.<br>
<br>
(Function is not yet perfect)<br>
## 1. B+tree Structure
  B-tree is a self-blancing tree data structure with sorted data and allows serches,sequential access,insertions,and deletions in logarithmic time(depending on the order num).The B-tree is a generalization of a binary search tree in that a node can have more than two children.Unlike self-balancing binary search trees, the B-tree is well suited for storage systems that read and write relatively large blocks of data, such as discs. It is commonly used in databases and file systems.<br>
  <br>
  Because of that B-tree has already existed for a long time.So many optimization measures have been developed.It is called B+Tree.In contrast to B-Tree,the page of B+Tree has only saved the key's thumbnail information.In order to saved more key in index page .On the other hand,every leaf node maybe saved the pointer of its bro of the same order number additional.It's fine like that we can sequential scanning keys without return parent node.<br>
  <br>
  The above points are examples.Other details will mentioned after the introduction.<br>
### 1.1 Serches
  The serches of B+tree are the same as other binary search trees.Because of the ordering,we only need traversal starts at the root node successively.Then enter the scope-conforming children node,will eventually find the record.<br>
### 1.2 Insertions
  The insertions of B+Tree must ensure ordered after insert in leaf node.So we need consider three kinds of situation.<br>
  <br>
  (1) leaf node non-full and index node non-full: Insert the record in leaf node directly.<br>
  <br>
  (2) leaf node full and index node non-full: First of all,split leaf node and put the middle key in index node.Secondly,put the smaller than midder node's record on the left node and put the others on the right node.Then insert the record in new leaf node again.br>
  <br>
  (3) leaf node full and index node full: First of all,split leaf node and put these records smaller than midder on the left node and put the others on the right node.Secondly,split index node and put the smaller than midder node's record on the left node and put the others on the right node.Lastly,put the middle record at the next higher order index node.<br>
### 1.3 Deletions
  B+Tree use the fill factor to control deleted changes.50% is the smallest value.The deletions of B+tree must ensure ordered after delete,too.Unlike insert,deletions measure in terms of the change in the fill factor.We also need consider three kinds of situation in deletions.<br>
  <br>
  (1) leaf node bigger than fill factor and middle node also bigger than it: Delete the record in leaf node directly,if this node is index node,use the right node instead.<br>
  <br>
  (2) leaf node smaller than fill factor and middle node bigger than it: Combine leaf node and its bro node.At the same time,update the index node.<br>
  <br>
  (3) leaf node smaller than fill factor and middle node also smaller than it: First of all,combine leaf node and its bro node.Secondly,update index node and combine index node and its bro node.<br>
  <br>
### 1.4 Comparison with LSM-tree
  According to the experience,LSM-tree usually write faster and B+Tree is considered to read faster.The following will introduce their respective advantages and disadvantages.<br>
  <br>
  (1) The advantages of LSM-tree<br>
  The B+tree index usually write data by twice.Once is Write-Ahead-Log.The another is write into tree page.If the page begin to split,it will waste more time.Even if the page is only changed by a few bytes,system also need to write the whole page.<br>
  <br>
  For heavy write intensive applications,the performance bottleneck is write speed.In this case,the cost of performance is write-amplification:The more times StorageEngine write into disk,the less number of writes per second that can be processed in disk bandwidth.In addition,LSM-tree usually can bear more write-throughput.Because of the less write-amplification(although it depends on the StorageEngine setting) and written way,it's sequential write can meet the disk working mode.<br>
  <br>  
  The LSM-tree support better compression,so the file in disk is smaller than B+tree.When any page begin split or a line of record can not save in one page.The fragmentation in B+tree make part of the space unusable.Due to the LSM-tree is not facine page,and rewrite SSTable to remove fragmentation regularly.So it have less storage cost.Especially when use delamination-compression.<br>
  <br>
  (2) The disadvantages of LSM-tree<br>
  The most obvious disadvantage in LSM-tree is that read/write operations in progress will be interferenced when SSTable is compressing.Even if StorageEngine try parallel compression,but disk concurrent resources is also finite.So the problem is also that compression is expensive,it make read/write operations affected.<br>
  <br>
  For heavy write-throughput,another problem comes soon:The disk's limited write bandwidth is insufficient.Because it is shared with initial written and compressed threads in the background.The more data size in date system,the more write bandwidth is required by compressed.<br>
  <br>
  If write-throughput is heavy and compression setting is not suitable for existing data written,then compression can not match the new data write rate.In this case,unmerged segment will becoming more and more untile disk space is full.And as checking more segment file,read rate become slow,too.**And here's the killer part:StorageEngine do not limiting write rate because of it.**<br>
  <br>
## 2. Disk Operation
  No matter how to optimize the Memory B+tree structure,we can not forget the nature of B+tree.It is a disk index structure. So that the disk operation in this whole StorageEngine is important.The following will also be introduced separately by several modules.<br>
  <br>
### 2.1 Disk file structure 
  The disk file storage is often accompanied by data encoding and decoding.We usually save data in object\structural body\list\array\hash table\tree in memory.These structures use pointer to optimize CPU access or operation.But when write data into disk file or send over the website,it used to encoding to some self-contained sequence of bytes(such as JSON).
### 2.2 Fsync mechanism
  
## 3. Buffer Pool
## 4. Transaction Module
### 4.1 Locking
### 4.2 Redo log 
### 4.3 Undo log
### 4.4 Purge
### 4.5 Group Commit
## 5. Other Features In OLTP Database System 
### 5.1 Double Write
### 5.2 Change Buffer
### 5.3 Self-adaption Hash Index
### 5.4 Async IO
### 5.5 Flush Neighbor Page
## 6. Distributed Module
