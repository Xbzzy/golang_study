# ZpaperDB(A distributed B+Tree StorageEngine)
>The purpose of this project is to understand how a BTree StorageEngine work in distributed database system.And this whole database system is divide into several module.<br>
(Function is not yet perfect)<br>
## 1. B+Tree Structure
  >B-Tree is a self-blancing tree data structure with sorted data and allows serches,sequential access,insertions,and deletions in logarithmic time(depending on the order num).The B-tree is a generalization of a binary search tree in that a node can have more than two children.Unlike self-balancing binary search trees, the B-tree is well suited for storage systems that read and write relatively large blocks of data, such as discs. It is commonly used in databases and file systems.<br>
  >Because of that B-Tree has already existed for a long time.So many optimization measures have been developed.It is called B+Tree.In contrast to B-Tree,the page of B+Tree has only saved the key's thumbnail information.In order to saved more key in index page .On the other hand,every leaf node maybe saved the pointer of its bro of the same order number additional.It's fine like that we can sequential scanning keys without return parent node.<br>
  >The above points are examples.Other details will mentioned after the introduction.<br>
### 1.1 Serches
  >The serches of B+Tree are the same as other binary search trees.Because of the ordering,we only need traversal starts at the root node successively.Then enter the scope-conforming children node,will eventually find the record.<br>
### 1.2 Insertions
  >The insertions of B+Tree must ensure ordered after insert in leaf node.So we need consider three kinds of situation.<br>
  >>(1) leaf node non-full and index node non-full: Insert the record in leaf node directly.<br>
  >>(2) leaf node full and index node non-full: First of all,split leaf node and put the middle node in index node.Secondly,put the smaller than midder node's record on the left node and put the others on the right node.<br>
  >>(3) leaf node full and index node full: First of all,split leaf node and put the smaller than midder node's record on the left node and put the others on the right node.Secondly,split index node and put the smaller than midder node's record on the left node and put the others on the right node.Lastly,put the middle node at the next higher order index node.<br>
### 1.3 Deletions
  >B+Tree use the fill factor to control deleted changes.50% is the smallest value.The deletions of B+Tree must ensure ordered after delete,too.Unlike insert,deletions measure in terms of the change in the fill factor.We also need consider three kinds of situation in deletions.<br>
  >>(1) leaf node bigger than fill factor and middle node also bigger than it: Delete the record in leaf node directly,if this node is index node,use the right node instead.<br>
  >>(2) leaf node smaller than fill factor and middle node bigger than it: Combine leaf node and its bro node.At the same time,update the index node.
  >>(3) leaf node smaller than fill factor and middle node also smaller than it: First of all,combine leaf node and its bro node.Secondly,update index node and combine index node and its bro node.<br>
### 1.4 Comparison with LSM-tree
  >According to the experience,LSM-tree usually write faster and B+Tree is considered to read faster.The following will introduce their respective advantages and disadvantages.<br>
  >>(1) The advantages of LSM-tree<br>
    
## 2. Disk Operation
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
