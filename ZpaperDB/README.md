# ZpaperDB(A distributed BTree StorageEngine)
The purpose of this project is to understand how a BTree StorageEngine work in distributed database system.And this whole database system is divide into several module.<br>
(Function is not yet perfect)<br>
## 1. B+Tree Structure
  B-Tree is a self-blancing tree data structure with sorted data and allows serches,sequential access,insertions,and deletions in logarithmic time(depending on the order num).The B-tree is a generalization of a binary search tree in that a node can have more than two children.Unlike self-balancing binary search trees, the B-tree is well suited for storage systems that read and write relatively large blocks of data, such as discs. It is commonly used in databases and file systems.<br>
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
