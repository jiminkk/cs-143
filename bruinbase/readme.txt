Ji Min Kim (804255847): jkim0110@ucla.edu
Nikhil Shridhar (504273348): nikhilshridhar@ucla.edu

Finished Part D - We found a bug after inserting the 2796 tuple (which is part of the 65th leaf node). That is why when we we try to run queries with the large and xlarge table, the results vary.

Our design of the B+ tree followed the spec pretty closely. We added some checks before reading through the tuples to account for some optimization. There were some cases where we wouldn't have to read through the disk, so we could skip that process altogether (an example was when attribute was 4 (count(*))). Other than that, we did a few more sanity checks to make sure that the conditions in the query were valid. This would prevent some page reads of the disk.