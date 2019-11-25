# SnowShu
container-driven data warehouse sampling for faster, more efficient transform development

### What is SnowShu?
SnowShu is a library for managing sampled images of data warehouses that can be run locally during transform development. If you are using TDD to develop data warehouse software, SnowShu is for you.  

### Why use SnowShu?
Data warehouses can be naturally be pretty massive. We want the software we write to run all-encompassing transforms on the datasets in these warehouses, and these transformations can reasonably take hours (or longer) to run. However this makes test driven development difficult; you can either write code that behaves differently between testing and production, or you can limit the test input data to a managable size for development. We feel that the first solution is highly undesirable, and so SnowShu aims to make the second solution trivial to implement. 
