# FIS (Frequent Item-Sets)

#### Big Data Project

**Students:**
+ Andrea Galloni (andrea [dot] galloni [at] studenti [dot] unitn [dot] it  )
+ Daniel Bruzual (daniel [dot] bruzualbalzan [at] studenti [dot] unitn [dot] it )

**Organization:** [UniTN](http://www.unitn.it/en)

**Course:** [Big Data and Social Networks](http://web.unitn.it/scienze/25367/struttura-del-corso)

## Frequent Itemsets in Map-Reduce with Spark

This document presents the work done for the **Big Data and Social Networks** course taken at **UNITN** during the first semester of 2015-2016. The project consisted of several steps.

First a dataset from traffic accidents in the USA NASS General Estimates System was selected and integrated to ensure consistency across different years. This involved studying the data, reading the description manuals, building a common schema and then converting the data with the help of special **data integration**  software **Talend Studio**

Afterwards, several known algorithms to calculate frequent itemsets were implemented. **Apache Spark** was used for implementing them in a parallel paradigm. **A new parallel algorithm was also developed**, which uses repeated random sampling and concepts from other algorithms to calculate frequent itemsets on big datasets while **reducing execution time**.

Finally, the algorithms were tested and compared using the integrated datasets, along with other classic datasets in the field of itemsets mining.

*For further details please have a look to the project report .pdf file.
**Any question? Please do not be shy write us an e-mail.** *

#### [ALGORITHMS](https://en.wikipedia.org/wiki/Algorithm):

+ [Apriori](https://en.wikipedia.org/wiki/Apriori_algorithm) [3]
+ [FP-Growth](https://en.wikipedia.org/wiki/Association_rule_learning#FP-growth_algorithm) [4]
+ SON [8]
+ BonGar (**novel solution**)

#### [DATA-SETS:](http://fimi.ua.ac.be/data/)

+ MushRooms
+ Retail
+ Kosarak
+ [GES (General Estimates System)](ftp://ftp.nhtsa.dot.gov/GES)

#### TESTING ENVIRONMENT:

**OperativeSystem:** OS X YOSEMITE 10.10.4<br/>
**Spark:** 1.6.0<br/>
**Scala:** 2.11.7<br/>
**Python:** 2.7.6<br/>
**Memory:** 8 GB DDR3<br/>
**Processor** INTEL CORE I5 2.6 GHz<br/>
**Number of Processors:** 1<br/>
**Total Number of Cores:** 2<br/>
**L2 Cache (per Core):** 256 KB<br/>
**L3 Cache:** 3 MB<br/>

#### REFERENCES:

[1] R. C. Agarwal, C. C. Aggarwal, and V. Prasad. A tree
projection algorithm for generation of frequent item
sets. Journal of parallel and Distributed Computing,
61(3):350–371, 2001.

[2] R. Agrawal, T. Imieli´nski, and A. Swami. Mining
association rules between sets of items in large
databases. ACM SIGMOD Record, 22(2):207–216,
1993.

[3] R. Agrawal and R. Srikant. Fast algorithms for mining
association rules in large databases. In Proceedings of
the 20th International Conference on Very Large Data
Bases, VLDB ’94, pages 487–499, San Francisco, CA,
USA, 1994. Morgan Kaufmann Publishers Inc.

[4] J. Han, J. Pei, and Y. Yin. Mining frequent patterns
without candidate generation. In ACM SIGMOD
Record, volume 29, pages 1–12. ACM, 2000.

[5] M. Hontsma and A. Swami. Set oriented mining for
association rules in relatrend database. Technical
report, The technical report RJ9567, IBM Almaden
Research Centre, San Jose, California, 1993.

[6] H. Li, Y. Wang, D. Zhang, M. Zhang, and E. Y.
Chang. Pfp: parallel fp-growth for query recommendation. In Proceedings of the 2008 ACM
conference on Recommender systems, pages 107–114.
ACM, 2008.

[7] J. Malviya, A. Singh, and D. Singh. An fp tree based
approach for extracting frequent pattern from large
database by applying parallel and partition projection.
International Journal of Computer Applications,
114(18), 2015.

[8] A. Savasere, E. R. Omiecinski, and S. B. Navathe. An
efficient algorithm for mining association rules in large
databases. 1995.

[9] H. Toivonen et al. Sampling large databases for
association rules. In VLDB, volume 96, pages 134–145,
1996.

[10] R. Ullman and Leskovec. Mining of Massive Datasets.
Cambridge University Press, Cambridge, 2014.

[11] N. U.S. Department of Transportation. National
automotive sampling system (nass) general estimates
system (ges). analytical user’s manual 1988-2014.
November 2015.

[12] T. Xiao, C. Yuan, and Y. Huang. Pson: A parallelized
son algorithm with mapreduce for mining frequent
sets. In Parallel Architectures, Algorithms and
Programming (PAAP), 2011 Fourth International
Symposium on, pages 252–257. IEEE, 2011.

## Technologies:

<p align="center">
  <a href="https://spark.apache.org/">
  <img src="https://spark.apache.org/docs/1.3.0/api/python/_static/spark-logo-hd.png" width="300">
  </a>
</p>

<p align="center">
  <a href="https://www.python.org">
  <img src="https://www.python.org/static/community_logos/python-logo-inkscape.svg" width="300">
  </a>
</p>

<p align="center">
  <a href="http://ipython.org/">
  <img src="http://ipython.org/_static/IPy_header.png" width="300">
  </a>
</p>

<p align="center">
  <a href="https://atom.io/">
  <img src="https://cloud.githubusercontent.com/assets/72919/2874231/3af1db48-d3dd-11e3-98dc-6066f8bc766f.png" width="300">
  </a>
</p>

<p align="center">
  <a href="https://git-scm.com/">
  <img src="https://git-scm.com/images/logos/downloads/Git-Logo-1788C.png" width="200">
  </a>
</p>

<p align="center">
  <a href="http://matplotlib.org/api/pyplot_api.html">
  <img src="http://matplotlib.org/xkcd/_static/logo2.png" width="300">
  </a>
</p>


### LICENSE:

This is free and unencumbered software released into the public domain.

Anyone is free to copy, modify, publish, use, compile, sell, or
distribute this software, either in source code form or as a compiled
binary, for any purpose, commercial or non-commercial, and by any
means.

In jurisdictions that recognize copyright laws, the author or authors
of this software dedicate any and all copyright interest in the
software to the public domain. We make this dedication for the benefit
of the public at large and to the detriment of our heirs and
successors. We intend this dedication to be an overt act of
relinquishment in perpetuity of all present and future rights to this
software under copyright law.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE.

For more information, please refer to <http://unlicense.org/>

#.

<p align="center">
  <a href="http://unitn.it/en">
  <img src="https://raw.githubusercontent.com/sn1p3r46/introsde-2015-assignment-3-client/master/images/LogoUniTn.png" width="300">
  </a>
</p>
