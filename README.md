# Spark Study
本项目包含了Spark的多种不同类型的Demo，便于快速掌握其开发特点，也适用于面试官和求职者。

## 环境要求
* Scala 2.11
* Java 8
* Spark 2.2.0

## 目录
### 1.RDD
| 编号   |  考点   |  简介   |
| ---- | ---- | ---- |
|   [WordCount](https://github.com/jason-wang1/sparkstudy/blob/master/src/main/scala/rdd/WordCount.scala)   |   flatMap / map / reduceByKey / sortBy   |   给一个RDD[String]，进行分词，word count 并倒序排序   |
|   [NumAcc](https://github.com/jason-wang1/sparkstudy/blob/master/src/main/scala/rdd/NumAcc.scala)   |   reduce / Spark原生累加器   |   给一个RDD[Long] / RDD[Double]，全量求和   |

### 2.DataFrame
| 编号   |  考点   |  简介   |
| ---- | ---- | ---- |
|   [Df01](https://github.com/jason-wang1/sparkstudy/blob/master/src/main/scala/dataframe/Df01.scala)   |   Window / groupBy / agg   |   给一张学生成绩明细表，找出所有科目成绩都大于某一学科平均乘积的学生   |
|   [Df02](https://github.com/jason-wang1/sparkstudy/blob/master/src/main/scala/dataframe/Df02.scala)   |   Window / groupBy / agg   |   给一张用户每天观看次数明细表，计算当月观看总次数与历史累计观看总次数   |
|   [Df03](https://github.com/jason-wang1/sparkstudy/blob/master/src/main/scala/dataframe/Df03.scala)   |   Window / groupBy / agg   |   给一张用户点击店铺的明细，输出点击每个店铺最多的top3用户，并给出该用户点击该店铺次数以及店铺内的排名   |
|   [Df04](https://github.com/jason-wang1/sparkstudy/blob/master/src/main/scala/dataframe/Df04.scala)   |   join   |   给一张用户A关注用户B的表，输出相互关注的用户对   |
|   [Df05](https://github.com/jason-wang1/sparkstudy/blob/master/src/main/scala/dataframe/Df05.scala)   |   union / join / except   |   给一张好友表，输出二度好友表   |
|   [Df06](https://github.com/jason-wang1/sparkstudy/blob/master/src/main/scala/dataframe/Df06.scala)   |   groupBy / agg / countDistinct   |   给一张用户点击商品明细表，输出uv大于1的top3的商品   |
|   [Df07](https://github.com/jason-wang1/sparkstudy/blob/master/src/main/scala/dataframe/Df07.scala)   |   Window / join   |   给一张商户交易明细表，记录了商户每一笔交易金额，输出每一天每个商户在最近2天的交易总金额、最近3天的交易总金额   |

## GraphX
| 编号   |  考点   |  简介   |
| ---- | ---- | ---- |
|   [GraphAttributes](https://github.com/jason-wang1/sparkstudy/blob/master/src/main/scala/graphx/GraphAttributes.scala)   |   vertices、edges、triplets   |   给一张用户关系图，根据其属性进行一些统计   |
|   [LikeAnalysis](https://github.com/jason-wang1/sparkstudy/blob/master/src/main/scala/graphx/LikeAnalysis.scala)   |   vertices、edges、triplets、Degrees、mapVertices、subgraph、join、aggregateMessages   |   给一张用户点赞关系图，进行各类统计   |

## Ml
| 编号   |  考点   |  简介   |
| ---- | ---- | ---- |
|   [DocumentClassification](https://github.com/jason-wang1/sparkstudy/blob/master/src/main/scala/ml/DocumentClassification.scala)   |   Pipeline / CrossValidator / MulticlassClassificationEvaluator   |   构建多分类模型：数据集中每一条样本包含文档内容、文档标签两个字段   |
|   [RegressionModelBuild](https://github.com/jason-wang1/sparkstudy/blob/master/src/main/scala/ml/RegressionModelBuild.scala)   |   特征工程 / Pipeline   |   数据清洗+特征工程+构建回归模型。部分方法有单元测试   |
|   [recommend/Item2Vec](https://github.com/jason-wang1/sparkstudy/blob/master/src/main/scala/ml/recommend/Item2Vec.scala)   |   Word2Vec   |   根据用户对物品对行为序列，训练出物品 embedding 向量  |
|   [recommend/ItemCF1](https://github.com/jason-wang1/sparkstudy/blob/master/src/main/scala/ml/recommend/ItemCF1.scala)   |   udf / 哈希表   |   基于物品的协同过滤：采用哈希表表示物品被评分的向量，以便做点积  |
|   [recommend/ItemCF2](https://github.com/jason-wang1/sparkstudy/blob/master/src/main/scala/ml/recommend/ItemCF2.scala)   |   udf / 有序数组   |   基于物品的协同过滤：采用有序数组表示物品被评分的向量，以便做点积  |
|   [recommend/ItemCF3](https://github.com/jason-wang1/sparkstudy/blob/master/src/main/scala/ml/recommend/ItemCF3.scala)   |   join / groupBy / agg   |   基于物品的协同过滤：不采用任何数据结构表示物品向量，直接对评分表join  |
|   [recommend/JaccardSim](https://github.com/jason-wang1/sparkstudy/blob/master/src/main/scala/ml/recommend/JaccardSim.scala)   |   join / groupBy / agg   |   计算杰卡德相似度  |
