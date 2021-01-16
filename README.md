# Spark Study
本项目包含了Spark的多种不同类型的Demo，便于快速掌握其开发特点，也适用于面试官和求职者。

## 环境要求
* Scala 2.11
* Java 8
* 2.2.0

## 目录
### 1.RDD
### 2.DataFrame
| 路径   |  考点   |  题目描述   |
| ---- | ---- | ---- |
|   [dataframe.Df01](https://github.com/jason-wang1/sparkstudy/blob/master/src/main/scala/dataframe/Df01.scala)   |   Window / groupBy / agg   |   给一张学生成绩明细表，找出所有科目成绩都大于某一学科平均乘积的学生   |
|   [dataframe.Df02](https://github.com/jason-wang1/sparkstudy/blob/master/src/main/scala/dataframe/Df02.scala)   |   Window / groupBy / agg   |   给一张用户每天观看次数明细表，计算当月观看总次数与历史累计观看总次数   |
|   [dataframe.Df03](https://github.com/jason-wang1/sparkstudy/blob/master/src/main/scala/dataframe/Df03.scala)   |   Window / groupBy / agg   |   给一张用户点击店铺的明细，输出点击每个店铺最多的top3用户，并给出该用户点击该店铺次数以及店铺内的排名   |

* GraphX
* Ml
