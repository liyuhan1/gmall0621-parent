package com.atguigu.gmall.utils

import java.util

import com.atguigu.gmall.bean.DauInfo
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index, Search, SearchResult}

object MyESUtil {

  private var factory: JestClientFactory = null

  def getClient: JestClient = {
    if (factory == null)
      build()
    factory.getObject
  }

  def build(): Unit = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder("http://hadoop102:9200")
      .multiThreaded(true)
      .maxTotalConnection(20)
      .connTimeout(10000).readTimeout(1000).build())
  }

  def bulkInsert(sourceList: List[(String, Any)], indexName: String): Unit = {
    if (sourceList != null && sourceList.size > 0) {
      val jestClient: JestClient = getClient
      //创建批量操作对象
      val builder: Bulk.Builder = new Bulk.Builder()
      //添加批量操作
      for ((id, source) <- sourceList) {
        val index: Index = new Index.Builder(source)
          .index(indexName)
          .`type`("_doc")
          .id(id)
          .build()
        builder.addAction(index)
      }
      val bulk: Bulk = builder.build()
      val result: BulkResult = jestClient.execute(bulk)
      println(s"向ES中插入${result.getItems.size()}条")
      jestClient.close()
    }
  }


  def main(args: Array[String]): Unit = {
    //putIndex()
    queryIndex()
  }

  //向ES中插入数据
  def putIndex(): Unit = {
    val jest: JestClient = getClient
    //Builder中的参数，底层会转换为Json格式字符串，所以我们这里封装Document为样例类Movie
    //当然也可以直接传递json
    val actorNameList = new util.ArrayList[String]()
    actorNameList.add("zhangsan")
    val index: Index = new Index.Builder(Movie("100", "天龙八部", actorNameList))
      .index("movie_index_5")
      .`type`("movie")
      .id("1")
      .build()
    //execute的参数类型为Action，Action是接口类型，不同的操作有不同的实现类，添加的实现类为Index
    jest.execute(index)
    //关闭连接
    jest.close()
  }

  case class Movie(id: String, movie_name: String, actorNameList: java.util.List[String]) {}


  //从ES中查询数据
  def queryIndex(): Unit = {
    val jest: JestClient = getClient

    //查询常用有两个实现类 Get通过id获取单个Document，以及Search处理复杂查询
    val query =
      """
        |{
        |  "query": {
        |    "bool": {
        |       "must": [
        |        {"match": {
        |          "name": "red"
        |        }}
        |      ],
        |      "filter": [
        |        {"term": { "actorList.name.keyword": "zhang han yu"}}
        |      ]
        |    }
        |  },
        |  "from": 0,
        |  "size": 20,
        |  "sort": [
        |    {
        |      "doubanScore": {
        |        "order": "desc"
        |      }
        |    }
        |  ],
        |  "highlight": {
        |    "fields": {
        |      "name": {}
        |    }
        |  }
        |}
    """.stripMargin
    val search: Search = new Search.Builder(query)
      .addIndex("movie_index_5")
      .build()
    //执行操作
    val result: SearchResult = jest.execute(search)
    //获取命中的结果  sourceType:对命中的数据进行封装，因为是Json，所以我们用map封装
    //注意：一定得是Java的Map类型
    val rsList: util.List[SearchResult#Hit[util.Map[String, Any], Void]] = result.getHits(classOf[util.Map[String, Any]])

    //将Java转换为Scala集合，方便操作
    import scala.collection.JavaConverters._
    //获取Hit中的source部分
    val list: List[util.Map[String, Any]] = rsList.asScala.map(_.source).toList
    println(list.mkString("\n"))

    jest.close()
  }

}
