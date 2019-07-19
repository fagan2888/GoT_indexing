from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import input_file_name
def explode(row):
   for word in row:
      yield row

APP_NAME = "Indexing"
file_name  = "coding_challenges/data/indexing"
def main(sc,inputfilename):
    files_df = spark.read.text(inputfilename).select(input_file_name().alias("filename"), "value")
    #partition by files
    files_part_df = files_df.repartition("filename")
    #convert to rdd
    files_part_df_rdd = files_part_df.rdd 
    words = files_part_df_rdd.flatMap(lambda word_file: word_file[1].lower().split(" "))
    distinct_words_rdd = words.flatMap(explode).distinct()
    distinct_word_rdd_index = distinct_words_rdd.zipWithIndex()
    #map each word to the filename
    word_document = files_part_df_rdd.map(lambda word_file_pair: ((word_file_pair[0],word_file_pair[1].lower().split()))).flatMap( lambda x: ([(y,x[0]) for y in x[1]]))
    #select the distinct (word,filename) pairs
    word_document_distinct_rdd = word_document.distinct()
    word_id_file_id_pairs = distinct_word_rdd_index.join(word_document_distinct_rdd).map(lambda x: x[1])
    #switch the ordering to (wid,fid)
    wid_fid_pairRDD = word_id_file_id_pairs.map(lambda x :(x[1],x[0]))
    fid_wid_part_rdd_wid_key = wid_fid_pairRDD.map(lambda x : (x[1],int(x[0].split('/')[-1])))
    words_fids_groups_rdd = fid_wid_part_rdd_wid_key.groupByKey().map(lambda x : (x[0], sorted(list(x[1]))))
    words_fids_groups_sorted_rdd = words_fids_groups_rdd.sortByKey()
    


if __name__ == "__main__":
    sc = SparkContext(conf=SparkConf().setAppName(APP_NAME))
    # Execute Main functionality
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    main(spark,file_name)

