# GoT_indexing
Data Engineering Challenge

## Introduction

Develop an inverted index to identify which words are present in the corpus of documents.

## How to run?

Program was developed using a spark script written in python. It can be run using the following command.

`./run.sh`

This is a shell script which runs the python script with the spark configurations.

## Approach:

### 1. Read in the documents along with the filename as a DataFrame and repartition the dataframe along the column "filename"

     `files_df = spark.read.text(path_to_files).select(input_file_name().alias("filename"), "value")`
     `files_part_df = files_df.repartition("filename")`

### 2. Convert the partitoned dataframe to an RDD. Split the document into words and generate a dictionary of words. The distinct words are mapped with an index.

       `words = files_part_df_rdd.flatMap(lambda word_file: word_file[1].lower().split(" "))
        distinct_words_rdd = words.flatMap(explode).distinct()
        distinct_word_rdd_index = distinct_words_rdd.zipWithIndex() `

   The final RDD is of the form (word, wordid)
        
### 3. Split the document into words, convert them to lowercase and map each word to its corresponding file id

     
      `files_part_df_rdd = files_part_df.rdd
       word_document = files_part_df_rdd.map(lambda word_file_pair: ((word_file_pair[0],word_file_pair[1].lower().split()))).flatMap( lambda x: ([(y,x[0]) for y in x[1]]))`
       
### 4. Select the distinct (word, filename) pairs
  
      `word_document_distinct_rdd = word_document.distinct()`

### 5. Perform a join on the distinct words RDD and the distinct (word,filename) RDD to map the latter to the wordid of the dictionary of words

       `word_id_file_id_pairs = distinct_word_rdd_index.join(word_document_distinct_rdd).map(lambda x: x[1])`
       

### 6. The RDD is now of the form (wid, fid). A GroupByKey() operation is performed on the same to collect all the files having the particular wid into a list in a sorted manner

       `words_fids_groups_rdd = fid_wid_part_rdd_wid_key.groupByKey().map(lambda x : (x[0], sorted(list(x[1]))))`


### 8. The word ids are also sorted to obtain the final inverted index.
     
       `words_fids_groups_sorted_rdd = words_fids_groups_rdd.sortByKey()`



      
      
      
   
