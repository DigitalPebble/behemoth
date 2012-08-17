# example of commands to run from Hadoop

export behe_home=`pwd .`

mvn clean package 

hadoop jar $behe_home/core/target/behemoth-core-1.0-SNAPSHOT-job.jar com.digitalpebble.behemoth.util.CorpusGenerator -i $behe_home/gate/src/test/resources/docs -o textcorpus

hadoop jar $behe_home/core/target/behemoth-core-1.0-SNAPSHOT-job.jar com.digitalpebble.behemoth.util.CorpusGenerator -i $behe_home/gate/src/test/resources/docs -o textcorpus

# have a quick look at the content
hadoop fs -libjars $behe_home/core/target/behemoth-core-1.0-SNAPSHOT-job.jar -text textcorpus

# process with GATE
module=gate
hadoop fs -copyFromLocal $behe_home/$module/src/test/resources/ANNIE.zip ANNIE.zip
hadoop jar $behe_home/$module/target/behemoth-$module-1.0-SNAPSHOT-job.jar com.digitalpebble.behemoth.gate.GATEDriver -conf $behe_home/conf/behemoth-site.xml textcorpus textcorpusANNIE ANNIE.zip

# generate a XML corpus locally 
hadoop jar $behe_home/$module/target/behemoth-$module-1.0-SNAPSHOT-job.jar com.digitalpebble.behemoth.gate.GATECorpusGenerator -conf $behe_home/conf/behemoth-site.xml -i textcorpusANNIE -o GATEXMLCorpus

# have a look at the seqfile after processing using standard hadoop method
hadoop fs -libjars $behe_home/core/target/behemoth-core-1.0-SNAPSHOT-job.jar -text textcorpusANNIE/part-*

# process with Tika
module=tika
hadoop jar $behe_home/$module/target/behemoth-$module-1.0-SNAPSHOT-job.jar com.digitalpebble.behemoth.tika.TikaDriver -i textcorpus -o textcorpusTika 

# process with Language-ID
module=language-id
hadoop jar $behe_home/$module/target/behemoth-$module-1.0-SNAPSHOT-job.jar com.digitalpebble.behemoth.languageidentification.LanguageIdDriver -i textcorpusTika -o textcorpusTikaLang

# same but filter on language 
hadoop jar $behe_home/$module/target/behemoth-$module-1.0-SNAPSHOT-job.jar com.digitalpebble.behemoth.languageidentification.LanguageIdDriver -D document.filter.md.keep.lang=en -i textcorpusTika -o -o textcorpusTika-EN

#filter on mime type
hadoop jar $behe_home/$module/target/behemoth-$module-1.0-SNAPSHOT-job.jar com.digitalpebble.behemoth.util.CorpusFilter -D document.filter.mimetype.keep=.+html.* -i textcorpusTika -o textcorpusTika-html

#filter on url
hadoop jar $behe_home/$module/target/behemoth-$module-1.0-SNAPSHOT-job.jar com.digitalpebble.behemoth.util.CorpusFilter -D document.filter.url.keep=.+13.* -i textcorpusTika -o textcorpusTika-13

# process with UIMA
module=uima
hadoop fs -copyFromLocal $behe_home/$module/src/test/resources/WhitespaceTokenizer.pear WhitespaceTokenizer.pear
hadoop jar $behe_home/$module/target/behemoth-$module-1.0-SNAPSHOT-job.jar com.digitalpebble.behemoth.uima.UIMADriver -conf $behe_home/behemoth-site.xml textcorpusTika textcorpusUIMA WhitespaceTokenizer.pear

# generate vectors for Mahout
module=mahout
hadoop jar $behe_home/$module/target/behemoth-$module-1.0-SNAPSHOT-job.jar com.digitalpebble.behemoth.mahout.SparseVectorsFromBehemoth -i textcorpusUIMA -o textcorpus-vectors -t org.apache.uima.TokenAnnotation 

# processing a web archive
module=io
hadoop fs -copyFromLocal $behe_home/$module/src/test/resources/ClueWeb09_English_Sample.warc ClueWeb09.warc
hadoop jar $behe_home/$module/target/behemoth-io-1.0-SNAPSHOT-job.jar com.digitalpebble.behemoth.io.warc.WARCConverterJob -conf $behe_home/conf/behemoth-site.xml ClueWeb09.warc ClueWeb09
module=gate
hadoop jar $behe_home/$module/target/behemoth-$module-1.0-SNAPSHOT-job.jar com.digitalpebble.behemoth.gate.GATEDriver -conf $behe_home/conf/behemoth-site.xml ClueWeb09 ClueWeb09Annie ANNIE.zip

# corpus reader (useful for older version of Hadoop e.g. 0.18.x)
module=core
hadoop jar $behe_home/$module/target/behemoth-$module-1.0-SNAPSHOT-job.jar  com.digitalpebble.behemoth.util.CorpusReader -conf $behe_home/conf/behemoth-site.xml -i ClueWeb09Annie

# use of SOLR -> requires to have a SOLR instance running
module=solr
hadoop jar $behe_solr/$module/target/behemoth-$module-1.0-SNAPSHOT-job.jar com.digitalpebble.solr.SOLRIndexerJob ClueWeb09Annie http://69.89.5.5:8080/solr


