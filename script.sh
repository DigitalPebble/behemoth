# example of commands to run from Hadoop

export behe_home=`pwd .`

mvn clean package 

hadoop jar $behe_home/core/target/behemoth-core-1.0-SNAPSHOT-job.jar com.digitalpebble.behemoth.util.CorpusGenerator $behe_home/gate/src/test/resources/docs textcorpus

# have a quick look at the content
hadoop fs -libjars $behe_home/core/target/behemoth-core-1.0-SNAPSHOT-job.jar -text textcorpus

# process with GATE
module=gate
hadoop fs -copyFromLocal $behe_home/$module/src/test/resources/ANNIE.zip ANNIE.zip
hadoop jar $behe_home/$module/target/behemoth-$module-1.0-SNAPSHOT-job.jar com.digitalpebble.behemoth.gate.GATEDriver -conf $behe_home/conf/behemoth-site.xml textcorpus textcorpusANNIE ANNIE.zip
# have a look at the seqfile after processing
hadoop fs -libjars $behe_home/core/target/behemoth-core-1.0-SNAPSHOT-job.jar -text textcorpusANNIE/part-*

# processing a web archive
module=io
hadoop fs -copyFromLocal $behe_home/$module/src/test/resources/ClueWeb09_English_Sample.warc ClueWeb09.warc
hadoop jar $behe_home/$module/target/behemoth-io-1.0-SNAPSHOT-job.jar com.digitalpebble.behemoth.io.warc.WARCConverterJob -conf $behe_home/conf/behemoth-site.xml ClueWeb09.warc ClueWeb09
module=gate
hadoop jar $behe_home/$module/target/behemoth-$module-1.0-SNAPSHOT-job.jar com.digitalpebble.behemoth.gate.GATEDriver -conf $behe_home/conf/behemoth-site.xml ClueWeb09 ClueWeb09Annie ANNIE.zip

# corpus reader (useful for older version of Hadoop e.g. 0.18.x)
module=core
hadoop jar $behe_home/$module/target/behemoth-$module-1.0-SNAPSHOT-job.jar  com.digitalpebble.behemoth.util.CorpusReader -conf $behe_home/conf/behemoth-site.xml ClueWeb09Annie

# use of SOLR -> requires to have a SOLR instance running
module=solr
hadoop jar $behe_solr/$module/target/behemoth-$module-1.0-SNAPSHOT-job.jar com.digitalpebble.solr.SOLRIndexerJob ClueWeb09Annie http://69.89.5.5:8080/solr


