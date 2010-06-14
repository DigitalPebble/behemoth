# example of commands to run from Hadoop

export behe_home=/data/behemoth-pebble

ant -f $behe_home/build.xml 

./hadoop jar $behe_home/build/behemoth-0.1-snapshot.job com.digitalpebble.behemoth.util.CorpusGenerator $behe_home/src/test/data/docs/ textcorpus
# have a quick look at the content
./hadoop fs -libjars $behe_home/build/behemoth-0.1-snapshot.job -text textcorpus
./hadoop fs -copyFromLocal $behe_home/src/test/data/ANNIE.zip ANNIE.zip
./hadoop jar $behe_home/build/behemoth-0.1-snapshot.job com.digitalpebble.behemoth.gate.GATEDriver -conf $behe_home/conf/behemoth-site.xml textcorpus textcorpusANNIE ANNIE.zip
# have a look at the seqfile after processing
./hadoop fs -libjars $behe_home/build/behemoth-0.1-snapshot.job -text textcorpusANNIE/part-*

# processing a web archive
./hadoop fs -copyFromLocal $behe_home/src/test/data/ClueWeb09_English_Sample.warc ClueWeb09.warc
./hadoop jar $behe_home/build/behemoth-0.1-snapshot.job com.digitalpebble.behemoth.io.warc.WARCConverterJob -conf $behe_home/conf/behemoth-site.xml ClueWeb09.warc ClueWeb09
./hadoop jar $behe_home/build/behemoth-0.1-snapshot.job com.digitalpebble.behemoth.gate.GATEDriver -conf $behe_home/conf/behemoth-site.xml ClueWeb09 ClueWeb09Annie ANNIE.zip

# corpus reader (useful for older version of Hadoop e.g. 0.18.x)
./hadoop jar $behe_home/build/behemoth-0.1-snapshot.job  com.digitalpebble.behemoth.util.CorpusReader -conf $behe_home/conf/behemoth-site.xml ClueWeb09Annie

# use of sandbox SOLR
export behe_solr=/data/behemoth-pebble/sandbox/solr-indexer 

ant -f $behe_home/build.xml  jar
ant -f $behe_solr/build.xml

./hadoop jar $behe_solr/build/behemoth-SOLR-0.1-snapshot.job com.digitalpebble.solr.SOLRIndexerJob ClueWeb09Annie http://69.89.5.5:8080/solr


