# example of commands to run from Hadoop

export behe_home=${PWD}
echo "Behemoth home is ${behe_home}"

if [ -n "${HADOOP_HOME+x}" ]; then
echo "HADOOP_HOME is set to ${HADOOP_HOME}"
else
echo "HADOOP_HOME is not set - please configure to run script"
exit
fi

export HADOOP=bin/hadoop
export TESTDIR=test

cd $HADOOP_HOME
${HADOOP} fs -rmr ${TESTDIR}

# load corpus into HDFS
${HADOOP} jar ${behe_home}/modules/core/build/behemoth-core.job com.digitalpebble.behemoth.util.CorpusGenerator -i ${behe_home}/modules/gate/src/test/data/docs/ -o ${TESTDIR}/textcorpus

# have a quick look at the content
${HADOOP} fs -libjars ${behe_home}/modules/core/build/behemoth-core.job -text ${TESTDIR}/textcorpus

# need to call tika before GATE
${HADOOP} jar ${behe_home}/modules/tika/build/behemoth-tika.job com.digitalpebble.behemoth.tika.TikaDriver -i ${TESTDIR}/textcorpus -o ${TESTDIR}/textCorpusProcessedWithTika

# process dataset with GATE
${HADOOP} fs -copyFromLocal ${behe_home}/modules/gate/src/test/data/ANNIE.zip ${TESTDIR}/ANNIE.zip
${HADOOP} jar ${behe_home}/modules/gate/build/behemoth-gate.job com.digitalpebble.behemoth.gate.GATEDriver -conf ${behe_home}/modules/core/conf/behemoth-site.xml -i ${TESTDIR}/textCorpusProcessedWithTika -o ${TESTDIR}/textcorpusANNIE -g ${TESTDIR}/ANNIE.zip

# have a look at the seqfile after processing
${HADOOP} fs -libjars $behe_home/modules/core/build/behemoth-core.job -text ${TESTDIR}/textcorpusANNIE/part-*

# processing a web archive
${HADOOP} fs -copyFromLocal $behe_home/modules/io/src/test/data/ClueWeb09_English_Sample.warc ${TESTDIR}/ClueWeb09.warc
${HADOOP} jar $behe_home/modules/io/build/behemoth-io.job com.digitalpebble.behemoth.io.warc.WARCConverterJob -conf ${behe_home}/modules/core/conf/behemoth-site.xml -i ${TESTDIR}/ClueWeb09.warc -o ${TESTDIR}/ClueWeb09
${HADOOP} jar $behe_home/modules/gate/build/behemoth-gate.job com.digitalpebble.behemoth.gate.GATEDriver -conf ${behe_home}/modules/core/conf/behemoth-site.xml -i ${TESTDIR}/ClueWeb09 -o ${TESTDIR}/ClueWeb09Annie -g ${TESTDIR}/ANNIE.zip

# corpus reader (useful for older version of Hadoop e.g. 0.18.x)
${HADOOP} jar ${behe_home}/modules/core/build/behemoth-core.job com.digitalpebble.behemoth.util.CorpusReader -i ${TESTDIR}/ClueWeb09Annie

# use of SOLR
# commented out because SOLR server not running at 69.89.5.5
#${HADOOP} jar ${behe_home}/modules/solr/build/behemoth-solr.job com.digitalpebble.behemoth.solr.SOLRIndexerJob -i ${TESTDIR}/ClueWeb09Annie -l http://69.89.5.5:8080/solr

# process dataset with UIMA
${HADOOP} fs -copyFromLocal ${behe_home}/modules/uima/src/test/data/WhitespaceTokenizer.pear ${TESTDIR}/WhitespaceTokenizer.pear
${HADOOP} jar ${behe_home}/modules/uima/build/behemoth-uima.job com.digitalpebble.behemoth.uima.UIMADriver -conf ${behe_home}/modules/core/conf/behemoth-site.xml -i ${TESTDIR}/textCorpusProcessedWithTika -o ${TESTDIR}/textcorpusUIMA -p ${TESTDIR}/WhitespaceTokenizer.pear

