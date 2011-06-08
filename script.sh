# Simple test script runs various Behemoth jobs at the command line

export behe_home=${PWD}
echo "Behemoth home is ${behe_home}"

if [ -n "${HADOOP_HOME+x}" ]; then
echo "HADOOP_HOME is set to ${HADOOP_HOME}"
else
echo "HADOOP_HOME is not set - please configure to run script"
exit
fi

export TESTDIR=test
export BEHEMOTH_JOB=${behe_home}/modules/cli/build/behemoth-cli.job
export HADOOP=bin/hadoop
export RUN_BEHEMOTH="${HADOOP} jar ${BEHEMOTH_JOB}"

cd $HADOOP_HOME

echo "Deleting test directory on HDFS"

${HADOOP} fs -rmr ${TESTDIR}

echo "Converting corpus into Behemoth Documents and storing on HDFS"

${RUN_BEHEMOTH} CorpusGenerator -i ${behe_home}/modules/gate/src/test/data/docs/ -o ${TESTDIR}/textcorpus

echo "Printing contents of corpus"

${HADOOP} fs -libjars ${BEHEMOTH_JOB} -text ${TESTDIR}/textcorpus

echo "Parsing corpus with Tika"

${RUN_BEHEMOTH} TikaDriver -i ${TESTDIR}/textcorpus -r

echo "Copying GATE application to HDFS"

${HADOOP} fs -copyFromLocal ${behe_home}/modules/gate/src/test/data/ANNIE.zip ${TESTDIR}/ANNIE.zip

echo "Processing corpus with GATE"

${RUN_BEHEMOTH} GATEDriver -conf ${behe_home}/modules/core/conf/behemoth-site.xml -i ${TESTDIR}/textcorpus -o ${TESTDIR}/textcorpusGATE -g ${TESTDIR}/ANNIE.zip

echo "Examining output of GATE job"

${HADOOP} fs -libjars ${BEHEMOTH_JOB} -text ${TESTDIR}/textcorpusGATE/part-*

echo "Copying Web archive file to HDFS"

${HADOOP} fs -copyFromLocal $behe_home/modules/io/src/test/data/ClueWeb09_English_Sample.warc ${TESTDIR}/ClueWeb09.warc

echo "Ingesting a Web Archive as Behemoth Documents"

${RUN_BEHEMOTH} WARCConverterJob -conf ${behe_home}/modules/core/conf/behemoth-site.xml -i ${TESTDIR}/ClueWeb09.warc -o ${TESTDIR}/ClueWeb09

echo "Parsing web archive data with Tika"

${RUN_BEHEMOTH} TikaDriver -i ${TESTDIR}/ClueWeb09 -r

echo "Examining output of Web Archive"

${RUN_BEHEMOTH} CorpusReader -i ${TESTDIR}/ClueWeb09

# use of SOLR
# commented out because SOLR server not running at 69.89.5.5
#${RUN_BEHEMOTH} SOLRIndexerJob -i ${TESTDIR}/ClueWeb09 -l http://localhost:8983/solr

echo "Copy UIMA application to HDFS"

${HADOOP} fs -copyFromLocal ${behe_home}/modules/uima/src/test/data/WhitespaceTokenizer.pear ${TESTDIR}/WhitespaceTokenizer.pear

echo "Processing dataset with UIMA"

${RUN_BEHEMOTH} UIMADriver -conf ${behe_home}/modules/core/conf/behemoth-site.xml -i ${TESTDIR}/textcorpus -o ${TESTDIR}/textcorpusUIMA -p ${TESTDIR}/WhitespaceTokenizer.pear

echo "Copying a Nutch segment to HDFS"

${HADOOP} fs -copyFromLocal $behe_home/modules/io/src/test/data/20110602184218 ${TESTDIR}/nutch20110602184218

echo "Ingesting a Nutch segment as Behemoth Documents"

${RUN_BEHEMOTH} NutchSegmentConverterJob -conf ${behe_home}/modules/core/conf/behemoth-site.xml -i ${TESTDIR}/nutch20110602184218 -o ${TESTDIR}/nutchIngest

echo "Parsing the Nutch segment using Tika"

${RUN_BEHEMOTH} TikaDriver -i ${TESTDIR}/nutchIngest -r

echo "Examining output of Nutch segment"

${RUN_BEHEMOTH} CorpusReader -i ${TESTDIR}/nutchIngest

echo "Tests completed"

