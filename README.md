# StateStats

## Overview
This program calculates various statistics for states using MapReduce on
Hadoop and data from Wikipedia. It strips HTML and searches Wikipedia pages
for the words "agriculture", "education", "politics", and "sports" (case
sensitive). It calculates the word usage for each state, the dominant words in
each state, and the rankings of words shared by the states.

## Setup
Clone this git repository:
```bash
git clone https://github.com/rxt1077/StateStats.git
cd StateStats
```
Make sure your HADOOP_CLASSPATH is setup correctly and the hadoop binary is in
your PATH, and YARN is running:
```bash
export PATH=$PATH:~/hadoop/bin
export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar
~/hadoop/sbin/start-yarn.sh
```
Add the states data to HDFS (if it isn't already):
```bash
hadoop fs -mkdir -p states
hadoop fs -copyFromLocal states
```
If you have old output data you will have to remove it:
```bash
hadoop fs -rm -r output_*
```

## Compiling
```bash
hadoop com.sun.tools.javac.Main StateStats.java
jar cf state_stats.jar StateStats*.class
```

## Running
The first argument is the directory in HDFS where we can find the data:
```bash
hadoop jar state_stats.jar StateStats states
```

## Output
* *output_statewise* - This directory has a part file with the occurences of the
search words used in each state. It has the form "state:word occurences".
* *output_dominant* - This directory has a part file the the dominant state word
usage in it. It has the form "word state".
* *output_ranking1* - This directory has a part file with every state and its
ranking of words. It has the form "ranking state".
* *output_ranking2* - This directory has a part file with every ranking and the
states that share it. It has the form "ranking state1,state2,state3..."
