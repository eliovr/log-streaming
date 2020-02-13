# Spark streaming example.

## Created for the Data Science master program of the University of Skövde.

Credits on the Apache Log Generator to [kiritbasu](https://github.com/kiritbasu/Fake-Apache-Log-Generator).

***

Folder 'loggen' contains:
* Python script for generating fake apache log files.
* Crontab example for executing the previous script every 5 seconds.

# Requirements
The steps below assume that you're using Linux, and that [Spark](https://spark.apache.org/)
and [SBT](https://www.scala-sbt.org/) have been installed.

# Deployment
1. Create two folders:
* One where log files will be saved, e.g., `/tmp/log-files/`
* Another for the checkpoints made by Spark, e.g., `/tmp/checkpoints/`

2. Update the paths in the source code correspondingly, e.g.:
```Scala
...
ssc.checkpoint("/tmp/checkpoints/")
...
val logs = ssc.textFileStream("/tmp/log-files/")
```

3. Update the paths of the `../loggen/fake-log-crontab` file, so that they point
to the logs folder, as well as to the `../loggen/apache-fake-log-gen.py`
Python script, e.g.:
```
* * * * * /path/to/log-streaming/loggen/apache-fake-log-gen.py -o LOG -n 200 -d /tmp/log-files/
* * * * * (sleep 5 ; /path/to/log-streaming/loggen/apache-fake-log-gen.py -o LOG -n 200 -d /tmp/log-files/)
* * * * * (sleep 10 ; /path/to/log-streaming/loggen/apache-fake-log-gen.py -o LOG -n 200 -d /tmp/log-files/)
...
```

4. Package the solution into a jar file using SBT.
```
cd /path/project/folder
sbt package
```

5. Start generating log files by copying the content of the `../loggen/fake-log-crontab`
file into crontab, i.e.:

```
crontab -e
```

6. Submit the jar to Spark to start streaming. For DStreams:
```
spark-submit --class "dstream.Example" --master local[*] target/scala-2.12/log-streaming_2.12-1.0.jar
```
For Structured streaming:
```
spark-submit --class "structured.Example" --master local[*] target/scala-2.12/log-streaming_2.12-1.0.jar
```

Once it is up and running, you should see the results from the streaming transformations
in the console. To stop the Spark streaming process, hit Ctr+C. To stop the generation of log
files, use `crontab -r` in the terminal.
