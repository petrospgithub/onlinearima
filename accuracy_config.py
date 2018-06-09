import ConfigParser
import os

config = ConfigParser.RawConfigParser()
config.read('./config/accuracy.properties')

horizon = [
    "1", "2", "4", "8", "16", "32"]

path = [
    ""
]

table = ["my_london", "my_spain"]

for p in path:
    for h in horizon:
        cfgfile = open("./config/onlinearima.properties", 'w')
        config.set('Spark', 'spark.path', p)
        config.set('Spark', 'spark.horizon', h)
        config.set('Spark', 'spark.table', t)

        config.write(cfgfile)
        cfgfile.close()

        os.system('spark-submit --master local[*] --driver-memory 12g '
                  '--properties-file "./config/accuracy.properties" '
                  '--class evaluation.Accuracy '
                  './target/OnlineArimaTrajectory-jar-with-dependencies.jar > error_'+p)
