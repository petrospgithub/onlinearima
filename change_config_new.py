import ConfigParser
import os

config = ConfigParser.RawConfigParser()
config.read('./config/onlinearima.properties')

window = [
    ('4', '3'),
    ('6', '3')]

rate=['0.0001','0.00001','0.0001', '0.001', '0.01']

horizon='32'

for w in window:
    for r in rate:

        config = ConfigParser.RawConfigParser()
        config.read('./config/onlinearima.properties')
        cfgfile = open("./config/onlinearima.properties", 'w')
        config.set('Spark', 'spark.window', w[0])
        config.set('Spark', 'spark.train_set', w[1])
        config.set('Spark', 'spark.lrate', r)
        config.set('Spark', 'spark.epsilon', r)
        config.set('Spark', 'spark.vectorinit', 'linear')
        config.write(cfgfile)
        cfgfile.close()

        os.system('spark-submit --master local[*] '
                  '--properties-file "./config/onlinearima.properties" '
                  '--class prediction.arma.OARMAGD '
                  './target/OnlineArimaTrajectory-jar-with-dependencies.jar ')

        os.system(' spark-submit --master local[*] '
                  '--properties-file "./config/onlinearima.properties" '
                  '--class prediction.arma.OARMANS '
                  './target/OnlineArimaTrajectory-jar-with-dependencies.jar ')

        config = ConfigParser.RawConfigParser()
        config.read('./config/onlinearima.properties')
        cfgfile = open("./config/onlinearima.properties", 'w')
        config.set('Spark', 'spark.window', w[0])
        config.set('Spark', 'spark.train_set', w[1])
        config.set('Spark', 'spark.lrate', r)
        config.set('Spark', 'spark.epsilon', r)
        config.set('Spark', 'spark.vectorinit', 'random')
        config.write(cfgfile)
        cfgfile.close()

        os.system('spark-submit --master local[*] '
                  '--properties-file "./config/onlinearima.properties" '
                  '--class prediction.arma.OArimaGD '
                  './target/OnlineArimaTrajectory-jar-with-dependencies.jar ')

        os.system(' spark-submit --master local[*] '
                  '--properties-file "./config/onlinearima.properties" '
                  '--class prediction.arma.OArimaNS '
                  './target/OnlineArimaTrajectory-jar-with-dependencies.jar ')