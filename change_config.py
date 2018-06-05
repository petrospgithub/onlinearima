import ConfigParser
import os

config = ConfigParser.RawConfigParser()
config.read('./config/onlinearima.properties')

window = [
    ('4', '3'),
    ('6', '5'),
    ('8', '7'),
    ('10', '9')]

path = [
    ('blmd_4s.csv', '4000'),
    ('london_4s.csv', '4000'),
    ('blmd_8s.csv', '8000'),
    ('london_8s.csv', '8000'),
]

horizon=['32']

for w in window:
    for p in path:
        for h in horizon:
            cfgfile = open("./config/onlinearima.properties", 'w')
            config.set('Spark', 'spark.window', w[0])
            config.set('Spark', 'spark.train_set', w[1])

            config.set('Spark', 'spark.path', p[0])
            config.set('Spark', 'spark.sampling', p[1])

            config.set('Spark', 'spark.horizon', h)
            config.write(cfgfile)
            cfgfile.close()

            os.system('timeout 11m spark-submit --master local[*] --driver-memory 12g '
                      '--properties-file "./config/onlinearima.properties" '
                      '--class prediction.arma.OARMAGD '
                      './target/OnlineArimaTrajectory-jar-with-dependencies.jar ')

            os.system('timeout 11m spark-submit --master local[*] --driver-memory 12g '
                      '--properties-file "./config/onlinearima.properties" '
                      '--class prediction.arma.OARMANS '
                      './target/OnlineArimaTrajectory-jar-with-dependencies.jar ')

            os.system('timeout 11m spark-submit --master local[*] --driver-memory 12g '
                      '--properties-file "./config/onlinearima.properties" '
                      '--class prediction.arima.OArimaGD '
                      './target/OnlineArimaTrajectory-jar-with-dependencies.jar ')

            os.system('timeout 11m spark-submit --master local[*] --driver-memory 12g '
                      '--properties-file "./config/onlinearima.properties" '
                      '--class prediction.arima.OArimaNS '
                      './target/OnlineArimaTrajectory-jar-with-dependencies.jar ')