import pandas as pd
import numpy as np
import pymysql

conn = pymysql.connect(host = 'match-stats.null.null2.rds.amazonaws.com',
                       port = 3306,
                       user = 'username',
                       password = 'password',
                       db = 'pubg-esports')