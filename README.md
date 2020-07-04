repo文件介绍：\
schemaInfor -- 各raw data数据文件的schema信息，对于ETL很有帮助。\
taxi_zone_geom.csv -- 纽约分区ID与分区名字的映射关系，可以丰富可视化效果和分析维度。\
Athena Create Scirpts -- 用于创建AWS Athena表的sql语句，此表用于与AWS Quciksight对接。\
taxiTotal -- ETL逻辑，用于AWS云计算平台，其中s3 path hard code，如果需要在jupyter notebook中运行，请自行修改相关内容。\
priceRegression -- fare_amount与trip_distance的线性回归分析。\
Trend -- ETL逻辑，用于google cloud platform

   *Spearman 相关系数：\
    fare_amount vs trip_distance :      0.9160795764969094\
    fare_amount vs Pickup_latitude :   -0.1035135803686105\
    fare_amount vs Pickup_longitude :   0.05064398224865199\
    所以这里只选取 fare_amount vs trip_distance 做相关分析。
    

   * Green 预测结果
   * 斜率：[2.5488253446537765]
   * 截距：6.131967138375027
   * R squat：0.8108542493360649
   * RMSE：5.339834796117596
   * MAE：2.678594766789961
   *
   * Yellow 预测结果
   * 斜率：[2.666984626667545]
   * 截距：5.526849903171023
   * R squat：0.7083570810108307
   * RMSE：7.508360165966041
   * MAE：2.230689943528214
   

