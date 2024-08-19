import pandas as pd
import os
import shutil
from pyspark.sql import SparkSession

def repartition(load_dt, base_path='~/data2/extract/'):
    home_dir = os.path.expanduser(base_path)
    read_dir = os.path.join(home_dir,f'load_dt={load_dt}')

    # Parquet 파일 읽기
    df = pd.read_parquet(read_dir)

    # load_dt 컬럼 추가
    df[load_dt] = load_dt

    write_dir = os.path.join(home_dir,'repartition')


    rm_dir(write_dir)

    # print(df)
    

    # df.write.mode("overwrite").paquet(write_dir)
    
    return df

#  이미 존재 하는 폴더 삭제 func
def rm_dir(write_dir):
    if os.path.exists(write_dir):
        shutil.rmtree(write_dir)



def join_df(load_dt, base_path='~/data2/repartition/'):
    spark = SparkSession.builder.appName("spark_flow").getOrCreate()
    
    home_dir = os.path.expanduser(base_path)
    spark.read.parquet(home_dir)

    df1 = spark.filter(f'load_dt == {load_dt}')

    df1.createOrReplaceTempView("one_day")

    df2 = spark.sql(f"""
        SELECT 
            movieCd, -- 영화의 대표코드
            repNationCd -- 한국/외국 영화별로 조회할 수 있습니다. “K: : 한국영화 “F” : 외국영화
        FROM one_day
        WHERE multiMovieYn IS NULL
    """)

    df2.createOrReplaceTempView("multi_null")

    df3 = spark.sql(f"""
        SELECT 
            movieCd, -- 영화의 대표코드
            multiMovieYn -- 다양성 영화/상업영화를 구분지어 조회할 수 있습니다. “Y” : 다양성 영화 “N”
        FROM one_day
        WHERE repNationCd IS NULL
        """)
    df3.createOrReplaceTempView("multi_null")

    df_meta = spark.sql("""SELECT
            COALESCE(m.movieCd, n.movieCd) AS movieCd,
            multiMovieYn,
            repNationCd
        FROM multi_null m FULL OUTER JOIN nation_null n
        ON m.movieCd = n.movieCd
            """)
    
    ######################################################

    df2 = spark.sql(f"""
        SELECT 
            movieCd, -- 영화의 대표코드
            movieNm,
            salesAmt, -- 매출액
            audiCnt, -- 관객수
            showCnt, --- 사영횟수
            -- multiMovieYn, -- 다양성 영화/상업영화를 구분지어 조회할 수 있습니다. “Y” : 다양성 영화 “N”
            repNationCd, -- 한국/외국 영화별로 조회할 수 있습니다. “K: : 한국영화 “F” : 외국영화
            '{load_dt}' AS load_dt
        FROM one_day
        WHERE multiMovieYn IS NULL
        """)

    df2.createOrReplaceTempView("multi_null")

    df3 = spark.sql(f"""
        SELECT 
            movieCd, -- 영화의 대표코드
            movieNm,
            salesAmt, -- 매출액
            audiCnt, -- 관객수
            showCnt, --- 사영횟수
            multiMovieYn, -- 다양성 영화/상업영화를 구분지어 조회할 수 있습니다. “Y” : 다양성 영화 “N”
            -- repNationCd, -- 한국/외국 영화별로 조회할 수 있습니다. “K: : 한국영화 “F” : 외국영화
            '{load_dt}' AS load_dt
        FROM one_day
        WHERE repNationCd IS NULL
        """)

    df3.createOrReplaceTempView("nation_null")

    df_j = spark.sql(f"""
        SELECT
            COALESCE(m.movieCd, n.movieCd) AS movieCd,
            COALESCE(m.salesAmt, n.salesAmt), -- 매출액
            COALESCE(m.audiCnt, n.audiCnt), -- 관객수
            COALESCE(m.showCnt, n.showCnt), --- 사영횟수
            multiMovieYn, -- 다양성 영화/상업영화를 구분지어 조회할 수 있습니다. “Y” : 다양성 영화 “N”
            repNationCd, -- 한국/외국 영화별로 조회할 수 있습니다. “K: : 한국영화 “F” : 외국영화
            '{load_dt}' AS load_dt
        FROM multi_null m FULL OUTER JOIN nation_null n
        ON m.movieCd = n.movieCd""")

    df_j.createOrReplaceTempView("join_df") 

    
    df_j.write.mode('overwrite').partitionBy("load_dt", "multiMovieYn", "repNationCd").parquet("/home/diginori/data/movie/hive")

    return join_df



def agg():
    # sparksql 을 사용하여 일별 독립영화 여부, 해외영화 여부에 대하여 각각 합을 구하기(누적은 제외 일별관객수, 수익 ... )
    # 위에서 구한 SUM 데이터를 "/home//data/movie/sum-multi", "/home//data/movie/sum-nation" 에 날짜를 파티션 하여 저장
    pass