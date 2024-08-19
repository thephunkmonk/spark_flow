import pandas as pd
import os
import shutil
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def repartition(load_dt, base_path='~/data2/'):
    home_dir = os.path.expanduser(base_path)
    read_dir = os.path.join(home_dir,'extract',f'load_dt={load_dt}')

    # Parquet 파일 읽기
    df = pd.read_parquet(read_dir)

    # load_dt 컬럼 추가
    df['load_dt'] = load_dt

    write_dir = os.path.join(home_dir,'repartition')
    rm_dir(write_dir)

    df.to_parquet(
        write_dir,
        partition_cols=['load_dt','multiMovieYn','repNationCd']
        )
    
    return read_dir, write_dir, len(df)

#  이미 존재 하는 폴더 삭제 func
def rm_dir(write_dir):
    if os.path.exists(write_dir):
        shutil.rmtree(write_dir)



def join_df(load_dt, base_path='~/data2/repartition'):
    spark : SparkSession = SparkSession.builder.appName("spark_flow").getOrCreate()
    
    read_dir = os.path.expanduser(base_path)
    df = spark.read.parquet(read_dir)

    df1 = df.filter(F.col('load_dt') == load_dt)
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
    df3.createOrReplaceTempView("nation_null")

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
            COALESCE(m.salesAmt, n.salesAmt) as totalSalesAmt, -- 매출액
            COALESCE(m.audiCnt, n.audiCnt) as totalaudiCnt, -- 관객수
            COALESCE(m.showCnt, n.showCnt) as totalShowCnt, --- 사영횟수
            multiMovieYn, -- 다양성 영화/상업영화를 구분지어 조회할 수 있습니다. “Y” : 다양성 영화 “N”
            repNationCd, -- 한국/외국 영화별로 조회할 수 있습니다. “K: : 한국영화 “F” : 외국영화
            '{load_dt}' AS load_dt
        FROM multi_null m FULL OUTER JOIN nation_null n
        ON m.movieCd = n.movieCd""")

    df_j.createOrReplaceTempView("join_df") 

    home = os.path.expanduser("~/data2/")
    write_dir = os.path.join(home, "movie","hive")
    
    df_j.write.mode('overwrite').partitionBy("load_dt", "multiMovieYn", "repNationCd").parquet(write_dir)

    return read_dir, df_j.show()


def agg(load_dt,base_dir='~/data2/movie/hive'):
    # sparksql 을 사용하여 일별 독립영화 여부, 해외영화 여부에 대하여 각각 합을 구하기(누적은 제외 일별관객수, 수익 ... )
    # 위에서 구한 SUM 데이터를 "/home//data/movie/sum-multi", "/home//data/movie/sum-nation" 에 날짜를 파티션 하여 저장
    spark : SparkSession = SparkSession.builder.appName("spark_flow").getOrCreate()

    home_dir = os.path.expanduser(base_dir)

    df = spark.read.parquet(home_dir)

    filter_df = df.filter(F.col('load_dt') == load_dt)

    nation_k_df = filter_df.filter(F.col('repNationCd') == 'K')
    nation_y_df = filter_df.filter(F.col('repNationCd') == 'F')

    nation_y_df.createTempView('nation_y')
    nation_k_df.createTempView('nation_k')

    agg_df = spark.sql("""
    SELECT 
        avg(sum(y.totalaudiCnt)) as globalMovieAgg,
        avg(sum(k.totalaudiCnt)) as nationMovieAgg
    FROM nation_y as y FULL JOIN nation_k as k
              ON y.movieCd == k.movieCd
    """)

    write_dir = os.path.expanduser("~/data2/agg")

    agg_df.write.mode("overwrite").parquet(write_dir)

    return home_dir, write_dir, agg_df.show()
