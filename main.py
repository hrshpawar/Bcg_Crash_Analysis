from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, sum, countDistinct, first, desc, rank, count, row_number
import yaml


class CrashAnalysis:

    def __init__(self, input_file_path_dict):

        # print(input_file_paths.get("charges"))
        self.df_charges = spark.read.csv(path=input_file_path_dict.get("charges"), sep=',', header=True).dropDuplicates()
        self.df_damages = spark.read.csv(path=input_file_path_dict.get("damages"), sep=',', header=True).dropDuplicates()
        self.df_endorsements = spark.read.csv(path=input_file_path_dict.get("endorse"), sep=',',
                                              header=True).dropDuplicates()
        self.df_person = spark.read.csv(path=input_file_path_dict.get("primary_person"), sep=',',
                                        header=True).dropDuplicates()
        self.df_restrict = spark.read.csv(path=input_file_path_dict.get("restricts"), sep=',', header=True).dropDuplicates()
        self.df_unit = spark.read.csv(path=input_file_path_dict.get("units"), sep=',', header=True).dropDuplicates()

    def male_crashes(self, output_path):
        """
        Find the number of crashes (accidents) in which number of persons killed are male?
        :return: write output in csv file, count
        """
        df1 = self.df_person.filter((col("PRSN_INJRY_SEV_ID") == 'KILLED') & (col("prsn_gndr_id") == 'MALE'))\
            .agg(countDistinct("crash_id").alias("count_distinct_crashes_male_killed"))

        df1.write.options(header='True', delimiter=',').mode('overwrite').csv(output_path)

    def two_weelers_crashes(self, output_path):
        """
        How many two wheelers are booked for crashes?
        :return: write output in csv file, count
        """
        df2 = self.df_unit.filter((col("veh_body_styl_id") == 'MOTORCYCLE')).\
            agg(countDistinct("vin").alias("count_distinct_vin"))

        df2.write.options(header='True', delimiter=',').mode('overwrite').csv(output_path)

    def max_female_crashes_state(self, output_path):
        """
        Which state has highest number of accidents in which females are involved?
        :return: write output in csv file, state
        """
        df3 = self.df_person.filter(col("prsn_gndr_id") == "FEMALE").groupBy("drvr_lic_state_id") \
            .agg(countDistinct("crash_id").alias("count_crashes")) \
            .orderBy("count_crashes", ascending=False)
        df3 = df3.select(first("drvr_lic_state_id").alias("state_with_highest_crashes_female"))

        df3.write.options(header='True', delimiter=',').mode('overwrite').csv(output_path)

    def top_5th_to_15th_veh_make_id(self, output_path):
        """
        Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
        :return: write output in csv file, veh_make_id
        """
        df4 = self.df_unit.groupBy("veh_make_id").agg(sum("tot_injry_cnt").alias("tot_injry_cnt"),
                                                      sum("death_cnt").alias("death_cnt"))
        df4 = df4.withColumn("total_injuries", col("tot_injry_cnt") + col("death_cnt"))

        w = Window.orderBy(desc("total_injuries"))

        df4 = df4.select("veh_make_id", "total_injuries", rank().over(w).alias("rank"))
        df4 = df4.filter((col("rank") >= 5) & (col("rank") <= 15)).select(col("veh_make_id"))

        df4.write.options(header='True', delimiter=',').mode('overwrite').csv(output_path)
        # revisit to check veh_make_id NA

    def top_ethenic_body_type(self, output_path):
        """
        For all the body styles involved in crashes, mention the top ethnic user group of each unique body style
        :return: write output in csv file, veh_body_styl_id, prsn_ethnicity_id
        """
        df_unit_person = self.df_unit.join(self.df_person, ["crash_id", "unit_nbr"], "inner") \
            .select("crash_id", "veh_body_styl_id", "prsn_ethnicity_id") \
            .distinct()
        df_unit_person = df_unit_person.groupBy("veh_body_styl_id", "prsn_ethnicity_id") \
            .agg(count("crash_id").alias("count"))

        w = Window.partitionBy("veh_body_styl_id").orderBy(desc("count"))

        df_unit_person = df_unit_person.select("veh_body_styl_id", "prsn_ethnicity_id", row_number().over(w).alias("rn"))
        df_unit_person = df_unit_person.filter(col("rn") == 1).select(col("veh_body_styl_id"), col("prsn_ethnicity_id"))
        # df_unit_person.show()
        # revisit to check whether to consider NA in veh_body_styl_id
        df_unit_person = df_unit_person.coalesce(1)
        df_unit_person.write.options(header='True', delimiter=',').mode('overwrite').csv(output_path)

    def top_5_zipcodes_alcohol(self, output_path):
        """
        Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes
        with alcohols as the contributing factor to a crash (Use Driver Zip Code)
        :return: write output in csv file, top 5 zip code with highest number crashes
        """
        df_unit_person = self.df_unit.join(self.df_person, ["CRASH_ID", "UNIT_NBR"], "INNER"). \
            dropna(subset=["DRVR_ZIP"]). \
            filter((col("CONTRIB_FACTR_1_ID").contains("ALCOHOL") | col("CONTRIB_FACTR_2_ID").contains("ALCOHOL")
                    | col("CONTRIB_FACTR_P1_ID").contains("ALCOHOL")) & col("VEH_BODY_STYL_ID").contains("CAR")) \
            .select("CRASH_ID", "DRVR_ZIP").distinct()
        df_unit_person = df_unit_person.groupby("DRVR_ZIP").count().orderBy(col("COUNT").desc()).limit(5)
        df_unit_person = df_unit_person.select(col("DRVR_ZIP"))

        df_unit_person.write.options(header='True', delimiter=',').mode('overwrite').csv(output_path)

    def count_distinct_crash_id_no_damages(self, output_path):
        """
        Count of Distinct Crash IDs where No Damaged Property was observed and
        Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
        :return: write output in csv file, Count of Distinct Crash IDs
        """
        df_damages_unit = self.df_damages.join(self.df_unit, ["CRASH_ID"], 'inner'). \
            filter(
            ((self.df_unit.VEH_DMAG_SCL_1_ID > "DAMAGED 4") & (
                ~self.df_unit.VEH_DMAG_SCL_1_ID.isin(["NA", "NO DAMAGE", "INVALID VALUE"])))
            | ((self.df_unit.VEH_DMAG_SCL_2_ID > "DAMAGED 4") & (
                ~self.df_unit.VEH_DMAG_SCL_2_ID.isin(["NA", "NO DAMAGE", "INVALID VALUE"])))
        ).filter(self.df_damages.DAMAGED_PROPERTY == "NONE").\
            filter(self.df_unit.FIN_RESP_TYPE_ID.contains("INSURANCE"))\
            .groupby().agg(countDistinct("crash_id").alias("count_distinct_crash_id"))

        df_damages_unit.write.options(header='True', delimiter=',').mode('overwrite').csv(output_path)

    def top_5_vehicle_speed_charged(self, output_path):
        """
        Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences,
        has licensed Drivers, used top 10 used vehicle colours and has car licensed
        with the Top 25 states with highest number of offences (to be deduced from the data)
        :return: write output in csv file, Top 5 Vehicle Brand
        """
        df_top_10_vehicle_colors = self.df_unit.filter(col("VEH_COLOR_ID") != "NA"). \
            groupby("VEH_COLOR_ID").agg(countDistinct("crash_id", "unit_nbr").alias("count")) \
            .orderBy(col("count").desc()).limit(10)

        top_10_vehicle_colors_list = [row[0] for row in df_top_10_vehicle_colors.collect()]

        df_top_25_state = self.df_unit.filter(
            (col("VEH_LIC_STATE_ID") != "NA") & (col("VEH_LIC_STATE_ID") != "98")).groupBy(
            "VEH_LIC_STATE_ID").agg(countDistinct("crash_id").alias("crash_count")) \
            .orderBy(desc("crash_count")).limit(25)

        top_25_state_list = [row[0] for row in df_top_25_state.collect()]

        df_top_5_vehicle = self.df_charges.join(self.df_person, ['CRASH_ID', 'UNIT_NBR', 'PRSN_NBR'], 'INNER'). \
            join(self.df_unit, ['CRASH_ID', 'UNIT_NBR'], 'INNER'). \
            filter(self.df_charges.CHARGE.contains("SPEED")). \
            filter(self.df_person.DRVR_LIC_TYPE_ID.isin(["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."])). \
            filter(self.df_unit.VEH_COLOR_ID.isin(top_10_vehicle_colors_list)). \
            filter(self.df_unit.VEH_LIC_STATE_ID.isin(top_25_state_list)). \
            groupby("VEH_MAKE_ID").agg(count("*").alias("count")). \
            orderBy(col("count").desc()).limit(5)
        df_top_5_vehicle = df_top_5_vehicle.select(col("VEH_MAKE_ID"))

        df_top_5_vehicle.write.options(header='True', delimiter=',').mode('overwrite').csv(output_path)


if __name__ == '__main__':
    # spark session
    conf = SparkConf().setAppName('CrashAnalysis').setMaster('local')
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)
    spark.sparkContext.setLogLevel("ERROR")

    # input/output path available in config file.
    config_file_path = "./config/config.yaml"

    with open(config_file_path, "r") as f:
        config = yaml.safe_load(f)

    input_file_paths = config.get("input_path")
    output_file_paths = config.get("output_path")

    crash_analysis = CrashAnalysis(input_file_paths)

# Analysis 1: Find the number of crashes (accidents) in which number of persons killed are male?
    crash_analysis.male_crashes(output_file_paths.get("path1"))
    print("Analysis 1 Completed, see the output file in Output Folder")

# Analysis 2: How many two wheelers are booked for crashes?
    crash_analysis.two_weelers_crashes(output_file_paths.get("path2"))
    print("Analysis 2 Completed, see the output file in Output Folder")

# Analysis 3: Which state has highest number of accidents in which females are involved?
    crash_analysis.max_female_crashes_state(output_file_paths.get("path3"))
    print("Analysis 3 Completed, see the output file in Output Folder")

# Analysis 4: Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
    crash_analysis.top_5th_to_15th_veh_make_id(output_file_paths.get("path4"))
    print("Analysis 4 Completed, see the output file in Output Folder")

# Analysis 5: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style
    crash_analysis.top_ethenic_body_type(output_file_paths.get("path5"))
    print("Analysis 5 Completed, see the output file in Output Folder")

# Analysis 6: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes
# with alcohols as the contributing factor to a crash (Use Driver Zip Code
    crash_analysis.top_5_zipcodes_alcohol(output_file_paths.get("path6"))
    print("Analysis 6 Completed, see the output file in Output Folder")

# Analysis 7: Count of Distinct Crash IDs where No Damaged Property was observed and
# Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
    crash_analysis.count_distinct_crash_id_no_damages(output_file_paths.get("path7"))
    print("Analysis 7 Completed, see the output file in Output Folder")

# Analysis 8: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences,
# has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states
# with highest number of offences (to be deduced from the data)
    crash_analysis.top_5_vehicle_speed_charged(output_file_paths.get("path8"))
    print("Analysis 8 Completed, see the output file in Output Folder")

    print("All Analysis Completed, Please see the Output csv files in Output Folder")
    spark.stop()
