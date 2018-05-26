#!/usr/bin/env bash
######################## FUNCTIONS #############################

sparkSubmit() {
	echo -e "\n New Job -> " $1
	sleep 5
	/usr/bin/time -ao $4 spark-submit $1 $2 $3 |& tee $4
	sleep 5
}

sparkqlSubmit(){
	echo -e "\n New Job -> " $1
        sleep 5
        /usr/bin/time -ao $4 spark-submit --packages com.databricks:spark-csv_2.10:1.2.0 $1 $2 $3 |& tee $4
        sleep 5
}

bye(){
	echo -e "Bye... "
	echo -e "\t\t\t ¯\_(ツ)_/¯ "
	echo -e "\t\t\t\t\t\t ...MtNnTn"
}

########################### START JOBS ############################


#sparkSubmit bd_ex1_ai/Esercizio1.py /user/root/reviews_clean.csv /user/root/output_spark/0/1 spark_0_1

#sparkSubmit bd_ex2_ai/Esercizio2.py /user/root/reviews_clean.csv /user/root/output_spark/0/2 spark_0_2
#sparkSubmit bd_ex2_ai/Esercizio22.py /user/root/reviews_clean.csv /user/root/output_spark/0/22 spark_0_22
#sparkqlSubmit bd_ex2_ai/Esercizio23.py /user/root/reviews_clean.csv /user/root/output_spark/0/23 spark_0_23

#sparkSubmit bd_ex3_ai/Esercizio3.py /user/root/reviews_clean.csv /user/root/output_spark/0/3 spark_0_3
#sparkqlSubmit bd_ex3_ai/Esercizio32.py /user/root/reviews_clean.csv /user/root/output_spark/0/32 spark_0_32

####

#sparkSubmit bd_ex1_ai/Esercizio1.py /user/root/reviews_clean_3x.csv /user/root/output_spark/1/1 spark_1_1

#sparkSubmit bd_ex2_ai/Esercizio2.py /user/root/reviews_clean_3x.csv /user/root/output_spark/1/2 spark_1_2
#sparkSubmit bd_ex2_ai/Esercizio22.py /user/root/reviews_clean_3x.csv /user/root/output_spark/1/22 spark_1_22
#sparkqlSubmit bd_ex2_ai/Esercizio23.py /user/root/reviews_clean_3x.csv /user/root/output_spark/1/23 spark_1_23

#sparkSubmit bd_ex3_ai/Esercizio3.py /user/root/reviews_clean_3x.csv /user/root/output_spark/1/3 spark_1_3
#sparkqlSubmit bd_ex3_ai/Esercizio32.py /user/root/reviews_clean_3x.csv /user/root/output_spark/1/32 spark_1_32

#####

sparkSubmit bd_ex1_ai/Esercizio1.py /user/root/reviews_clean_17x.csv /user/root/output_spark/5/1 spark_5_1  

sparkSubmit bd_ex2_ai/Esercizio2.py /user/root/reviews_clean_17x.csv /user/root/output_spark/5/2 spark_5_2
sparkSubmit bd_ex2_ai/Esercizio22.py /user/root/reviews_clean_17x.csv /user/root/output_spark/5/22 spark_5_22
sparkqlSubmit bd_ex2_ai/Esercizio23.py /user/root/reviews_clean_17x.csv /user/root/output_spark/5/23 spark_5_23

sparkSubmit bd_ex3_ai/Esercizio3.py /user/root/reviews_clean_17x.csv /user/root/output_spark/5/3 spark_5_3
sparkqlSubmit bd_ex3_ai/Esercizio32.py /user/root/reviews_clean_17x.csv /user/root/output_spark/5/32 spark_5_32


############################# END ################################

bye
