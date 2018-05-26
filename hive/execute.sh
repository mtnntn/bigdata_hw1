######################## FUNCTIONS #############################

hiveSubmit() {
	sleep 10
	echo -e "\n"
	echo -e "Starting JOB -> "$1
	/usr/bin/time -ao $2 hive -f  $1 |& tee $2	
}

bye(){
	echo -e "Bye... "
	echo -e "\t\t\t ¯\_(ツ)_/¯ "
	echo -e "\t\t\t\t\t\t ...MtNnTn"
}

########################### START JOBS ############################
            
hiveSubmit hive_0/0.hql hive_0_0  
hiveSubmit hive_0/1.hql hive_0_1
hiveSubmit hive_0/2.hql hive_0_2
hiveSubmit hive_0/3.hql hive_0_3
hiveSubmit hive_0/11.hql hive_0_11

hiveSubmit hive_1/0.hql hive_1_0
hiveSubmit hive_1/1.hql hive_1_1
hiveSubmit hive_1/2.hql hive_1_2
hiveSubmit hive_1/3.hql hive_1_3
hiveSubmit hive_1/11.hql hive_1_11

hiveSubmit hive_5/0.hql hive_5_0
hiveSubmit hive_5/1.hql hive_5_1
hiveSubmit hive_5/2.hql hive_5_2
hiveSubmit hive_5/3.hql hive_5_3
hiveSubmit hive_5/11.hql hive_5_11


############################# END ################################

bye

##################################################################
