#---------------------------------------
# SCRIPT	: crm_sh_mod.R
# DESC		: Self-help modeling for CRM
# AUTHOR	: hanbing
# VERSION	: v1.0
# DATE		: 20190915
# RUN-WITH	: R 3.2.2, SparkR 2.2.0
#---------------------------------------

# close connections and remove lists
rm(list=ls())		# Clear all vectors

# Global variables
filename<-"/app/spark/crm_sh_mod.log"
masterName<-"yarn"
deployMode<-"client"
executorNum<-"2"
executorCores<-"2"
executorMemory<-"1g"

# Load packages
options(scipen=200)	# Close scientific format
options(warn=-1)	# Close warning message
if(nchar(Sys.getenv("SPARK_HOME"))<1) Sys.setenv(SPARK_HOME="/usr/hdp/2.6.3.0-235/spark2")
library(SparkR, lib.loc=c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
library(jsonlite)
	
# Function definition
describing<-function(qstr, vtype, count){
	query_str<-tolower(qstr)	# tolower is necessary for column names
	var_type<-vtype				# variable type(non-flag:0, flag:1)
	ct<-as.integer(count)		# more time needed if using nrow method  
	
	if(!(var_type %in% c(1,0))){
		msg<-paste("Unrecognized VAR_TYPE value:", var_type, sep="")
		cat("[EXCEPTION]", msg, "\n", file=filename, append=T)
		return(c(1, msg))
	}
		
	run_id<-paste("[D", gsub(":", "", gsub(" ", "", (gsub("-", "", Sys.time())))), "]", sep="")
	cat(as.character(Sys.time()), "\t", run_id, "【describing】\n", file=filename, append=T)

	# Initial session
    msg<-try({sparkR.session(appName="describing"
            , master=masterName
            , sparkConfig=list(spark.submit.deployMode=deployMode
			        , spark.executor.instances=executorNum
                    , spark.executor.cores=executorCores
					, spark.executor.memory=executorMemory
            ))}, silent=TRUE)
	if(class(msg)=="jobj"){
		cat(as.character(Sys.time()), "\t", run_id, "SparkR session created \n", file=filename, append=T)
		setLogLevel("ERROR")
	}else if('try-error' %in% class(msg)){
		msg<-strsplit(attr(msg, "condition")[1]$message, "\n")[[1]][1]
		cat("[EXCEPTION]", msg, "\n", file=filename, append=T)
		return(c(1, msg))
	}else{
		msg<-"Unkown error, check your initial-session statement"
		cat("[EXCEPTION]", msg, "\n", file=filename, append=T)
		return(c(1, msg))
	}
	
	# Constant definition
	max_col<-6
	min_count<-1000
	max_count<-1e+08
	max_his_count<-1e+06
	
	# Load Dataset
	msg<-try({mydata<-sql(query_str)}, silent=TRUE)
	# cat...

	if(class(msg)[1]=="SparkDataFrame" && length(colnames(msg)!=0)){
		
		cache(mydata)
		ncolumn<-ncol(mydata)
	
		if(ncolumn< 2 | ncolumn > (max_col+1)){# including label column
			msg<-paste("Too less or many variables(", ncolumn-1, "/", max_col, ")", sep="")
			cat("[EXCEPTION]", msg, "\n", file=filename, append=T)
			sparkR.session.stop()
			return(c(1, msg))
		}
		
		if("character" %in% coltypes(mydata)){
			msg<-"Illegal variable column type(character type not allowed)"
			cat("[EXCEPTION]", msg, "\n", file=filename, append=T)
			sparkR.session.stop()
			return(c(1, msg))
		}
		
		#ct<-nrow(mydata)
		cat(as.character(Sys.time()), "\t", run_id, "ct:", ct, "\n", file=filename, append=T)

		if(ct < min_count | ct > max_count){
			msg<-paste("Input dataset(", ct, ") is smaller than MIN_COUNT(", min_count, ") or larger than MAX_COUNT(", max_count, ")", sep="")
			cat("[EXCEPTION]", msg, "\n", file=filename, append=T)
			sparkR.session.stop()
			return(c(1, msg))
		}
		
		col_names<-colnames(mydata)
		if(col_names[ncolumn]!="label"){
			names(mydata)[ncolumn]<-"label"	
		}
			
		# Generate Describe
		# #	summary
		# #1   count
		# #2    mean
		# #3  stddev
		# #4     min
		# #5     max
		desc<-head(describe(mydata[, -ncolumn]))		# "character"(summary, c1 ~ c24)
		row_names<-desc[,1]								# [,1](character) is different with [1](list)
		desc<-round(apply(desc[-1], 2, as.numeric), 3)	# Remove first column(summary) and desc-type changes from "list" to "double"
		desc<-as.data.frame(desc)						# Recover desc-type from "double" to "list"
		rownames(desc)<-row_names
		cat(as.character(Sys.time()), "\t", run_id, "desc(count/mean/stddev/min/max) complete \n", file=filename, append=T)
				
		# Count NA
		desc_nrow<-nrow(desc)	# Nrow will change as add-value operations
		for(i in 1:ncol(desc)){
			nan_ct<-0
			is_nan<-select(mydata, isNaN(column(col_names[i])))	# Or use is.nan
			#head_nan<-head(count(groupBy(is_nan, names(is_nan))))
			nan_ct<-nrow(filter(is_nan, column(names(is_nan))==TRUE))
			# if(length(head_nan[which(head_nan[1]==TRUE),2])!=0){
				# nan_ct<-head_nan[which(head_nan[1]==TRUE),2]
			# }
			desc[desc_nrow+1,i]<-nan_ct
		}
		rownames(desc)[desc_nrow+1]<-"na"
		cat(as.character(Sys.time()), "\t", run_id, "na complete \n", file=filename, append=T)
	
		# Sampling data
		#mydata<-ifelse(ct<(1.5*max_his_count), mydata, sample(mydata, FALSE, max_his_count/ct, 1234))	# Error:attempt to replicate an object of type 'S4'
		if(ct>(1.5*max_his_count)){
			mydata<-sample(mydata, FALSE, max_his_count/ct, 1234)
			cat(as.character(Sys.time()), "\t", run_id, "sampling complete \n", file=filename, append=T)
		}

		# Calculate Correlation
		desc_nrow<-nrow(desc)
		for(i in 1:ncol(desc)){
			corr_value<-corr(mydata, col_names[i], "label")
			desc[desc_nrow+1,i]<-ifelse(base::is.nan(corr_value), 0, corr_value)
		}
		rownames(desc)[desc_nrow+1]<-"corr"
		cat(as.character(Sys.time()), "\t", run_id, "corr complete \n", file=filename, append=T)
	
		# Generate median sql
		pct<-0.5
		b<-99
		med_sql<-"select 0"
		for(i in 1:(ncolumn-1)){	# Exclude c25(label)
			med_sql<-paste(med_sql, ", percentile_approx(", col_names[i], ", ", pct, ", ", b, ")", " as ", col_names[i], sep="")
			if(i==(ncolumn-1)){
				med_sql<-paste(med_sql, " from mydata", sep="")
			}
		}
		#cat("[SQL]******", med_sql, "******\n", file=filename, append=T)
		
		# Calculate median value
		createOrReplaceTempView(mydata, "mydata")
		msg<-try({med_list<-sql(med_sql)}, silent=TRUE)
		# cat...
		if(class(msg)[1]=="SparkDataFrame" && length(colnames(msg)!=0)){
			med<-first(med_list)	# list(0, c1 ~ c24)
			rownames(med)<-"median"
			desc<-rbind(desc, med[-1])
			cat(as.character(Sys.time()), "\t", run_id, "median complete \n", file=filename, append=T)
		}else if('try-error' %in% class(msg)){
			msg<-strsplit(attr(msg, "condition")[1]$message, "\n")[[1]][1]
			cat("[EXCEPTION]", msg, "\n", file=filename, append=T)
			sparkR.session.stop()
			return(c(1, msg))
		}else{
			msg<-"Unkown error, check your query sql statement(med_list)"
			cat("[EXCEPTION]", msg, "\n", file=filename, append=T)
			sparkR.session.stop()
			return(c(1, msg))
		}
		
		# Make return matrix
		desc<-as.data.frame(t(desc))	# t(desc) will change desc-type from "list" to "double"
		desc<-cbind(desc, ifelse(desc[,"corr"]>0, 1, ifelse(desc[,"corr"]<0, -1, 0)))
		desc[,"corr"]<-abs(desc[,"corr"])
		colnames(desc)[ncol(desc)]<-"effect" 
		desc<-desc[,c("min"
			,"mean"
			,"median"
			,"max"
			,"stddev"
			,"na"
			,"corr"
			,"effect")]
		colnames(desc)<-c("minValue"
			,"avgValue"
			,"medianValue"
			,"maxValue"
			,"standardDeviation"
			,"misValueNum"
			,"correlationSize"
			,"effectType")
		desc_json<-toJSON(as.data.frame(desc), pretty=TRUE, rownames=TRUE)	# "_row"
		desc_json<-gsub("_row", "objId", desc_json)
		#cat("[DESC_JSON] \n", desc_json, "\n", file=filename, append=T)
		
		# Make hist data
		his_list<-NULL
		nbs<-ifelse(var_type, 2, 10)
		for(i in 1:(ncolumn-1)){	# Exclude c25(label)
			his_list[[i]]<-histogram(mydata, col_names[i], nbins=nbs)
			his_list[[i]]['counts']<-his_list[[i]]['counts']/sum(his_list[[i]]['counts'])
			# if variable has a single value
			if(nrow(his_list[[i]])==nbs+1 & his_list[[i]][1,'bins']!=-1){
				his_list[[i]]<-his_list[[i]][nrow(his_list[[i]]),]
				his_list[[i]]['bins']<-0
				his_list[[i]]['centroids']<-desc[col_names[i],'minValue']
			}else if(var_type) his_list[[i]]['centroids']<-as.data.frame(c(0,1))
			names(his_list)[i]<-col_names[i]
		}
		his_json<-toJSON(his_list, pretty=TRUE)
		cat(as.character(Sys.time()), "\t", run_id, "hist complete \n", file=filename, append=T)
		#cat("[HIS_JSON] \n", his_json, "\n", file=filename, append=T)
		
		sparkR.session.stop()
		cat(as.character(Sys.time()), "\t", run_id, "session stopped \n", file=filename, append=T)
		return(c(0, ct, desc_json, his_json))

	}else if("try-error" %in% class(msg)){
		msg<-strsplit(attr(msg, "condition")[1]$message, "\n")[[1]][1]
		cat("[EXCEPTION]", msg, "\n", file=filename, append=T)
		sparkR.session.stop()
		return(c(1, msg))
	}else{
		msg<-"Unkown error, check your query sql statement(mydata)"
		cat("[EXCEPTION]", msg, "\n", file=filename, append=T)
		sparkR.session.stop()
		return(c(1, msg))
	}	

# end of function
}	


# Function definition
modeling<-function(teller, qstr, model){

	teller_no<-as.character(teller)
	query_str<-tolower(qstr)	# necessary for column names
	model_name<-model			# DT/LG
	#stat_level<-level			# L1/L2
	#ct<-as.integer(count)		# more time needed if using nrow method  
	
	run_id<-paste("[M", gsub(":", "", gsub(" ", "", (gsub("-", "", Sys.time())))), "]", sep="")
	cat(as.character(Sys.time()), "\t", run_id, "【modeling:", teller_no, "/", model_name, "】\n", file=filename, append=T)

	# Initial session
    msg<-try({sparkR.session(appName="modeling"
            , master=masterName
            , sparkConfig=list(spark.submit.deployMode=deployMode
			        , spark.executor.instances=executorNum
                    , spark.executor.cores=executorCores
					, spark.executor.memory=executorMemory
            ))}, silent=TRUE)
	if(class(msg)=="jobj"){
		cat(as.character(Sys.time()), "\t", run_id, "SparkR session created \n", file=filename, append=T)
		setLogLevel("ERROR")
	}else if('try-error' %in% class(msg)){
		msg<-strsplit(attr(msg, "condition")[1]$message, "\n")[[1]][1]
		cat("[EXCEPTION]", msg, "\n", file=filename, append=T)
		return(c(1, msg))
	}else{
		msg<-"Unkown error, check your initial-session statement"
		cat("[EXCEPTION]", msg, "\n", file=filename, append=T)
		return(c(1, msg))
	}
	
	# Constant definition
	fcol_names<-c("cust_no","cust_name","belong_inst")				# front columns
	fcol_types<-c("string","string","string")						# front column types
	scol_names<-c("asset_manage_bal","dep_bal","hold_prodt_num")	# statistic variables
	scol_types<-c("double","double","int")							# statistic variables types
	stat_types<-c("sum","sum","avg")								# statistic types
	min_count<-1000
	max_count<-1e+08
	max_train_count<-5e+06
	threshold<-0.5						# Must be between 0 and 1
	label_ratio<-200					# Must be greater than 1
	#hbase_tbs<-"crmbat"				# HBase tablespace
	#host_name<-"crm025"				# Only support nodes which ThriftServer has been started
	#port_name<-9090					# Default port for thrift service
	hive_tbs<-"crmbat"					# Hive tablespace

	# # Constant judgement(rewrite)
	# if(min_count > max_count
		# | threshold > 1
		# | label_ratio <1 
		# # |...
		# ){
		# msg<-"..."
		# cat("[EXCEPTION]", msg, "\n", file=filename, append=T)
		# sparkR.session.stop()
		# return(c(1, msg))
	# }
		
	# Load Dataset
	msg<-try({mydata<-sql(query_str)}, silent=TRUE)
	# cat ...
	
	if(class(msg)[1]=="SparkDataFrame" && length(colnames(msg)!=0)){
		
		cache(mydata)
		ncolumn<-ncol(mydata)
		
		if(ncolumn < (length(fcol_names)+length(scol_names)+2)){
			msg<-paste("Insufficient column count(", ncolumn, "/", length(fcol_names)+length(scol_names)+2, ")", sep="")
			cat("[EXCEPTION]", msg, "\n", file=filename, append=T)
			sparkR.session.stop()
			return(c(1, msg))
		}
		
		col_names<-colnames(mydata)
		for(i in 1:length(fcol_names)){
			if(col_names[i]!= fcol_names[i]){
				msg<-paste("Illegal front column name(", col_names[i], ")", sep="")
				cat("[EXCEPTION]", msg, "\n", file=filename, append=T)
				sparkR.session.stop()
				return(c(1, msg))
			}
		}
		
		for(i in 1:length(scol_names)){
			if(col_names[i+length(fcol_names)]!= scol_names[i]){
				msg<-paste("Illegal statistic column name(", col_names[i], ")", sep="")
				cat("[EXCEPTION]", msg, "\n", file=filename, append=T)
				sparkR.session.stop()
				return(c(1, msg))
			}
		}
		
		if("character" %in% coltypes(mydata)[c(-1:-length(fcol_names))]){
			msg<-"Illegal variable column type(character type not allowed)"
			cat("[EXCEPTION]", msg, "\n", file=filename, append=T)
			sparkR.session.stop()
			return(c(1, msg))
		}

		ct<-nrow(mydata)
		cat(as.character(Sys.time()), "\t", run_id, "ct:", ct, "\n", file=filename, append=T)

		if(ct < min_count | ct > max_count){
			msg<-paste("Input dataset(", ct, ") is smaller than MIN_COUNT(", min_count, ") or larger than MAX_COUNT(", max_count, ")", sep="")
			cat("[EXCEPTION]", msg, "\n", file=filename, append=T)
			sparkR.session.stop()
			return(c(1, msg))
		}

		if(col_names[ncolumn]!="label"){
			names(mydata)[ncolumn]<-"label"	
		}
		
		n1<-nrow(mydata[mydata$label==1,])
		cat(as.character(Sys.time()), "\t", run_id, "n1:", n1, "\n", file=filename, append=T)
		
		n0<-nrow(mydata[mydata$label==0,])
		cat(as.character(Sys.time()), "\t", run_id, "n0:", n0, "\n", file=filename, append=T)
		
		if(ct-n1-n0!=0){
			msg<-"Existing illegal label values(other than 0 or 1)"
			cat("[EXCEPTION]", msg, "\n", file=filename, append=T)
			sparkR.session.stop()
			return(c(1, msg))
		}

		if(n1==0 | n0==0){
			msg<-"No positive(1) or negative(0) data samples"
			cat("[EXCEPTION]", msg, "\n", file=filename, append=T)
			sparkR.session.stop()
			return(c(1, msg))
		}
		
		larger<-ifelse(n1>n0, n1, n0)
		smaller<-ifelse(n1<n0, n1, n0)
		if(larger/smaller > label_ratio){
			msg<-paste("Over the LABEL_RATIO(", label_ratio, "/", round(larger/smaller, 1), ")", sep="")
			cat("[EXCEPTION]", msg, "\n", file=filename, append=T)
			sparkR.session.stop()
			return(c(1, msg))
		}

		# Generate training dataset
		# <1> 
		train<-if(ct<max_train_count) mydata else sql(paste(query_str, "distribute by rand() sort by rand() limit", format(max_train_count, scientific=FALSE)))		
		# <2>
		# temp_str<-strsplit(query_str, 'from')[[1]]
		# new_query<-paste('select * from('
			# , temp_str[1]
			# , ', rand(1234) as random from'
			# , temp_str[2]
			# , ' oder by cust_no) t where random between 0 and '
			# , format(max_train_count/ct, scientific=FALSE)
			# , sep='')
		# train<-if(ct<max_train_count) mydata else sql(new_query)
		# <3>
		# train<-sql('select cust_no from (select t.cust_no, row_number() over(order by cust_no) rank from crmbat.tb_dm_self_model_dataset t) p where rank%10 = 0 ')
		
		cache(train)

		if(colnames(train)[ncol(train)]!="label"){
			names(train)[ncol(train)]<-"label"	
		}
		train<-train[, c(-1:(-length(fcol_names)-length(scol_names)))]
		cat(as.character(Sys.time()), "\t", run_id, "train-set complete:", ifelse(ct<max_train_count, ct, max_train_count), " of ", ct, "\n", file=filename, append=T) 

		# Model operation
		if(model_name=="DT"){
			model <- spark.randomForest(train, label ~ ., "classification", numTrees = 1)
		} else if(model_name=="LR"){
			model<-spark.logit(train, label ~ ., maxIter=10)		
		} else {
			msg<-paste("Unrecognized MODEL_NAME(", model_name, ")", sep="")
			cat("[EXCEPTION]", msg, "\n", file=filename, append=T)
			sparkR.session.stop()
			return(c(1, msg))
		}
		cat(as.character(Sys.time()), "\t", run_id, "FIT complete \n", file=filename, append=T)

		# Make prediction
		predictions<-predict(model, mydata)
		cat(as.character(Sys.time()), "\t", run_id, "CALCULATING... \n", file=filename, append=T)
		
		# Make confusion matrix
		# actually prediction value is only 1 or 0		
		tp<-nrow(filter(predictions, column("label")==1 & column("prediction")>threshold))
		fn<-nrow(filter(predictions, column("label")==1 & column("prediction")<threshold))
		fp<-nrow(result<-filter(predictions, column("label")==0 & column("prediction")>threshold))
		tn<-nrow(filter(predictions, column("label")==0 & column("prediction")<threshold))
		cat("\t[1]\t[0]\n [1] ", tp, "\t", fp,"\n [0] ", fn, "\t", tn, "\n", file=filename, append=T)
		
		cover_rate<-ifelse(tp+fn!=0, tp/(tp+fn), 0)
		target_rate<-ifelse(tp+fp!=0, tp/(tp+fp), 0)
		#accu_rate<-(tp+tn)/ct
		cat("-------------------------- \n", file=filename, append=T)
		cat(" cover_rate:", cover_rate,"\ntarget_rate:", target_rate, "\n", file=filename, append=T)
		cat("-------------------------- \n", file=filename, append=T)

		if(fp!=0){
			# Make statistic report
			# if(stat_level %in% c("L1","L2")){
				createOrReplaceTempView(result, "result")
				stat_sql<- paste("select belong_inst, count(0) as cust_qty", sep="")
				#gcol<-ifelse(stat_level=="L1", 2, 3)
				for(i in 1:length(scol_names)){
					stat_sql<-paste(stat_sql, ", ", stat_types[i], "(", scol_names[i], ") as ", scol_names[i], sep="")
					if(i == length(scol_names)){
						stat_sql<-paste(stat_sql, " from result group by belong_inst", sep="") 
						#cat("[SQL]******", stat_sql, "******\n", file=filename, append=T)
					}
				}
				statistic<-sql(stat_sql)
				createOrReplaceTempView(statistic, "statistic")
				
				# Save result 
				ftb_name<-paste(hive_tbs, ".tb_", teller_no, sep="")			# tb_ needed because teller_no is numeric
				stb_name<-paste(hive_tbs, ".tb_", teller_no, "_s", sep="")		# tb_ needed because teller_no is numeric	
				acol_names<-c("belong_inst","cust_qty")							# additional columns for report table
				acol_types<-c("string","int")									# additional columns for report table
				
				cat(as.character(Sys.time()), "\t", run_id, "indata:", ftb_name, "\n", file=filename, append=T)
				rc<-indata(ftb_name, fcol_names, fcol_types, "result")
				if(rc[1]==1) return(c(1, rc[2]))	#typeof(rc[1]) is character
	
				cat(as.character(Sys.time()), "\t", run_id, "indata:", stb_name, "\n", file=filename, append=T)
				rc<-indata(stb_name, c(acol_names, scol_names), c(acol_types, scol_types), "statistic")
				if(rc[1]==1) return(c(1, rc[2]))
				
				sparkR.session.stop()
				cat(as.character(Sys.time()), "\t", run_id, "session stopped \n", file=filename, append=T)
				return(as.character(c(0, fp, cover_rate, target_rate)))
				
			# } else {
				# msg<-paste("Unrecognized STAT_LEVEL(", stat_level, ")", sep="")
				# cat("[EXCEPTION]", msg, "\n", file=filename, append=T)
				# sparkR.session.stop()
				# return(c(1, msg))
			# }
		} else{
			cat("[WARNING] Result set is empty \n", file=filename, append=T)
			sparkR.session.stop()
			cat(as.character(Sys.time()), "\t", run_id, "session stopped \n", file=filename, append=T)
			return(as.character(c(0, fp, cover_rate, target_rate)))
		} 
		
		# Save result and statistic to Hive(both in and out are feasible)		
		
	} else if('try-error' %in% class(msg)){
		msg<-strsplit(attr(msg, "condition")[1]$message, "\n")[[1]][1]
		cat("[EXCEPTION]", msg, "\n", file=filename, append=T)
		sparkR.session.stop()
		return(c(1, msg))
	} else{
		msg<-"Unkown error, check your query sql statement(mydata)"
		cat("[EXCEPTION]", msg, "\n", file=filename, append=T)
		sparkR.session.stop()
		return(c(1, msg))
	}

	#end of function
}


indata<-function(htable, hcol, htype, hview) {

	dsql<-paste("drop table if exists ", htable, sep="")
	csql<-paste("create table ", htable, "(", sep="")
	isql<-paste("insert into table ", htable, " select ", sep="")
	
	for(i in 1:length(hcol)){
		csql<-paste(csql, hcol[i], " ", htype[i], sep="")
		isql<-paste(isql, hcol[i], sep="")
		if(i!=length(hcol)){
			csql<-paste(csql, ",", sep="")
			isql<-paste(isql, ",", sep="")
		}else{
			csql<-paste(csql, ")", sep="")
			isql<-paste(isql, " from ", hview, sep="")
		}
	}
	msg<-try({
		cat("[SQL]******", dsql, "\n", file=filename, append=T)
		sql(dsql)
		cat("[SQL]******", csql, "\n", file=filename, append=T)
		sql(csql)
		cat("[SQL]******", isql, "\n", file=filename, append=T)
		sql(isql)
		}, silent=TRUE)
			
	if(class(msg)[1]=="SparkDataFrame"){
		return(c(0))
	} else if('try-error' %in% class(msg)){
		msg<-strsplit(attr(msg, "condition")[1]$message, "\n")[[1]][1]
		cat("[EXCEPTION]", msg, "\n", file=filename, append=T)
		return(c(1, msg))
	} else{
		msg<-"Unkown error, check your query sql statement***(indata)"
		cat("[EXCEPTION]", msg, "\n", file=filename, append=T)
		return(c(1, msg))
	}
	
	#end of function
}

# end of script

