# -----------------------------------------
# vip
# customer group:	THISM_HIGH_ASSET_FLAG=1
# 	model target:	cur_mon_day_avg_total_asset>=100000 and CUST_ASSET_DECLI_FG=1
# cdt
# customer group:	CUST_PARTITION_FLAG=1 and CRCARD_CUS_FLAG=1
# 	model target:	HANDLE_STAGES_FLAG=1
# -----------------------------------------

[spark@crm020 ~]$ sparkR
options(scipen=200)	# Close scientific format

temp<-sql("select * from crmbat.tb_dm_self_model_dataset_temp")	# 32561 row(s)
temp<-as.data.frame(temp)
mydata<-data.frame(
statis_date         	       = base::sample(c('20190831'), 32561, replace=TRUE)                                               , # string 
cust_no             	       = temp$cust_no							                                                	    , # string 
cust_belong_inst_no 	       = base::sample(c('30000000001'), 32561, replace=TRUE)                                            , # string 
inst_name           	       = base::sample(c('中国****银行某某支行'), 32561, replace=TRUE)                               , # string 
cust_name           	       = base::sample(c('赵某某','钱某某','孙某某','李某某'), 32561, replace=TRUE)                      , # string 
cust_contact_mobile 	       = base::sample(c('13800000000'), 32561, replace=TRUE)                                            , # string 
cert_type           	       = base::sample(c(1), 32561, replace=TRUE)                                                        , # string 
cert_no             	       = base::sample(c('12345678901234567X'), 32561, replace=TRUE)                                     , # string 
cust_sex_kind       	       = base::sample(c(1,2), 32561, replace=TRUE)                                                      , # int    
age                 	       = abs(floor(rnorm(32561, sd=10, mean=40)))                                                 		, # double 
marrg_situ_code     	       = base::sample(c(1,2,3,4), 32561, replace=TRUE, prob=c(0.5,0.4,0.05,0.05))                       , # int(1/2/3/4)        
highest_edu         	       = base::sample(c(1,2,3,4,5,6,7), 32561, replace=TRUE, prob=c(0.1,0.1,0.1,0.2,0.2,0.2,0.1))       , # int(1/2/3/4/5/6/7)          
year_income         	       = abs(floor(rnorm(32561, sd=10, mean=15)))                                              		    , # double 
cur_mon_day_avg_total_asset	   = temp$cur_total_asset_fluctrate*100000                                                 		    , # double 
last_mon_day_avg_total_asset   = temp$curt_dep_mon_day_avg_asset                                                       		    , # double 
m2a_mon_day_avg_total_asset	   = temp$fix_dep_mon_day_avg_asset                                                        		    , # double 
m3a_mon_day_avg_total_asset	   = temp$chrem_mon_day_avg_asset                                                          		    , # double 
curt_dep_mon_day_avg_asset	   = temp$fcrsav_mon_day_avg_bal                                                           		    , # double 
fix_dep_mon_day_avg_asset	   = temp$fund_mon_day_avg_bal             		                                           		    , # double 
chrem_mon_day_avg_asset	  	   = temp$nat_mon_day_avg_bal                        	                                   		    , # double 
fcrsav_mon_day_avg_bal	   	   = temp$chrem_mon_day_avg_bal                                                            		    , # double 
fund_mon_day_avg_bal	   	   = temp$insu_mon_day_avg_bal                               	                           		    , # double 
nat_mon_day_avg_bal 	   	   = temp$metal_mon_day_avg_bal                                  	                       		    , # double 
chrem_mon_day_avg_bal	   	   = temp$loan_mon_day_avg_bal                                                             		    , # double 
insu_mon_day_avg_bal	   	   = temp$pre_qtr_day_avg_total_asset                                                      		    , # double 
metal_mon_day_avg_bal	   	   = temp$fut_m3_chrem_due_amt                                           		           		    , # double 
loan_mon_day_avg_bal	   	   = temp$fut_m3_fix_dep_due_amt                                                           		    , # double 
pre_qtr_day_avg_total_asset	   = runif(32561, 1000, 100000)                                                            		    , # double 
fut_m3_chrem_due_amt	   	   = runif(32561, 1000, 100000)                                                            		    , # double 
fut_m3_fix_dep_due_amt	   	   = runif(32561, 1000, 100000)                                                            		    , # double 
cur_total_asset_fluctrate	   = runif(32561)                                                                          		    , # double(%)  
pre_total_asset_fluctrate	   = runif(32561)                                                                          		    , # double(%) 
m2_total_asset_fluctrate	   = runif(32561)                                                                          		    , # double(%) 
fix_dep_total_asset_pct	       = runif(32561)                                                                          		    , # double(%) 
curt_dep_total_asset_pct	   = runif(32561)                                                                          		    , # double(%) 
fut_m3_chrem_total_asset_rtae  = runif(32561)                                                                          		    , # double(%) 
fut_m3_fix_dep_total_asset_rtae= runif(32561)                                                                          		    , # double(%) 
tran_acct_amt       	       = runif(32561, 1000, 100000)                                                            		    , # double
hold_prodt_num      	       = base::sample(1:10, 32561, replace=TRUE, prob=c(0.4,0.2,0.1,0.1,0.05,0.05,0.04,0.03,0.02,0.01)) , # int 
m6_bec_highend_num  	       = base::sample(c(0,1,2,3,4,5,6), 32561, replace=TRUE, prob=c(0.5,0.2,0.1,0.05,0.05,0.05,0.05))   , # int
lastmon_high_asset_cust_flag   = base::sample(c(1,0), 32561, replace=TRUE, prob=c(0.2,0.8))                                     , # int(1/0)  
thism_high_asset_flag	       = base::sample(1, 32561, replace=TRUE) 															, # int(0/1)    
cust_asset_decli_fg 	       = temp$cur_total_asset_fluctrate	 					                                  		    , # int(1/0)  
crcard_cus_flag     	       = base::sample(1, 32561, replace=TRUE)						                                    , # int(1/0)  
sign_quk_pay_flag   	       = base::sample(c(1,0), 32561, replace=TRUE, prob=c(0.2,0.8))                                     , # int(1/0)  
sign_mobile_bank_flag	       = base::sample(c(1,0), 32561, replace=TRUE, prob=c(0.2,0.8))                                     , # int(1/0)  
sign_wx_bank_flag   	       = base::sample(c(1,0), 32561, replace=TRUE, prob=c(0.2,0.8))                                     , # int(1/0)  
sign_tel_bank_flag  	       = base::sample(c(1,0), 32561, replace=TRUE, prob=c(0.2,0.8))                                     , # int(1/0)  
sign_wbank_flag     	       = base::sample(c(1,0), 32561, replace=TRUE, prob=c(0.2,0.8))                                     , # int(1/0)  
loan_bal            	       = runif(32561, 1000, 100000)                                                            			, # double              	            
sum_ovdue_times     	       = base::sample(0:12, 32561, replace=TRUE, prob=c(0.7,0.1,0.1,base::sample(0.01, 10, replace=TRUE)))	, # int 
mm1_personal_wbank_tx_num	   = floor(runif(32561, 0, 15))                                                             		, # int 
mm1_personal_wbank_tx_amt	   = runif(32561, 1000, 100000)                                                             		, # double              
mm1_mobile_bank_tx_num	       = floor(runif(32561, 0, 15))                                                             		, # int 
mm1_mobile_bank_tx_amt	       = runif(32561, 1000, 100000)                                                             		, # double
mm1_wx_bank_tx_num  	       = floor(runif(32561, 0, 15))                                                             		, # int
mm1_wx_bank_tx_amt  	       = runif(32561, 1000, 100000)                                                             		, # double
mm1_selfdev_tx_num  	       = floor(runif(32561, 0, 15))                                                             		, # int
mm1_selfdev_tx_amt  	       = runif(32561, 1000, 100000)                                                             		, # double
mm1_quk_pay_tx_amt  	       = runif(32561, 1000, 100000)                                                             		, # double
mm1_quk_pay_tx_num  	       = floor(runif(32561, 0, 15))                                                             		, # int
personal_cust_vip_lvl	       = base::sample(c(1,2,3,4,0), 32561, replace=TRUE, prob=c(0.1,0.05,0.05,0.2,0.6))         		, # int(1/2/3/4)
open_acc_time       	       = floor(runif(32561, 1, 240))                                                            		, # int(by month)
asset_manage_bal    	       = runif(32561, 1000, 100000)                                                             		, # double
dep_bal             	       = runif(32561, 1000, 100000)                                                             		, # double
cust_partition_flag 	       = base::sample(1, 32561, replace=TRUE)						                             		, # int(1/0)
handle_stages_flag  	       = temp$cur_total_asset_fluctrate								                             		, # int(1/0)
crt_extd_limit      	       = runif(32561, 1000, 100000)                                                             		, # double
cur_aval_limit      	       = runif(32561, 1000, 100000)                                                             		, # double
m3_avg_aval_limit   	       = runif(32561, 1000, 100000)                                                             		, # double
cur_consm_num       	       = floor(runif(32561, 0, 15))                                                             		, # int
crcard_limit_use_rate	       = runif(32561)                                                                           		, # double(%)
cur_draw_amt        	       = runif(32561, 1000, 100000)                                                             		, # double
cur_draw_num        	       = floor(runif(32561, 0, 15))                                                             		, # int
cur_dw_bal_m6_dw_bal_ratio	   = runif(32561)                                                                           		, # double(%) 
cur_cs_bal_pre_cs_bal_ratio	   = runif(32561)                                                                           		, # double(%) 
cur_lmt_usr_m9_lmt_usr_ratio   = runif(32561)                                                                           		, # double(%)
m9_dealw_instal_num 	       = base::sample(0:12, 32561, replace=TRUE, prob=c(0.7,0.1,0.1,base::sample(0.01, 10, replace=TRUE)))	, # int
m12_dealw_instal_num	       = base::sample(0:12, 32561, replace=TRUE, prob=c(0.7,0.1,0.1,base::sample(0.01, 10, replace=TRUE)))	, # int
m3_consm_num        	       = abs(floor(rnorm(32561, sd=10, mean=10)))                                               		, # int
m6_consm_num        	       = temp$curt_dep_mon_day_avg_asset																, # int
m9_consm_num        	       = temp$fix_dep_mon_day_avg_asset 																, # int
m9_consm_atm        	       = temp$chrem_mon_day_avg_asset   																, # double
m6_avg_repay_rate   	       = temp$fcrsav_mon_day_avg_bal    																, # double(%)
m12_consm_atm_desc_monnum	   = temp$fund_mon_day_avg_bal      																, # int(by month)
m12_consm_num_desc_monnum	   = temp$nat_mon_day_avg_bal       																, # int(by month)
m12_max_limit_use_rate	       = temp$chrem_mon_day_avg_bal     																, # double(%)
m12_min_limit_use_rate	       = temp$insu_mon_day_avg_bal      																, # double(%)
m9_draw_amt_desc_monnum	       = temp$metal_mon_day_avg_bal     																, # int(by month)
m9_draw_amt         	       = temp$loan_mon_day_avg_bal      																, # double
m9_revolv_num       	       = temp$pre_qtr_day_avg_total_asset 																, # int
latest_once_consm_month_num	   = temp$fut_m3_chrem_due_amt        																, # int(by month)	
latest_once_draw_month_num	   = temp$fut_m3_fix_dep_due_amt      																, # int(by month)
cust_loss_warn_flag 	       = base::sample(c(1,0), 32561, replace=TRUE, prob=c(0.5,0.2))                                     , # int(0/1)    
deal_date           		   = base::sample(c('20190831'), 32561, replace=TRUE)                                               ) # string
# brh_lvl1_inst_seq_no	       = unlist(lapply(base::sample(10000000001:10000000009, 32561, replace=TRUE), as.character))       ) # string(partition column)
#
#
hive> show create table crmbat.tb_dm_self_model_dataset;	# Not skip first row
# PARTITIONED BY ( 
#   `brh_lvl1_inst_seq_no` string COMMENT 'һ������')
# ROW FORMAT DELIMITED 
#   FIELDS TERMINATED BY '\u0001' 
#
> write.table(mydata, "mydata_tst_32561.txt", sep="\u0001", quote=F, row.names=F, col.names=F)
# Parameters like sep, row.names and col.names must be considered to keep off load-errors.

[spark@crm020 ~]$ scp mydata_tst_32561.txt root@20.200.27.90:/home/spark

# For not partitioned
hive> LOAD DATA LOCAL INPATH 'mydata_tst_32561.txt' [OVERWRITE] INTO TABLE crmbat.tb_dm_self_model_dataset; 
hive> insert into crmbat.tb_dm_self_model_dataset select * from crmbat.tb_dm_self_model_dataset;	# repeat
hive> create table crmbat.tb_dm_self_model_dataset_64M as select regexp_replace(reflect("java.util.UUID", "randomUUID"), "-", "") as cust_no, ...
hive> create table crmbat.tb_dm_self_model_dataset_3M as select * from crmbat.tb_dm_self_model_dataset limit 0;

# For partitioned
hive> set hive.cli.print.header=true;						# display column names
hive> set hive.resultset.use.unique.column.names = false;	# but not display table names
hive> select brh_code, brh_name from crmbat.tb_ods_inst_info where brh_addr='"02"' and brh_code not like '"%A"';
# "11000013"	中国****银行股份有限公司北京分行			# for tst env 
# "44008995"	中国****银行股份有限公司深圳分行			# for tst env 
# "37000025"	中国****银行股份有限公司山东省分行			# for tst env 
# "15010852"	中国****银行股份有限公司内蒙古自治区分行
# "33000072"	中国****银行股份有限公司宁波分行
# "34000010"	中国****银行股份有限公司安徽省分行
# "35000011"	中国****银行股份有限公司福建省分行
# "31000017"	中国****银行股份有限公司上海分行

hive> truncate table crmbat.tb_dm_self_model_dataset;

hive> LOAD DATA LOCAL INPATH '/home/spark/mydata_tst_32561.txt' into TABLE crmbat.tb_dm_self_model_dataset partition( brh_lvl1_inst_seq_no='11000013');
# repeat for other brh_code

hive> select brh_lvl1_inst_seq_no, count(0) from crmbat.tb_dm_self_model_dataset group by brh_lvl1_inst_seq_no;
# brh_lvl1_inst_seq_no	_c1
# 11000013	32561
# 15010852	32561
# 31000017	32561
# 33000072	32561
# 34000010	32561
# 35000011	32561
# 37000025	32561
# 44008995	32561
# Time taken: 5.88 seconds, Fetched: 8 row(s)

# For more data 
hive> create table crmbat.tb_dm_self_model_dataset_tmp as select * from crmbat.tb_dm_self_model_dataset;	# crmbat.tb_dm_self_model_dataset_tmp is not partitioned
hive> insert into table crmbat.tb_dm_self_model_dataset_tmp select * from crmbat.tb_dm_self_model_dataset_tmp;
# repeat to make tmp table more large 

hive> truncate table crmbat.tb_dm_self_model_dataset;
hive> set hive.support.quoted.identifiers=None;	# for column exclude, executed every time
hive> from crmbat.tb_dm_self_model_dataset_tmp t 
insert into crmbat.tb_dm_self_model_dataset 
partition(brh_lvl1_inst_seq_no='11000013')
select t.statis_date, regexp_replace(reflect("java.util.UUID", "randomUUID"), "-", "") as cust_no, `(statis_date|cust_no|brh_lvl1_inst_seq_no)?+.+` where t.brh_lvl1_inst_seq_no='11000013'
insert into crmbat.tb_dm_self_model_dataset 
partition(brh_lvl1_inst_seq_no='44008995')
select t.statis_date, regexp_replace(reflect("java.util.UUID", "randomUUID"), "-", "") as cust_no, `(statis_date|cust_no|brh_lvl1_inst_seq_no)?+.+` where t.brh_lvl1_inst_seq_no='44008995'
insert into crmbat.tb_dm_self_model_dataset 
partition(brh_lvl1_inst_seq_no='37000025')
select t.statis_date, regexp_replace(reflect("java.util.UUID", "randomUUID"), "-", "") as cust_no, `(statis_date|cust_no|brh_lvl1_inst_seq_no)?+.+` where t.brh_lvl1_inst_seq_no='37000025'
insert into crmbat.tb_dm_self_model_dataset 
partition(brh_lvl1_inst_seq_no='15010852')
select t.statis_date, regexp_replace(reflect("java.util.UUID", "randomUUID"), "-", "") as cust_no, `(statis_date|cust_no|brh_lvl1_inst_seq_no)?+.+` where t.brh_lvl1_inst_seq_no='15010852'
insert into crmbat.tb_dm_self_model_dataset 
partition(brh_lvl1_inst_seq_no='33000072')
select t.statis_date, regexp_replace(reflect("java.util.UUID", "randomUUID"), "-", "") as cust_no, `(statis_date|cust_no|brh_lvl1_inst_seq_no)?+.+` where t.brh_lvl1_inst_seq_no='33000072'
insert into crmbat.tb_dm_self_model_dataset 
partition(brh_lvl1_inst_seq_no='34000010')
select t.statis_date, regexp_replace(reflect("java.util.UUID", "randomUUID"), "-", "") as cust_no, `(statis_date|cust_no|brh_lvl1_inst_seq_no)?+.+` where t.brh_lvl1_inst_seq_no='34000010'
insert into crmbat.tb_dm_self_model_dataset 
partition(brh_lvl1_inst_seq_no='35000011')
select t.statis_date, regexp_replace(reflect("java.util.UUID", "randomUUID"), "-", "") as cust_no, `(statis_date|cust_no|brh_lvl1_inst_seq_no)?+.+` where t.brh_lvl1_inst_seq_no='35000011'
insert into crmbat.tb_dm_self_model_dataset 
partition(brh_lvl1_inst_seq_no='31000017')
select t.statis_date, regexp_replace(reflect("java.util.UUID", "randomUUID"), "-", "") as cust_no, `(statis_date|cust_no|brh_lvl1_inst_seq_no)?+.+` where t.brh_lvl1_inst_seq_no='31000017'
;
# repeat from insert 

hive> select count(distinct(cust_no)), count(0) from crmbat.tb_dm_self_model_dataset;
# check unique cust_no




