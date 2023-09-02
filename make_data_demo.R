mydata<-data.frame(
statis_date         	       = sample(c('20190831'), 1e+06, replace=TRUE)                                               , # string 
cust_no             	       = sample(c('cust_no'), 1e+06, replace=TRUE)                                                , # string 
brh_lvl1_inst_seq_no	       = unlist(lapply(sample(10000000001:10000000099, 1e+06, replace=TRUE), as.character))       , # string 
cust_belong_inst_no 	       = sample(c('30000000001'), 1e+06, replace=TRUE)                                            , # string 
inst_name           	       = sample(c('中国****银行某某支行'), 1e+06, replace=TRUE)                               , # string 
cust_name           	       = sample(c('赵某某','钱某某','孙某某','李某某'), 1e+06, replace=TRUE)                      , # string 
cust_contact_mobile 	       = sample(c('13800000000'), 1e+06, replace=TRUE)                                            , # string 
cert_type           	       = sample(c(1), 1e+06, replace=TRUE)                                                        , # string 
cert_no             	       = sample(c('12345678901234567X'), 1e+06, replace=TRUE)                                     , # string 
cust_sex_kind       	       = sample(c(1,2), 1e+06, replace=TRUE)                                                      , # int    
age                 	       = abs(floor(rnorm(1e+06, sd=10, mean=40)))                                                 , # double 
marrg_situ_code     	       = sample(c(1,2,3,4), 1e+06, replace=TRUE, prob=c(0.5,0.4,0.05,0.05))                       , # int(1/2/3/4)        
highest_edu         	       = sample(c(1,2,3,4,5,6,7), 1e+06, replace=TRUE, prob=c(0.1,0.1,0.1,0.2,0.2,0.2,0.1))       , # int(1/2/3/4/5/6/7)          
year_income         	       = abs(floor(rnorm(1e+06, sd=10, mean=15)))                                                 , # double 
cur_mon_day_avg_total_asset	   = runif(1e+06, 1000, 100000)                                                               , # double 
last_mon_day_avg_total_asset   = runif(1e+06, 1000, 100000)                                                               , # double 
m2a_mon_day_avg_total_asset	   = runif(1e+06, 1000, 100000)                                                               , # double 
m3a_mon_day_avg_total_asset	   = runif(1e+06, 1000, 100000)                                                               , # double 
curt_dep_mon_day_avg_asset	   = runif(1e+06, 1000, 100000)                                                               , # double 
fix_dep_mon_day_avg_asset	   = runif(1e+06, 1000, 100000)                                                               , # double 
chrem_mon_day_avg_asset	  	   = runif(1e+06, 1000, 100000)                                                               , # double 
fcrsav_mon_day_avg_bal	   	   = runif(1e+06, 1000, 100000)                                                               , # double 
fund_mon_day_avg_bal	   	   = runif(1e+06, 1000, 100000)                                                               , # double 
nat_mon_day_avg_bal 	   	   = runif(1e+06, 1000, 100000)                                                               , # double 
chrem_mon_day_avg_bal	   	   = runif(1e+06, 1000, 100000)                                                               , # double 
insu_mon_day_avg_bal	   	   = runif(1e+06, 1000, 100000)                                                               , # double 
metal_mon_day_avg_bal	   	   = runif(1e+06, 1000, 100000)                                                               , # double 
loan_mon_day_avg_bal	   	   = runif(1e+06, 1000, 100000)                                                               , # double 
pre_qtr_day_avg_total_asset	   = runif(1e+06, 1000, 100000)                                                               , # double 
fut_m3_chrem_due_amt	   	   = runif(1e+06, 1000, 100000)                                                               , # double 
fut_m3_fix_dep_due_amt	   	   = runif(1e+06, 1000, 100000)                                                               , # double 
cur_total_asset_fluctrate	   = runif(1e+06)                                                                             , # double(%)  
pre_total_asset_fluctrate	   = runif(1e+06)                                                                             , # double(%) 
m2_total_asset_fluctrate	   = runif(1e+06)                                                                             , # double(%) 
fix_dep_total_asset_pct	       = runif(1e+06)                                                                             , # double(%) 
curt_dep_total_asset_pct	   = runif(1e+06)                                                                             , # double(%) 
fut_m3_chrem_total_asset_rtae  = runif(1e+06)                                                                             , # double(%) 
fut_m3_fix_dep_total_asset_rtae= runif(1e+06)                                                                             , # double(%) 
tran_acct_amt       	       = runif(1e+06, 1000, 100000)                                                               , # double
hold_prodt_num      	       = sample(1:10, 1e+06, replace=TRUE, prob=c(0.4,0.2,0.1,0.1,0.05,0.05,0.04,0.03,0.02,0.01)) , # int 
m6_bec_highend_num  	       = sample(c(0,1,2,3,4,5,6), 1e+06, replace=TRUE, prob=c(0.5,0.2,0.1,0.05,0.05,0.05,0.05))   , # int
lastmon_high_asset_cust_flag   = sample(c(1,0), 1e+06, replace=TRUE, prob=c(0.2,0.8))                                     , # int(1/0)  
cust_asset_decli_fg 	       = sample(c(1,0), 1e+06, replace=TRUE, prob=c(0.2,0.8))                                     , # int(1/0)  
crcard_cus_flag     	       = sample(c(1,0), 1e+06, replace=TRUE, prob=c(0.2,0.8))                                     , # int(1/0)  
sign_quk_pay_flag   	       = sample(c(1,0), 1e+06, replace=TRUE, prob=c(0.2,0.8))                                     , # int(1/0)  
sign_mobile_bank_flag	       = sample(c(1,0), 1e+06, replace=TRUE, prob=c(0.2,0.8))                                     , # int(1/0)  
sign_wx_bank_flag   	       = sample(c(1,0), 1e+06, replace=TRUE, prob=c(0.2,0.8))                                     , # int(1/0)  
sign_tel_bank_flag  	       = sample(c(1,0), 1e+06, replace=TRUE, prob=c(0.2,0.8))                                     , # int(1/0)  
sign_wbank_flag     	       = sample(c(1,0), 1e+06, replace=TRUE, prob=c(0.2,0.8))                                     , # int(1/0)  
loan_bal            	       = runif(1e+06, 1000, 100000)                                                               , # double              	            
sum_ovdue_times     	       = sample(0:12, 1e+06, replace=TRUE, prob=c(0.7,0.1,0.1,sample(0.01, 10, replace=TRUE)))    , # int 
mm1_personal_wbank_tx_num	   = floor(runif(1e+06, 0, 15))                                                               , # int 
mm1_personal_wbank_tx_amt	   = runif(1e+06, 1000, 100000)                                                               , # double              
mm1_mobile_bank_tx_num	       = floor(runif(1e+06, 0, 15))                                                               , # int 
mm1_mobile_bank_tx_amt	       = runif(1e+06, 1000, 100000)                                                               , # double
mm1_wx_bank_tx_num  	       = floor(runif(1e+06, 0, 15))                                                               , # int
mm1_wx_bank_tx_amt  	       = runif(1e+06, 1000, 100000)                                                               , # double
mm1_selfdev_tx_num  	       = floor(runif(1e+06, 0, 15))                                                               , # int
mm1_selfdev_tx_amt  	       = runif(1e+06, 1000, 100000)                                                               , # double
mm1_quk_pay_tx_amt  	       = runif(1e+06, 1000, 100000)                                                               , # double
mm1_quk_pay_tx_num  	       = floor(runif(1e+06, 0, 15))                                                               , # int
personal_cust_vip_lvl	       = sample(c(1,2,3,4,0), 1e+06, replace=TRUE, prob=c(0.1,0.05,0.05,0.2,0.6))                 , # int(1/2/3/4)
open_acc_time       	       = floor(runif(1e+06, 1, 240))                                                              , # int(by month)
asset_manage_bal    	       = runif(1e+06, 1000, 100000)                                                               , # double
dep_bal             	       = runif(1e+06, 1000, 100000)                                                               , # double
cust_partition_flag 	       = sample(c(1,0), 1e+06, replace=TRUE, prob=c(0.5,0.2))                                     , # int(1/0)
handle_stages_flag  	       = sample(c(1,0), 1e+06, replace=TRUE, prob=c(0.5,0.2))                                     , # int(1/0)
crt_extd_limit      	       = runif(1e+06, 1000, 100000)                                                               , # double
cur_aval_limit      	       = runif(1e+06, 1000, 100000)                                                               , # double
m3_avg_aval_limit   	       = runif(1e+06, 1000, 100000)                                                               , # double
cur_consm_num       	       = floor(runif(1e+06, 0, 15))                                                               , # int
crcard_limit_use_rate	       = runif(1e+06)                                                                             , # double(%)
cur_draw_amt        	       = runif(1e+06, 1000, 100000)                                                               , # double
cur_draw_num        	       = floor(runif(1e+06, 0, 15))                                                               , # int
cur_dw_bal_m6_dw_bal_ratio	   = runif(1e+06)                                                                             , # double(%) 
cur_cs_bal_pre_cs_bal_ratio	   = runif(1e+06)                                                                             , # double(%) 
cur_lmt_usr_m9_lmt_usr_ratio   = runif(1e+06)                                                                             , # double(%)
m9_dealw_instal_num 	       = sample(0:12, 1e+06, replace=TRUE, prob=c(0.7,0.1,0.1,sample(0.01, 10, replace=TRUE)))    , # int
m12_dealw_instal_num	       = sample(0:12, 1e+06, replace=TRUE, prob=c(0.7,0.1,0.1,sample(0.01, 10, replace=TRUE)))    , # int
m3_consm_num        	       = abs(floor(rnorm(1e+06, sd=10, mean=10)))                                                 , # int
m6_consm_num        	       = abs(floor(rnorm(1e+06, sd=10, mean=20)))                                                 , # int
m9_consm_num        	       = abs(floor(rnorm(1e+06, sd=10, mean=30)))                                                 , # int
m9_consm_atm        	       = runif(1e+06, 1000, 100000)                                                               , # double
m6_avg_repay_rate   	       = runif(1e+06)                                                                             , # double(%)
m12_consm_atm_desc_monnum	   = floor(runif(1e+06, 0, 12))                                                               , # int(by month)
m12_consm_num_desc_monnum	   = floor(runif(1e+06, 0, 12))                                                               , # int(by month)
m12_max_limit_use_rate	       = runif(1e+06)                                                                             , # double(%)
m12_min_limit_use_rate	       = runif(1e+06)                                                                             , # double(%)
m9_draw_amt_desc_monnum	       = sample(0:9, 1e+06, replace=TRUE, prob=c(0.91,sample(0.01, 9, replace=TRUE)))             , # int(by month)
m9_draw_amt         	       = runif(1e+06, 1000, 100000)                                                               , # double
m9_revolv_num       	       = sample(0:9, 1e+06, replace=TRUE, prob=c(0.91,sample(0.01, 9, replace=TRUE)))             , # int
latest_once_consm_month_num	   = sample(1:10, 1e+06, replace=TRUE, prob=c(0.4,0.2,0.1,0.1,0.05,0.05,0.04,0.03,0.02,0.01)) , # int(by month)	
latest_once_draw_month_num	   = sample(0:9, 1e+06, replace=TRUE, prob=c(0.91,sample(0.01, 9, replace=TRUE)))             , # int(by month)
cust_loss_warn_flag 	       = sample(c(1,0), 1e+06, replace=TRUE, prob=c(0.5,0.2))                                     , # int(0/1)    
deal_date           		   = sample(c('20190831'), 1e+06, replace=TRUE)                                               , # string
thism_high_asset_flag	       = sample(c(1,0), 1e+06, replace=TRUE, prob=c(0.5,0.2))                                     ) # int(0/1)    
#
#
hive> show create table crmbat.tb_dm_self_model_dataset;	# not partitioned, not skip first row
# ROW FORMAT DELIMITED 
#   FIELDS TERMINATED BY '\u0001' 
#
> write.table(mydata, "mydata_u0001.txt", sep="\u0001", quote=F, row.names=F, col.names=F)
# about 2 minutes required
# 1017706427 Sep  7 16:25 mydata.txt
# Parameters like sep, row.names and col.names must be considered to keep off load-errors.
hive> LOAD DATA LOCAL INPATH 'mydata_u0001.txt' [OVERWRITE] INTO TABLE crmbat.tb_dm_self_model_dataset; 
hive> insert into crmbat.tb_dm_self_model_dataset select * from crmbat.tb_dm_self_model_dataset;	# repeat
hive> 
hive> create table crmbat.tb_dm_self_model_dataset_64M as select regexp_replace(reflect("java.util.UUID", "randomUUID"), "-", "") as cust_no, ...
hive> create table crmbat.tb_dm_self_model_dataset_3M as select * from crmbat.tb_dm_self_model_dataset limit 0;

