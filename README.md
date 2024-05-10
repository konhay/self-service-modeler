# Self-service modeling
## Description

At present, most of the data modeling tools require users to have a high level of programming ability in data processing and model algorithm selection, and the technical threshold is high, and the modeling process cannot be fully automated, which brings no small challenge to the front-line business personnel. At the same time, due to the increasing amount of data to be processed, the traditional modeling process based on R language consumes a lot of time, and can not realize the real-time synchronization of modeling results and client requests. The tool is to solve the problem of self-service and real-time data analysis modeling.

## Processing flow
Step 1. Define modeling goals

	Built-in scenario: VIP customer loss warning
	Custom modeling: Define the modeling target by query criteria

Step 2. Select Customer group

	Filter by indicator: For example, credit card customer segmentation identifier =2
	Filter by label: If it is our credit card customer (0 or 1)

Step 3. Generate statistical description report

	Including max and min, mean and median, standard deviation, missing value, correlation, histogram, etc.
	
step 4. Selected model algorithm

	Logistic regression (spark.logit)
	Decision tree (spark.randomForest)

Step 5. Model execution result

	Output hit rate, coverage and result set
	Data statistics (Summary by institutions, e.g. clients, total assets, average holdings)
## Technical architecture
![微信截图_20240510104534](https://github.com/konhay/crm-sh-mod/assets/26830433/0d52eca8-0890-49d3-aec0-f5b50495f79e)

The tool relies on and requires the use of the [R](https://www.r-project.org/) language environment, the [SparkR](https://spark.apache.org/docs/3.2.0/sparkr.html) distributed computing environment, and the [Rserve](https://www.rforge.net/Rserve/) component service.

First, the powerful function of R language in the field of statistical analysis and predictive modeling is used to realize data storage and processing, array and matrix operations, and statistical description and mapping.

Second, using the lightweight front end provided by the SparkR distributed computing environment, Apache Spark can be called on R. With the help of various operations such as selection, filtering and aggregation based on distributed data frames provided by SparkR, the processing of massive data sets can be realized. With the [MLlib](https://spark.apache.org/mllib/) distributed machine learning algorithm library integrated in Spark, the tool makes it easy to build back-end algorithm engines.

Third, use Rserve component service technology to realize the remote call of interactive side to R language server. With the feature that Rserve uses C/S (client/server) mode to call, the interactive side does not need to connect to the R language library, and the purpose of low coupling between the interactive side Java program and the background R program can be realized.

At present, this tool has been well applied in large commercial banks.
