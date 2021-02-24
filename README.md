# Hawk-batch-prediction

This project is meant to predict the personal infomation (demography, finance etc) in a batch manner. We need to provide S3 path of input .csv file 
where there should be 2 columns (namely text and pii) and each row should contain one input item and it'd store the output in a given S3 location. 

### How to set up a spark cluster to process this. 

1. Download the code in local.

2. In the home directory run 
 
 $ make build

3. It should create/update jobs.zip file and main.py inside HOME/dist directory.

4. store both file in some S3 location. Also add src/bootstrap.sh to  S3.

5. In Aws EMR, create a new cluster.
    
    a. Go to advanced option.
    
    b. Select latest emr release and check spark 3.0.1 
    
    c. Add a step with below settings.
        
           i. Step type: Spark application Deploy mode: client 
           
           ii. Spark-submit options : --packages com.hortonworks:shc-core:1.1.1-2.1-s_2.11 --repositories http://repo.hortonworks.com/content/groups/public/ --py-files <S3 path for jobs.zip>,
           
           iii. Application location: <S3 path for main.py>
             
           iv. Arguments: --job <job_name>    For example, --job demography
           
           v. Add a Bootstrap action. Provide script location as the S3 path to the bootstrap.sh
    
    d. This is bare minimum setting. Feel free to provide other settings like security, log, hardware
    as per requirement.
    
 6. Once the  cluster finishes processing, it should provide output in S3.
 
 
 #### How to add a job
 
 1. create a folder  src/<job_name>
 
 2. Inside that create a method 'analyze' which accepts sparkcontext as an argument. This method will be triggered
    when the spark job would be triggered with <job_name> as a parameter.