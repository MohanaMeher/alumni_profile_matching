# PROJECT : ALUMNI PROFILE MATCHING

**Motivation**: 
It’s always good to reach out to people with similar interests and backgrounds. Usually, we only reach out to alumni who are introduced during special events or seminars. This could overwhelm the alumni or they might not answer all the current students' questions. For suppose, if there are 2 students - one with a Finance background with several years of experience and the other with a Computer Science background with no/little experience. Alumni with relative or similar backgrounds can make better suggestions. This project aims to pick the top 5 similar alumni profiles for each current student to whom they can reach out by determining the similarity scores for the students and the alumni.


Data sources : 
1) List of alumni profiles by cohort maintained on GCP
2) Scraped linkedin profiles data(in json) per cohort



In phase 1, we setup a DAG airflow pipeline to extract data from the aforementioned data sources, perform transformations on spark and load into
mongo db.
Listed below are the modules in which the ETL setup using DAG has been implemented.


**DAG pipeline**

msds697_task2.py

The DAG is setup in a way that it dynamically generates task workflows for each cohort.
We read the main file(source_file#1) in get_cohorts() function.
Each flow consists of two tasks: one for extract and other for tranform&load



**Extract :**

alumni_list.py
alumni_profiles.py

**Transform & Load**

aggregates_to_mongo.py

**Load**

mongodb.py

**User definition**

user_definition.py


