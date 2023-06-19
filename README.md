# Twitter Data Mining with Hadoop MapReduce: USA Election Analysis <img src="./images/usa-flag.png" width=50/>

<p align='center'> <a href="https://hadoop.apache.org/" target="_blank" rel="noreferrer"> <img src="https://www.vectorlogo.zone/logos/apache_hadoop/apache_hadoop-icon.svg" alt="hadoop" width="50" height="50"/> </a>ㅤ<a href="https://www.java.com" target="_blank" rel="noreferrer"> <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/java/java-original.svg" alt="java" width="50" height="50"/> </a></p>
This project focuses on mining <a href="https://www.kaggle.com/datasets/manchunhui/us-election-2020-tweets?select=hashtag_joebiden.csv">Twitter data</a> related to the USA election between Trump and Biden. The data is stored in the Hadoop Distributed File System (HDFS), and the mining process is implemented using the MapReduce framework in Java. The project aims to provide insights into the election by performing three query tasks.

## Query Tasks:
<p align="center"> <img src="./images/donald-biden.png" width="500"/></p>

1. **Number of Likes and Retweets for Each Candidate:** This task involves analyzing the engagement on tweets for each candidate, specifically tracking the number of likes and retweets they received.

2. **Number and Percentage of Tweets Grouped by Country and Candidate:** Here, tweets are categorized based on the country of origin and the candidate mentioned. The task calculates the total number of tweets and provides the percentage breakdown for each country-candidate combination.

3. **Geographical Analysis of Tweets by Country and Candidate:** Similar to the previous task, this query involves grouping tweets by country and candidate. However, instead of relying on country names, this task utilizes longitude and latitude coordinates to determine the country of origin for each tweet. The results provide insights into the geographic distribution of tweets for each candidate.

### Key Features:

- Utilizes Hadoop MapReduce to process and analyze Twitter data stored in HDFS.
- Performs three query tasks, including likes and retweets analysis, country-based tweet grouping, and geographical analysis.
- Implements efficient algorithms to handle large volumes of Twitter data.

## Prerequisites:

- Hadoop cluster with HDFS set up and running.
- Java Development Kit (JDK) installed.
- Twitter data obtained and stored in HDFS.

## Installation and Usage:

1. Clone this repository to your local machine.
2. Set up and configure the Hadoop cluster with HDFS.
3. Ensure the Java Development Kit (JDK) is installed.
4. Retrieve the Twitter data and store it in the Hadoop Distributed File System (HDFS).
5. Compile and build the project using the provided scripts or build tools.
6. Execute the MapReduce jobs for each query task, specifying the input and output paths.
7. Monitor the job execution and retrieve the results from the output directory.
8. Analyze and interpret the results to gain insights into the USA election.

For detailed instructions and additional documentation, please refer to this [medium post](https://pnunofrancog.medium.com/how-to-set-up-hadoop-3-2-1-multi-node-cluster-on-ubuntu-20-04-inclusive-terminology-2dc17b1bff19).

## License:

This project is licensed under the [MIT License](LICENSE). You are free to use, modify, and distribute the code according to the terms and conditions of the license.
