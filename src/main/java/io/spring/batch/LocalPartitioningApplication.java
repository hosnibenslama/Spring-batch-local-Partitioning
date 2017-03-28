package io.spring.batch;

import io.spring.batch.configuration.JobConfiguration;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import java.util.Scanner;
import java.util.logging.Logger;

@EnableTransactionManagement
public class LocalPartitioningApplication {

    final static Logger logger = Logger.getLogger("LocalPartitioningApplication");

	public static void main(String[] args) throws Exception {

        System.out.println("\nMenu Options\n");
        System.out.println("(1) - Export");
        System.out.println("(2) - Import");

        System.out.print("Please enter your selection:\t");

        Scanner scanner = new Scanner(System.in);

        int selection = scanner.nextInt();


        ApplicationContext context= new AnnotationConfigApplicationContext(JobConfiguration.class);


		JobLauncher jobLauncher = context.getBean(JobLauncher.class);

        if (selection== 1) {

            try {

                JobParameters jobParameters = new JobParametersBuilder().addLong("time",System.currentTimeMillis()).toJobParameters();


                Job jobExport = (Job) context.getBean("jobExport");

                long startTime = System.currentTimeMillis();
                // launch the job
                jobLauncher.run(jobExport, jobParameters);
                long endTime = System.currentTimeMillis();
                double seconds = (endTime - startTime) / 1000.0;
                logger.info("Export duration : " + seconds + " seconds");
            } catch (JobExecutionAlreadyRunningException | JobRestartException
                    | JobInstanceAlreadyCompleteException
                    | JobParametersInvalidException e) {
                e.printStackTrace();
            }

        }


        if  (selection== 2){
            try {
                JobParameters jobParameters = new JobParametersBuilder().addLong("time",System.currentTimeMillis()).toJobParameters();


                Job jobImport = (Job) context.getBean("jobImport");

                long startTime = System.currentTimeMillis();

                jobLauncher.run(jobImport,jobParameters);
                long endTime = System.currentTimeMillis();

                double seconds = (endTime - startTime) / 1000.0;

                logger.info("Import duration : " + seconds + " seconds");

            } catch (JobExecutionAlreadyRunningException | JobRestartException
                    | JobInstanceAlreadyCompleteException
                    | JobParametersInvalidException e) {
                e.printStackTrace();
            }

        }


	}


}

