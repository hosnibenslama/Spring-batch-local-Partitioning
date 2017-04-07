/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.spring.batch.configuration;

import io.spring.batch.domain.ColumnRangePartitioner;
import io.spring.batch.domain.Customer;
import io.spring.batch.domain.CustomerRowMapper;
import io.spring.batch.partitioner.CustomMultiResourcePartitioner;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.batch.item.xml.StaxEventItemReader;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.oxm.Marshaller;
import org.springframework.oxm.xstream.XStreamMarshaller;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Hosni
 */
@Configuration
@EnableBatchProcessing
@ComponentScan("io.spring.batch")
@EnableTransactionManagement
@ImportResource("classpath:/config/config-beans.xml")
public class JobConfiguration {

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;


	@Autowired
	private ResourcePatternResolver resourcePatternResolver;


	@Autowired
	private DataSource dataSource;

	@Bean
	public Partitioner importPartitioner() {
		CustomMultiResourcePartitioner partitioner = new CustomMultiResourcePartitioner();
		Resource[] resources;
		try {
			resources = resourcePatternResolver.getResources("file:src/main/resources/data/*.xml");
		} catch (IOException e) {
			throw new RuntimeException("I/O problems when resolving the input file pattern.", e);
		}
		partitioner.setResources(resources);
		return partitioner;
	}

	@Bean
	@StepScope
	public StaxEventItemReader<Customer> CustomerItemReader(
			@Value("#{stepExecutionContext['resourceName']}") String file
	) {
		XStreamMarshaller unmarshaller = new XStreamMarshaller();

		Map<String, Class> aliases = new HashMap<>();
		aliases.put("customer", Customer.class);

		unmarshaller.setAliases(aliases);


		StaxEventItemReader<Customer> reader = new StaxEventItemReader<>();


		reader.setResource(new ClassPathResource("data/" + file));

		reader.setFragmentRootElementName("customer");
		reader.setUnmarshaller(unmarshaller);

		return reader;
	}


	@Bean
	public JdbcBatchItemWriter<Customer> customerItemWriterImport() {

		JdbcBatchItemWriter<Customer> itemWriter = new JdbcBatchItemWriter<>();


		itemWriter.setDataSource(this.dataSource);
		itemWriter.setSql("INSERT INTO CUSTOMER VALUES (:i, :f, :l, :b)");

		itemWriter.afterPropertiesSet();

		itemWriter.setItemSqlParameterSourceProvider(item -> {
			Map<String, Object> params = new HashMap<>();
			params.put("i", item.getId());
			params.put("f", item.getFirstName());
			params.put("l", item.getLastName());
			params.put("b", item.getBirthdate());
			return new MapSqlParameterSource(params);
		});

		return itemWriter;

	}

	@Bean
	public Step ImportStep() throws Exception {
		return stepBuilderFactory.get("step1")
				.partitioner(slaveStep().getName(), importPartitioner())
				.step(slaveStep())
				.taskExecutor(batchTaskExecutor())
				.build();
	}


	@Bean
	public TaskExecutor batchTaskExecutor() {
		ThreadPoolTaskExecutor batchTaskExecutor = new ThreadPoolTaskExecutor();

		batchTaskExecutor.setCorePoolSize(10);
		batchTaskExecutor.setMaxPoolSize(10);
		batchTaskExecutor.afterPropertiesSet();

		return batchTaskExecutor;
	}

	@Bean
	public Step slaveStep() {
		return stepBuilderFactory.get("slaveStep")
				.<Customer, Customer>chunk(100000)
				.reader(CustomerItemReader(null))
				.writer(customerItemWriterImport())
				.build();

	}

	@Bean
	public Job jobImport() throws Exception {

		return jobBuilderFactory.get("jobImport")
				.incrementer(new RunIdIncrementer())
				.start(ImportStep())
				.build();
	}


	@Bean
	public ColumnRangePartitioner exportPartitioner() {

		ColumnRangePartitioner columnRangePartitioner = new ColumnRangePartitioner();

		columnRangePartitioner.setColumn("id");
		columnRangePartitioner.setDataSource(this.dataSource);
		columnRangePartitioner.setTable("customer");

		return columnRangePartitioner;
	}

	@Bean
	@StepScope
	public JdbcPagingItemReader<Customer> pagingItemReader(
			@Value("#{stepExecutionContext['minValue']}") Long minValue,
			@Value("#{stepExecutionContext['maxValue']}") Long maxValue
	) {

		JdbcPagingItemReader<Customer> reader = new JdbcPagingItemReader<>();

		reader.setDataSource(this.dataSource);
		reader.setFetchSize(100000);
		reader.setPageSize(100000);
		reader.setRowMapper(new CustomerRowMapper());

		MySqlPagingQueryProvider queryProvider = new MySqlPagingQueryProvider();
		queryProvider.setSelectClause("id, firstName, lastName, birthdate");
		queryProvider.setFromClause("from customer");
		queryProvider.setWhereClause("where id >= " + minValue + " and id <= " + maxValue);

		Map<String, Order> sortKeys = new HashMap<>(1);

		sortKeys.put("id", Order.ASCENDING);

		queryProvider.setSortKeys(sortKeys);

		reader.setQueryProvider(queryProvider);

		return reader;
	}


	@Bean
	@StepScope
	public StaxEventItemWriter<Customer> customerItemWriter(

			@Value("#{stepExecutionContext['ressource']}") Object ressource

	) {

		StaxEventItemWriter<Customer> itemWriter = new StaxEventItemWriter<>();

		itemWriter.setRootTagName("customers");
		itemWriter.setMarshaller(marshaller());

		File file = new File("./src/main/resources/data/" + ressource + ".xml");

		String customerOutputPath = file.getAbsolutePath();

		itemWriter.setResource(new FileSystemResource(customerOutputPath));


		return itemWriter;
	}

	@Bean
	public Marshaller marshaller() {
		XStreamMarshaller marshaller = new XStreamMarshaller();

		Map<String, Class> aliases = new HashMap<>();

		aliases.put("customer", Customer.class);

		marshaller.setAliases(aliases);

		return marshaller;
	}


	@Bean
	public Step exportStep() throws Exception {
		return stepBuilderFactory.get("exportStep")
				.partitioner(slaveStep1().getName(), exportPartitioner())
				.step(slaveStep1())
				.gridSize(4)
				.taskExecutor(batchTaskExecutor())
				.build();
	}


	@Bean
	public Step slaveStep1() {
		return stepBuilderFactory.get("slaveStep")
				.<Customer, Customer>chunk(100000)
				.reader(pagingItemReader(null, null))
				.writer(customerItemWriter(null))
				.build();
	}

	@Bean
	public Job jobExport() throws Exception {

		return jobBuilderFactory.get("jobExport")
				.incrementer(new RunIdIncrementer())
				.start(exportStep())
				.build();
	}
}