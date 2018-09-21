package com.zj.ch9_2.batch;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.validator.Validator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.transaction.PlatformTransactionManager;

import com.zj.ch9_2.domain.People;

@Configuration
@EnableBatchProcessing //开启批处理的支持
public class TriggerBatchConfig {
	
	@Bean
	@StepScope
	public FlatFileItemReader<People> reader(@Value("#{jobParameters['input.file.name']}") String pathToFile) throws Exception{
		//使用FlatFileItemReader读取文件
		FlatFileItemReader<People> reader=new FlatFileItemReader<>();
		//设置csv文件路径
		reader.setResource(new ClassPathResource(pathToFile));
		//对csv文件的数据和领域模型类做对应映射
		reader.setLineMapper(new DefaultLineMapper<People>(){{
			setLineTokenizer(new DelimitedLineTokenizer(){{
				setNames(new String[]{"name","age","nation","address"});
			}});
			setFieldSetMapper(new BeanWrapperFieldSetMapper<People>(){{
				setTargetType(People.class);
			}});
		}});
		return reader;
	}
	
	@Bean
	public ItemProcessor<People, People> processor(){
		//使用自定义的ItemProcessor实现CsvItemProcessor
		CsvItemProcessor processor=new CsvItemProcessor();
		//为processor指定校验器为csvBeanValidator
		processor.setValidator(csvBeanValidator());
		return processor;
	}
	
	@Bean
	public ItemWriter<People> writer(DataSource dataSource){
		//使用JDBC批处理的JdbcBatchItemWriter来写数据到数据库
		JdbcBatchItemWriter<People> writer=new JdbcBatchItemWriter<>();
		writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<People>());
		String sql="insert into people "+"(name,age,nation,address) "+"values(:name,:age,:nation,:address)";
		writer.setSql(sql);//设置要执行批处理的SQL语句
		writer.setDataSource(dataSource);
		return writer;
	}
	
	@Bean
	public JobRepository jobRepository(DataSource dataSource,PlatformTransactionManager transactionManager) throws Exception{
		JobRepositoryFactoryBean jobRepositoryFactoryBean=new JobRepositoryFactoryBean();
		jobRepositoryFactoryBean.setDataSource(dataSource);
		jobRepositoryFactoryBean.setTransactionManager(transactionManager);
		jobRepositoryFactoryBean.setDatabaseType("mysql");
		return jobRepositoryFactoryBean.getObject();
	}
	
	@Bean
	public SimpleJobLauncher jobLauncher(DataSource dataSource,PlatformTransactionManager transactionManager) throws Exception{
		SimpleJobLauncher jobLauncher=new SimpleJobLauncher();
		jobLauncher.setJobRepository(jobRepository(dataSource,transactionManager));
		return jobLauncher;
	}
	
	@Bean 
	public Job importJob(JobBuilderFactory jobs,Step s1){
		return jobs.get("importJob")
				.incrementer(new RunIdIncrementer())
				.flow(s1) //为Job指定Step
				.end().listener(csvJobListener()) //绑定监听器csvJobListener
				.build();
	}
	
	@Bean
	public Step step1(StepBuilderFactory stepBuilderFactory,ItemReader<People> reader,ItemWriter<People> writer,ItemProcessor<People, People> processor){
		return stepBuilderFactory.get("step1")
				.<People,People>chunk(65000) //批处理每次提交65000条数据
				.reader(reader) //给step绑定reader
				.processor(processor) //给step绑定processor
				.writer(writer) //给step绑定writer
				.build();
	}

	@Bean
	private JobExecutionListener csvJobListener() {
		return new CsvJobListener();
	}

	@Bean
	private Validator<People> csvBeanValidator() {
		return new CsvBeanValidator<>();
	}
}
