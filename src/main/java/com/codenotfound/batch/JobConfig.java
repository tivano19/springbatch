package com.codenotfound.batch;

import com.codenotfound.step.PersonItemProcessor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import com.codenotfound.model.Person;
import org.springframework.core.io.FileSystemResource;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class JobConfig {

    @Autowired
    private DataSource dxSource;

    @Bean
    public Job myJob(JobBuilderFactory jobBuilders,
                            StepBuilderFactory stepBuilders) {
        return jobBuilders.get("myJob")
                .start(initStep(stepBuilders)).build();
    }

    @Bean
    public Step initStep(StepBuilderFactory stepBuilders) {
        return stepBuilders.get("myStep")
                .<Person, String>chunk(10).reader(reader())
                .processor(processor()).writer(writer()).build();
    }

    @Bean
    ItemReader<Person> reader() {
        JdbcPagingItemReader<Person> databaseReader = new JdbcPagingItemReader<>();
        databaseReader.setDataSource(dxSource);
        databaseReader.setPageSize(10);
        PagingQueryProvider queryProvider = createQueryProvider();
        databaseReader.setQueryProvider(queryProvider);
        databaseReader.setRowMapper(new PersonRowMapper());
        return databaseReader;
    }

    private PagingQueryProvider createQueryProvider() {
        MySqlPagingQueryProvider queryProvider = new MySqlPagingQueryProvider();
        queryProvider.setSelectClause("SELECT col1, col2, col3, col4");
        queryProvider.setFromClause("FROM mydb.mytable");
        Map<String, Order> sortConfiguration = new HashMap<>();
        sortConfiguration.put("col1", Order.ASCENDING);
        queryProvider.setSortKeys(sortConfiguration);
        return queryProvider;
    }

    @Bean
    public ItemProcessor processor() {
        return new PersonItemProcessor();
    }


    @Bean
    public FlatFileItemWriter<Person> writer() {
        FlatFileItemWriter<Person> writer = new FlatFileItemWriter<Person>();
        writer.setResource(new FileSystemResource(("target/test-outputs/greetings.csv")));
        writer.setLineAggregator(new DelimitedLineAggregator<Person>() {{
            setDelimiter(",");
            setFieldExtractor(new BeanWrapperFieldExtractor<Person>() {{
                setNames(new String[]{"id", "firstName", "lastName", "fullName"});
            }});
        }});

        return writer;
    }

    private class PersonRowMapper implements org.springframework.jdbc.core.RowMapper<Person> {
        @Override
        public Person mapRow(ResultSet rs, int i) throws SQLException {
            Person user = new Person();
            user.setId(rs.getLong("col1"));
            user.setFirstName(rs.getString("col2"));
            user.setLastName(rs.getString("col3"));
            user.setFullName(rs.getString("col4"));
            return user;
        }
    }
}
