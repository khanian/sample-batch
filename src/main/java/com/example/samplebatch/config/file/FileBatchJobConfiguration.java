package com.example.samplebatch.config.file;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import java.math.BigDecimal;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class FileBatchJobConfiguration {
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private static final int CHCNK_SIZE = 2;
    private static final int ADD_PRICE = 1000;
    private static final String JOB_NAME = "fileJob";
    private static final String STEP_NAME = "fileStep";

    private final Resource inputFileResource = new FileSystemResource("input/sample-product.csv");
    private final Resource outputFileResource = new FileSystemResource("output/output-product.csv");

    @Bean
    public Job fileJob() {
        return this.jobBuilderFactory.get(JOB_NAME)
                .incrementer(new RunIdIncrementer())
                .start(fileStep())
                .build();
    }

    @Bean
    public Step fileStep() {
        return this.stepBuilderFactory.get(STEP_NAME)
                .<Product, Product> chunk(CHCNK_SIZE)
                .reader(fileItemReader())
                .processor(fileItemProcessor())
                .writer(fileItemWriter())
                .build();
    }

    @Bean
    public FlatFileItemReader fileItemReader() {
        FlatFileItemReader<Product> productItemReader = new FlatFileItemReader<>();
        productItemReader.setResource(this.inputFileResource);

        DefaultLineMapper<Product> lineMapper = new DefaultLineMapper<>();
        lineMapper.setLineTokenizer(new DelimitedLineTokenizer());
        lineMapper.setFieldSetMapper(new ProductFileSetMapper());

        productItemReader.setLineMapper(lineMapper);
        return productItemReader;
    }

    @Bean
    public ItemProcessor<Product, Product> fileItemProcessor() {
        return product -> {
            BigDecimal updatedPrice = product.getPrice().add(new BigDecimal(ADD_PRICE));
            log.info("[ItemProcessor] Updated Product Price - {}", updatedPrice);
            product.setPrice(updatedPrice);
            return product;
        };
    }

    @Bean
    public FlatFileItemWriter<Product> fileItemWriter() {
        FlatFileItemWriter fileItemWriter = new FlatFileItemWriter();
        fileItemWriter.setResource(outputFileResource);
        fileItemWriter.setAppendAllowed(true);

        DelimitedLineAggregator<Product> lineAggregator = new DelimitedLineAggregator<>();
        lineAggregator.setFieldExtractor(new BeanWrapperFieldExtractor<>() {
            {
                setNames(new String[]{"id", "name", "price"});
            }
        });
        fileItemWriter.setLineAggregator(lineAggregator);

        return fileItemWriter;
    }
}
