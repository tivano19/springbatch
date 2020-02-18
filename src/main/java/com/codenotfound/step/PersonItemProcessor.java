package com.codenotfound.step;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import com.codenotfound.model.Person;

public class PersonItemProcessor implements ItemProcessor<Person, Person> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PersonItemProcessor.class);

  @Override
  public Person process(Person person) throws Exception {
    LOGGER.info("Processing '{}'", person);
    return person;
  }
}
