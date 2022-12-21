package com.bilik;

import com.bilik.domain.Person;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FromIteratorFunction;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static com.bilik.utils.ListUtils.listWithElements;

public class Main {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        Collection<Person> col = new ArrayList<>();
        PersonShuffleIterator it = new PersonShuffleIterator(50);
        while(it.hasNext()) {
            col.add(it.next());
        }
        DataStream<Person> flintstones = env.fromCollection(col);
        //DataStream<Person> flintstones = env.addSource(new FromIteratorFunction(col.iterator()));

        DataStream<Person> adults = flintstones.filter(printingFilter(person -> person.getAge() >= 18, "ADULT AGE FILTER"));
        adults.print();

        JobExecutionResult result = env.execute();
        System.out.println("RESULT:\n" + result);
    }

/*
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Person> peopleStream = env.addSource(new DelayedIteratorFunction(new PersonShuffleIterator(50), 250));

        DataStream<Person> adults = peopleStream.filter(person -> person.getAge() >= 18);

        adults.print();

        env.execute();
    }

 */


    private static <T> FilterFunction<T> printingFilter(FilterFunction<T> filter, String name) {
        return t -> {
            //System.out.println("APPLY FILTER: " + name + " | " + t.toString() + " | " + filter.filter(t));
            return filter.filter(t);
        };
    }

}

class PersonShuffleIterator implements Iterator<Person>, Serializable {

    private static final long serialVersionUID = 1L;

    private AtomicInteger id = new AtomicInteger();
    private volatile int range;
    private volatile int processed;

    private static final List<String> NAMES = listWithElements(
            "Breogg",
            "Nobuas",
            "Ayuni",
            "Edirret",
            "Aluulac",
            "Indredieth Rulliltrol",
            "Findal",
            "Jovun Rirgoninn",
            "Feraa"
    );

    public PersonShuffleIterator(Integer range) {
        this.range = range != null ? range : Integer.MAX_VALUE;
    }

    @Override
    public boolean hasNext() {
        return range > processed;
    }

    @Override
    public Person next() {
        processed += 1;
        return randomPerson();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void forEachRemaining(Consumer<? super Person> action) {
        Iterator.super.forEachRemaining(action);
    }

    private Person randomPerson() {
        return new Person(
                id.incrementAndGet(),
                ThreadLocalRandom.current().nextInt(10, 80),
                NAMES.get(ThreadLocalRandom.current().nextInt(NAMES.size()))
        );
    }

}

